import csv
import io
import json
import logging
import os
import queue
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor

import azure.functions as func
from common_utils.utils import (
    get_entity_key_value,
    get_metameta_dict,
    get_snowflake_connection,
)
from sendEmailNotification import send_email

# =========================================================================
# CONNECTION POOL
# =========================================================================
_conn_pool = queue.Queue()


def init_connection_pool(pool_size: int) -> None:
    """Initializes snowflake connections into the thread pool."""
    logging.info(f"Initializing connection pool with {pool_size} connection(s)...")
    created = 0
    try:
        for _ in range(pool_size):
            cs, ctx = get_snowflake_connection()
            _conn_pool.put((cs, ctx))
            created += 1
        logging.info(f"Connection pool successfully initialized ({created} active).")
    except Exception as exc:
        logging.error(f"Connection pool initialization failed: {exc}")
        close_connection_pool()
        raise


def borrow_connection():
    return _conn_pool.get()


def return_connection(conn_tuple):
    _conn_pool.put(conn_tuple)


def close_connection_pool() -> None:
    while not _conn_pool.empty():
        try:
            cs, ctx = _conn_pool.get_nowait()
            cs.close()
            ctx.close()
        except Exception as exc:
            logging.warning(f"Error closing pooled Snowflake connection: {exc}")


# =========================================================================
# METADATA EXTRACTION
# =========================================================================
def extract_all_db_schema_targets(metadata: dict):
    """Extracts DB -> Schemas and DB -> Excluded Views mapping."""
    db_schema_map = {}
    excluded_views_map = {}

    entities = metadata.get("entities", [])
    if not isinstance(entities, list):
        entities = [entities] if entities else []

    if not entities:
        top_db = get_entity_key_value("snowflake_database", None, metadata) or get_entity_key_value("source_database", None, metadata)
        top_schema = get_entity_key_value("snowflake_schemas", None, metadata) or get_entity_key_value("default_source_schema", None, metadata) or ""
        if top_db:
            top_db_clean = str(top_db).strip().upper()
            db_schema_map.setdefault(top_db_clean, set())
            if str(top_schema).strip():
                db_schema_map[top_db_clean].add(str(top_schema).strip().upper())
            excluded_views_map.setdefault(top_db_clean, set())

    for ent in entities:
        db = get_entity_key_value("snowflake_database", ent, metadata) or get_entity_key_value("source_database", ent, metadata)
        if not db or not str(db).strip():
            continue

        db_clean = str(db).strip().upper()
        db_schema_map.setdefault(db_clean, set())
        excluded_views_map.setdefault(db_clean, set())

        schemas = (
            get_entity_key_value("snowflake_schemas", ent, metadata)
            or get_entity_key_value("source_schema", ent, metadata)
            or ""
        )

        if isinstance(schemas, list):
            for s in schemas:
                if s and str(s).strip():
                    db_schema_map[db_clean].add(str(s).strip().upper())
        elif schemas and str(schemas).strip():
            db_schema_map[db_clean].add(str(schemas).strip().upper())

        exc_views = ent.get("exclude_views") or ent.get("excluded_views") or []
        if not isinstance(exc_views, list):
            exc_views = [exc_views]

        for ev in exc_views:
            if ev and str(ev).strip():
                excluded_views_map[db_clean].add(str(ev).strip().upper())

    return db_schema_map, excluded_views_map


# =========================================================================
# DISCOVERY: 90+ DAYS STALE VIEWS
# =========================================================================
def fetch_stale_views_for_db(target_db: str, target_schemas: set, excluded_views: set = None, days_threshold: int = 90) -> list:
    """Discovers views created >= 90 days ago and not accessed in the last 90 days."""
    logging.info(f"Scanning '{target_db}' for views unused for >= {days_threshold} days...")
    views_found = []

    where_clauses = [
        f"v.CREATED <= DATEADD(day, -{days_threshold}, CURRENT_TIMESTAMP)",
        f"(u.LAST_ACCESSED IS NULL OR u.LAST_ACCESSED <= DATEADD(day, -{days_threshold}, CURRENT_TIMESTAMP))"
    ]

    schemas_list = [s.replace("'", "''") for s in target_schemas if s]
    if schemas_list:
        schema_list_str = ", ".join([f"'{s}'" for s in schemas_list])
        where_clauses.append(f"v.TABLE_SCHEMA IN ({schema_list_str})")

    if excluded_views:
        plain_views = []
        fq_views = []
        for v in excluded_views:
            sanitized = str(v).replace("'", "''")
            if "." in sanitized:
                fq_views.append(f"'{sanitized}'")
            else:
                plain_views.append(f"'{sanitized}'")

        if plain_views:
            where_clauses.append(f"v.TABLE_NAME NOT IN ({', '.join(plain_views)})")
        if fq_views:
            where_clauses.append(f"(v.TABLE_SCHEMA || '.' || v.TABLE_NAME) NOT IN ({', '.join(fq_views)})")

    where_stmt = f"WHERE {' AND '.join(where_clauses)}"

    query = f"""
        WITH recent_view_usage AS (
            SELECT
                f.value:objectName::STRING AS full_object_name,
                MAX(query_start_time) AS LAST_ACCESSED
            FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY,
            LATERAL FLATTEN(input => base_objects_accessed) f
            WHERE query_start_time >= DATEADD(day, -{days_threshold}, CURRENT_TIMESTAMP)
              AND f.value:objectDomain::STRING = 'Table'
            GROUP BY 1
        )
        SELECT
            v.TABLE_SCHEMA,
            v.TABLE_NAME,
            TO_VARCHAR(v.CREATED, 'YYYY-MM-DD HH24:MI:SS') AS CREATED_AT,
            COALESCE(TO_VARCHAR(u.LAST_ACCESSED, 'YYYY-MM-DD HH24:MI:SS'), 'NEVER') AS LAST_USED_AT
        FROM "{target_db}".INFORMATION_SCHEMA.VIEWS v
        LEFT JOIN recent_view_usage u
            ON UPPER(u.full_object_name) = ('{target_db.upper()}.' || UPPER(v.TABLE_SCHEMA) || '.' || UPPER(v.TABLE_NAME))
        {where_stmt};
    """

    cs, ctx = borrow_connection()
    try:
        cs.execute(query)
        for row in cs.fetchall():
            # (db, schema, view_name, created_at, last_accessed_at)
            views_found.append((target_db, row[0].upper(), row[1].upper(), row[2], row[3]))
        logging.info(f"Discovered {len(views_found)} stale view(s) in Snowflake DB '{target_db}'.")
    except Exception as exc:
        logging.error(f"Failed to query stale views for DB '{target_db}': {exc}")
    finally:
        return_connection((cs, ctx))

    return views_found


# =========================================================================
# DROP / DELETION WORKER
# =========================================================================
def drop_single_stale_view(view_tuple: tuple, dry_run: bool = True) -> tuple:
    """Executes 'DROP VIEW IF EXISTS' on a single stale view."""
    v_db, v_schema, v_view, v_created, v_last_used = view_tuple
    fq_name = f'"{v_db}"."{v_schema}"."{v_view}"'
    drop_stmt = f"DROP VIEW IF EXISTS {fq_name}"

    if dry_run:
        logging.info(f"[DRY-RUN] Would drop: {fq_name}")
        return (v_db, v_schema, v_view, v_created, v_last_used, "DRY_RUN", "")

    cs, ctx = borrow_connection()
    try:
        cs.execute(drop_stmt)
        logging.info(f"SUCCESS - Dropped view: {fq_name}")
        return (v_db, v_schema, v_view, v_created, v_last_used, "DROPPED", "")
    except Exception as exc:
        err_msg = str(exc).replace("\n", " ").replace("\r", " ")
        logging.error(f"FAILED - Drop failed on {fq_name}: {err_msg}")
        return (v_db, v_schema, v_view, v_created, v_last_used, "FAILED", err_msg)
    finally:
        return_connection((cs, ctx))


def drop_empty_schemas_for_db(target_db: str, schemas: set, dry_run: bool = True) -> list:
    """Checks and drops schemas if they contain no remaining tables or views."""
    schema_cleanup_results = []
    cs, ctx = borrow_connection()
    try:
        for schema in schemas:
            if not schema:
                continue
            check_query = f"""
                SELECT COUNT(*) 
                FROM "{target_db}".INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = '{schema.upper()}'
            """
            cs.execute(check_query)
            count = cs.fetchone()[0]

            if count == 0:
                drop_stmt = f'DROP SCHEMA IF EXISTS "{target_db}"."{schema}"'
                if dry_run:
                    logging.info(f"[DRY-RUN] Schema '{target_db}.{schema}' is empty. Would execute: {drop_stmt}")
                    schema_cleanup_results.append((target_db, schema, "DRY_RUN"))
                else:
                    cs.execute(drop_stmt)
                    logging.info(f"SUCCESS - Dropped empty schema '{target_db}.{schema}'")
                    schema_cleanup_results.append((target_db, schema, "DROPPED"))
    except Exception as exc:
        logging.error(f"Error checking/dropping schemas in '{target_db}': {exc}")
    finally:
        return_connection((cs, ctx))

    return schema_cleanup_results


# =========================================================================
# NOTIFICATION & REPORTING
# =========================================================================
def generate_stale_views_csv(action_results: list) -> str:
    """Generates a CSV report of all processed stale views."""
    output = io.StringIO()
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["Database", "Schema", "View Name", "Created At", "Last Accessed At", "Action Status", "Error Details"])
    for row in action_results:
        writer.writerow(row)
    return output.getvalue()


def send_cleanup_email(metameta_name: str, action_results: list, schema_results: list, dry_run: bool) -> None:
    to_list = ["bhangaleg@careoregon.org", "kalea@careoregon.org"]
    cc_list = []

    total_views = len(action_results)
    failed_cnt = sum(1 for r in action_results if r[5] == "FAILED")
    success_cnt = total_views - failed_cnt

    mode_label = "DRY-RUN" if dry_run else "EXECUTED"
    subject = f"Snowflake Stale Views Cleanup Report [{mode_label}]: {metameta_name}"

    view_rows = ""
    for db, schema, view, created, last_used, status, err in action_results:
        status_color = "#2e7d32" if status in ["DROPPED", "DRY_RUN"] else "#d32f2f"
        view_rows += f"""
        <tr>
            <td style="padding: 8px 12px; border: 1px solid #dee2e6;">{db}</td>
            <td style="padding: 8px 12px; border: 1px solid #dee2e6;">{schema}</td>
            <td style="padding: 8px 12px; border: 1px solid #dee2e6;"><b>{view}</b></td>
            <td style="padding: 8px 12px; border: 1px solid #dee2e6;">{created}</td>
            <td style="padding: 8px 12px; border: 1px solid #dee2e6;">{last_used}</td>
            <td style="padding: 8px 12px; border: 1px solid #dee2e6; color: {status_color}; font-weight: bold;">{status}</td>
            <td style="padding: 8px 12px; border: 1px solid #dee2e6; font-family: Consolas, monospace; font-size: 11px;">{err}</td>
        </tr>
        """

    if not view_rows:
        view_rows = '<tr><td colspan="7" style="padding: 8px 12px; border: 1px solid #dee2e6; text-align: center; color: #777;">No stale views identified.</td></tr>'

    schema_rows = ""
    for db, schema, status in schema_results:
        schema_rows += f"""
        <tr>
            <td style="padding: 8px 12px; border: 1px solid #dee2e6;">{db}</td>
            <td style="padding: 8px 12px; border: 1px solid #dee2e6;">{schema}</td>
            <td style="padding: 8px 12px; border: 1px solid #dee2e6; font-weight: bold;">{status}</td>
        </tr>
        """

    schema_section = ""
    if schema_results:
        schema_section = f"""
        <h3>Empty Schemas Cleaned</h3>
        <table style="border-collapse: collapse; width: 600px; font-size: 13px; text-align: left; margin-bottom: 20px; border: 1px solid #dee2e6;">
            <thead>
                <tr style="background-color: #343a40; color: #ffffff;">
                    <th style="padding: 8px 12px;">Database</th>
                    <th style="padding: 8px 12px;">Schema</th>
                    <th style="padding: 8px 12px;">Status</th>
                </tr>
            </thead>
            <tbody>{schema_rows}</tbody>
        </table>
        """

    html_body = f"""
    <html>
    <body style="font-family: Arial, sans-serif; color: #333; line-height: 1.6;">
        <h2 style="color: #1565c0; border-bottom: 2px solid #1565c0; padding-bottom: 8px;">
            Stale Views Cleanup Pipeline - {mode_label}
        </h2>
        <p>Automated identification and cleanup of Snowflake views created <b>90+ days ago</b> and <b>not queried in 90+ days</b>.</p>

        <h3>Summary</h3>
        <table style="border-collapse: collapse; width: 350px; margin-bottom: 20px; border: 1px solid #dee2e6; font-size: 13px;">
            <tr style="background-color: #f8f9fa;">
                <td style="padding: 8px 12px; border: 1px solid #dee2e6;"><b>Target Metadata</b></td>
                <td style="padding: 8px 12px; border: 1px solid #dee2e6;">{metameta_name}</td>
            </tr>
            <tr>
                <td style="padding: 8px 12px; border: 1px solid #dee2e6;"><b>Execution Mode</b></td>
                <td style="padding: 8px 12px; border: 1px solid #dee2e6;"><b>{mode_label}</b></td>
            </tr>
            <tr style="background-color: #f8f9fa;">
                <td style="padding: 8px 12px; border: 1px solid #dee2e6;"><b>Total Identified Views</b></td>
                <td style="padding: 8px 12px; border: 1px solid #dee2e6;">{total_views}</td>
            </tr>
            <tr>
                <td style="padding: 8px 12px; border: 1px solid #dee2e6;"><b>Processed / Dropped</b></td>
                <td style="padding: 8px 12px; border: 1px solid #dee2e6; color: #2e7d32; font-weight: bold;">{success_cnt}</td>
            </tr>
            <tr style="background-color: #f8f9fa;">
                <td style="padding: 8px 12px; border: 1px solid #dee2e6;"><b>Failed Drop Operations</b></td>
                <td style="padding: 8px 12px; border: 1px solid #dee2e6; color: {'#d32f2f' if failed_cnt > 0 else '#333'}; font-weight: bold;">{failed_cnt}</td>
            </tr>
        </table>

        {schema_section}

        <h3>Stale Views List</h3>
        <table style="border-collapse: collapse; width: 100%; font-size: 12px; text-align: left; border: 1px solid #dee2e6;">
            <thead>
                <tr style="background-color: #343a40; color: #ffffff;">
                    <th style="padding: 8px 10px;">Database</th>
                    <th style="padding: 8px 10px;">Schema</th>
                    <th style="padding: 8px 10px;">View Name</th>
                    <th style="padding: 8px 10px;">Created Date</th>
                    <th style="padding: 8px 10px;">Last Used</th>
                    <th style="padding: 8px 10px;">Status</th>
                    <th style="padding: 8px 10px;">Error Details</th>
                </tr>
            </thead>
            <tbody>{view_rows}</tbody>
        </table>
        <br>
        <p>Regards,<br><b>Automated Azure Function Cleanup Pipeline</b></p>
    </body>
    </html>
    """
    send_email(subject=subject, body=html_body, to=to_list, cc=cc_list)


# =========================================================================
# MAIN ENTRYPOINT
# =========================================================================
def main(mytimer: func.TimerRequest) -> None:
    start_time = time.perf_counter()
    utc_timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    logging.info(f"stale_view_cleanup_trigger started at {utc_timestamp}.")

    envs = os.getenv("environment", "prd").strip().lower()
    if envs != "prd":
        logging.warning(f"Environment is '{envs}'. Skipping cleanup execution.")
        return

    # Safety toggle: defaults to True (read-only / log only)
    dry_run = os.getenv("CLEANUP_DRY_RUN", "true").strip().lower() == "true"
    drop_empty_schemas_flag = os.getenv("DROP_EMPTY_SCHEMAS", "false").strip().lower() == "true"
    metameta_name = os.getenv("TARGET_METAMETA_NAME", "View_validation")
    max_workers = int(os.getenv("CLEANUP_MAX_WORKERS", "4"))

    try:
        # 1. Load Metadata
        try:
            metadata = get_metameta_dict(db_name=metameta_name)
            all_db_schema_map, excluded_views_map = extract_all_db_schema_targets(metadata)
        except Exception as exc:
            logging.error(f"Failed to load metadata '{metameta_name}': {exc}")
            return

        if not all_db_schema_map:
            logging.warning(f"No valid database targets found in metadata '{metameta_name}'.")
            return

        # 2. Initialize Pool
        init_connection_pool(pool_size=max_workers)
        action_results = []
        schema_cleanup_results = []

        try:
            # 3. Discovery Phase
            stale_views_to_drop = []
            for db, schemas in all_db_schema_map.items():
                db_exc = excluded_views_map.get(db, set())
                db_stale = fetch_stale_views_for_db(target_db=db, target_schemas=schemas, excluded_views=db_exc, days_threshold=90)
                stale_views_to_drop.extend(db_stale)

            # 4. Multi-Threaded Drop Phase
            if stale_views_to_drop:
                logging.info(f"Dropping/Processing {len(stale_views_to_drop)} stale views (dry_run={dry_run})...")
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    action_results = list(
                        executor.map(lambda v: drop_single_stale_view(v, dry_run=dry_run), stale_views_to_drop)
                    )

            # 5. Schema Deletion (Optional)
            if drop_empty_schemas_flag:
                for db, schemas in all_db_schema_map.items():
                    res = drop_empty_schemas_for_db(target_db=db, schemas=schemas, dry_run=dry_run)
                    schema_cleanup_results.extend(res)

        finally:
            close_connection_pool()

        # 6. Save CSV Report to temp directory
        if action_results:
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            filename = f"stale_views_cleanup_{metameta_name}_{timestamp}.csv"
            csv_content = generate_stale_views_csv(action_results)
            try:
                local_path = os.path.join(tempfile.gettempdir(), filename)
                with open(local_path, "w", newline="", encoding="utf-8") as f:
                    f.write(csv_content)
                logging.info(f"Cleanup report saved to: {local_path}")
            except Exception as err:
                logging.warning(f"Could not persist local CSV copy: {err}")

        # 7. Notification
        send_cleanup_email(
            metameta_name=metameta_name,
            action_results=action_results,
            schema_results=schema_cleanup_results,
            dry_run=dry_run
        )

        elapsed = round(time.perf_counter() - start_time, 2)
        logging.info(f"Stale view cleanup completed in {elapsed}s. Dry Run: {dry_run}")

    except Exception as fatal_exc:
        logging.error(f"Fatal error in stale view cleanup job: {fatal_exc}", exc_info=True)
