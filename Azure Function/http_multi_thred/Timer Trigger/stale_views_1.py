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
    logging.info(f"Initializing connection pool with {pool_size} connection(s)...")
    created = 0
    try:
        for _ in range(pool_size):
            cs, ctx = get_snowflake_connection()
            _conn_pool.put((cs, ctx))
            created += 1
        logging.info(f"Connection pool initialized ({created} active).")
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
            logging.warning(f"Error closing connection: {exc}")


# =========================================================================
# METADATA EXTRACTION
# =========================================================================
def _parse_schema_field(val) -> set:
    """Helper to convert list or comma-separated string to an uppercase set."""
    schemas = set()
    if isinstance(val, list):
        for s in val:
            if s and str(s).strip():
                schemas.add(str(s).strip().upper())
    elif val and str(val).strip():
        for s in str(val).split(","):
            if s.strip():
                schemas.add(s.strip().upper())
    return schemas


def extract_schema_configurations(metadata: dict):
    """
    Extracts distinct mappings per DB:
    - standard_schemas_map: DB -> Set of snowflake_schemas (Direct validation)
    - stale_schemas_map: DB -> Set of stale_view_schemas (Drop if stale, Validate if active)
    - excluded_views_map: DB -> Set of excluded views
    """
    standard_schemas_map = {}
    stale_schemas_map = {}
    excluded_views_map = {}

    entities = metadata.get("entities", [])
    if not isinstance(entities, list):
        entities = [entities] if entities else []

    # Fallback if no entities list
    if not entities:
        top_db = get_entity_key_value("snowflake_database", None, metadata) or get_entity_key_value("source_database", None, metadata)
        if top_db:
            db_clean = str(top_db).strip().upper()
            standard_schemas_map[db_clean] = _parse_schema_field(get_entity_key_value("snowflake_schemas", None, metadata))
            stale_schemas_map[db_clean] = _parse_schema_field(get_entity_key_value("stale_view_schemas", None, metadata))
            
            top_exclude = metadata.get("exclude_views", [])
            if not isinstance(top_exclude, list):
                top_exclude = [top_exclude]
            excluded_views_map[db_clean] = {str(e).strip().upper() for e in top_exclude if str(e).strip()}

    for ent in entities:
        db = get_entity_key_value("snowflake_database", ent, metadata) or get_entity_key_value("source_database", ent, metadata)
        if not db or not str(db).strip():
            continue

        db_clean = str(db).strip().upper()
        standard_schemas_map.setdefault(db_clean, set())
        stale_schemas_map.setdefault(db_clean, set())
        excluded_views_map.setdefault(db_clean, set())

        # 1. Standard validation schemas
        std_raw = get_entity_key_value("snowflake_schemas", ent, metadata) or get_entity_key_value("source_schema", ent, metadata)
        standard_schemas_map[db_clean].update(_parse_schema_field(std_raw))

        # 2. Stale evaluation schemas
        stale_raw = get_entity_key_value("stale_view_schemas", ent, metadata)
        stale_schemas_map[db_clean].update(_parse_schema_field(stale_raw))

        # 3. Excluded views
        exc_raw = ent.get("exclude_views") or ent.get("excluded_views") or []
        if not isinstance(exc_raw, list):
            exc_raw = [exc_raw]
        for ev in exc_raw:
            if ev and str(ev).strip():
                excluded_views_map[db_clean].add(str(ev).strip().upper())

    return standard_schemas_map, stale_schemas_map, excluded_views_map


# =========================================================================
# VIEW DISCOVERY
# =========================================================================
def _build_sql_filter(target_schemas: set, excluded_views: set = None) -> str:
    where_clauses = []
    schemas_list = [s.replace("'", "''") for s in target_schemas if s]
    if schemas_list:
        where_clauses.append(f"v.TABLE_SCHEMA IN ({', '.join([f"'{s}'" for s in schemas_list])})")

    if excluded_views:
        plain_views, fq_views = [], []
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

    return f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""


def fetch_standard_views(target_db: str, target_schemas: set, excluded_views: set = None) -> list:
    """Discovers all views for standard snowflake_schemas."""
    if not target_schemas:
        return []

    where_stmt = _build_sql_filter(target_schemas, excluded_views)
    query = f"""
        SELECT TABLE_SCHEMA, TABLE_NAME
        FROM "{target_db}".INFORMATION_SCHEMA.VIEWS v
        {where_stmt};
    """
    views_found = []
    cs, ctx = borrow_connection()
    try:
        cs.execute(query)
        for row in cs.fetchall():
            views_found.append((target_db, row[0].upper(), row[1].upper(), "STANDARD_SCHEMA"))
    except Exception as exc:
        logging.error(f"Failed to fetch standard views for DB '{target_db}': {exc}")
    finally:
        return_connection((cs, ctx))

    return views_found


def fetch_stale_and_active_views(target_db: str, target_schemas: set, excluded_views: set = None, days_threshold: int = 90):
    """
    Evaluates stale_view_schemas:
    - Stale: Created >= 90 days ago and not accessed in the last 90 days.
    - Active: Created < 90 days ago OR queried in the last 90 days.
    """
    if not target_schemas:
        return [], []

    where_stmt = _build_sql_filter(target_schemas, excluded_views)
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
            COALESCE(TO_VARCHAR(u.LAST_ACCESSED, 'YYYY-MM-DD HH24:MI:SS'), 'NEVER') AS LAST_USED_AT,
            CASE 
                WHEN v.CREATED <= DATEADD(day, -{days_threshold}, CURRENT_TIMESTAMP)
                 AND (u.LAST_ACCESSED IS NULL OR u.LAST_ACCESSED <= DATEADD(day, -{days_threshold}, CURRENT_TIMESTAMP))
                THEN 'STALE'
                ELSE 'ACTIVE'
            END AS STALENESS_STATUS
        FROM "{target_db}".INFORMATION_SCHEMA.VIEWS v
        LEFT JOIN recent_view_usage u
            ON UPPER(u.full_object_name) = ('{target_db.upper()}.' || UPPER(v.TABLE_SCHEMA) || '.' || UPPER(v.TABLE_NAME))
        {where_stmt};
    """

    stale_views, active_views = [], []
    cs, ctx = borrow_connection()
    try:
        cs.execute(query)
        for row in cs.fetchall():
            item = (target_db, row[0].upper(), row[1].upper(), row[2], row[3])
            if row[4] == "STALE":
                stale_views.append(item)
            else:
                # Add to validation list
                active_views.append((target_db, row[0].upper(), row[1].upper(), "STALE_SCHEMA_ACTIVE"))
    except Exception as exc:
        logging.error(f"Failed to query stale schemas for DB '{target_db}': {exc}")
    finally:
        return_connection((cs, ctx))

    return stale_views, active_views


# =========================================================================
# DROP & VALIDATION WORKERS
# =========================================================================
def drop_single_stale_view(view_tuple: tuple, dry_run: bool = True) -> tuple:
    """Drops stale view: view_tuple is (db, schema, view, created, last_used)."""
    v_db, v_schema, v_view, v_created, v_last_used = view_tuple
    fq_name = f'"{v_db}"."{v_schema}"."{v_view}"'
    drop_stmt = f"DROP VIEW IF EXISTS {fq_name}"

    if dry_run:
        logging.info(f"[DRY-RUN] Would drop stale view: {fq_name}")
        return (v_db, v_schema, v_view, v_created, v_last_used, "DRY_RUN", "")

    cs, ctx = borrow_connection()
    try:
        cs.execute(drop_stmt)
        logging.info(f"DROPPED - Stale view: {fq_name}")
        return (v_db, v_schema, v_view, v_created, v_last_used, "DROPPED", "")
    except Exception as exc:
        err_msg = str(exc).replace("\n", " ").replace("\r", " ")
        logging.error(f"FAILED DROP - {fq_name}: {err_msg}")
        return (v_db, v_schema, v_view, v_created, v_last_used, "FAILED_DROP", err_msg)
    finally:
        return_connection((cs, ctx))


def validate_single_view(view_tuple: tuple) -> tuple:
    """Health checks active views: view_tuple is (db, schema, view, origin_source)."""
    v_db, v_schema, v_view, origin_src = view_tuple
    fq_name = f'"{v_db}"."{v_schema}"."{v_view}"'

    cs, ctx = borrow_connection()
    try:
        cs.execute(f"SELECT * FROM {fq_name} LIMIT 0")
        logging.info(f"VALIDATED OK - {fq_name} ({origin_src})")
        return (v_db, v_schema, v_view, origin_src, "PASSED", "")
    except Exception as exc:
        err_msg = str(exc).replace("\n", " ").replace("\r", " ")
        logging.error(f"VALIDATION FAILED - {fq_name}: {err_msg}")
        return (v_db, v_schema, v_view, origin_src, "FAILED", err_msg)
    finally:
        return_connection((cs, ctx))


# =========================================================================
# NOTIFICATION DISPATCH
# =========================================================================
def send_summary_email(metameta_name: str, stale_results: list, validation_results: list, dry_run: bool) -> None:
    to_list = ["bhangaleg@careoregon.org", "kalea@careoregon.org"]
    cc_list = []

    stale_count = len(stale_results)
    val_total = len(validation_results)
    val_failed = sum(1 for r in validation_results if r[4] == "FAILED")
    val_passed = val_total - val_failed

    mode_label = "DRY-RUN" if dry_run else "EXECUTED"
    overall_status = "FAILED" if val_failed > 0 else "PASSED"
    subject = f"Snowflake View Processing Report [{mode_label}] - {overall_status}: {metameta_name}"

    failed_val_rows = ""
    for db, schema, view, origin, status, err in validation_results:
        if status == "FAILED":
            failed_val_rows += f"""
            <tr>
                <td style="padding: 8px 12px; border: 1px solid #dee2e6;">{db}</td>
                <td style="padding: 8px 12px; border: 1px solid #dee2e6;">{schema}</td>
                <td style="padding: 8px 12px; border: 1px solid #dee2e6;"><b>{view}</b></td>
                <td style="padding: 8px 12px; border: 1px solid #dee2e6;">{origin}</td>
                <td style="padding: 8px 12px; border: 1px solid #dee2e6; color: #d32f2f; font-weight: bold;">{status}</td>
                <td style="padding: 8px 12px; border: 1px solid #dee2e6; font-family: monospace; font-size: 11px;">{err}</td>
            </tr>
            """

    failed_section = ""
    if failed_val_rows:
        failed_section = f"""
        <h3 style="color: #d32f2f;">Failed View Validations</h3>
        <table style="border-collapse: collapse; width: 100%; font-size: 12px; border: 1px solid #dee2e6; margin-bottom: 20px;">
            <thead>
                <tr style="background-color: #343a40; color: #ffffff;">
                    <th style="padding: 8px 10px;">Database</th>
                    <th style="padding: 8px 10px;">Schema</th>
                    <th style="padding: 8px 10px;">View Name</th>
                    <th style="padding: 8px 10px;">Origin Type</th>
                    <th style="padding: 8px 10px;">Status</th>
                    <th style="padding: 8px 10px;">Error Details</th>
                </tr>
            </thead>
            <tbody>{failed_val_rows}</tbody>
        </table>
        """

    html_body = f"""
    <html>
    <body style="font-family: Arial, sans-serif; color: #333; line-height: 1.6;">
        <h2 style="color: #1565c0; border-bottom: 2px solid #1565c0; padding-bottom: 8px;">
            Snowflake Views Processing Summary ({mode_label})
        </h2>
        <p>Processed view validation for standard schemas and automated staleness drop/validation for adhoc schemas.</p>

        <h3>Summary</h3>
        <table style="border-collapse: collapse; width: 400px; margin-bottom: 20px; border: 1px solid #dee2e6; font-size: 13px;">
            <tr style="background-color: #f8f9fa;">
                <td style="padding: 8px 12px; border: 1px solid #dee2e6;"><b>Target Metadata</b></td>
                <td style="padding: 8px 12px; border: 1px solid #dee2e6;">{metameta_name}</td>
            </tr>
            <tr>
                <td style="padding: 8px 12px; border: 1px solid #dee2e6;"><b>Stale Views (90+ Days Unused)</b></td>
                <td style="padding: 8px 12px; border: 1px solid #dee2e6; color: #e65100; font-weight: bold;">{stale_count} ({'Would Drop' if dry_run else 'Dropped'})</td>
            </tr>
            <tr style="background-color: #f8f9fa;">
                <td style="padding: 8px 12px; border: 1px solid #dee2e6;"><b>Total Active Views Validated</b></td>
                <td style="padding: 8px 12px; border: 1px solid #dee2e6;">{val_total}</td>
            </tr>
            <tr>
                <td style="padding: 8px 12px; border: 1px solid #dee2e6;"><b>Validated Views Passed</b></td>
                <td style="padding: 8px 12px; border: 1px solid #dee2e6; color: #2e7d32; font-weight: bold;">{val_passed}</td>
            </tr>
            <tr style="background-color: #f8f9fa;">
                <td style="padding: 8px 12px; border: 1px solid #dee2e6;"><b>Validated Views Failed</b></td>
                <td style="padding: 8px 12px; border: 1px solid #dee2e6; color: {'#d32f2f' if val_failed > 0 else '#333'}; font-weight: bold;">{val_failed}</td>
            </tr>
        </table>

        {failed_section}

        <br>
        <p>Regards,<br><b>Automated Snowflake Validation Pipeline</b></p>
    </body>
    </html>
    """
    send_email(subject=subject, body=html_body, to=to_list, cc=cc_list)


# =========================================================================
# MAIN FUNCTION ENTRYPOINT
# =========================================================================
def main(mytimer: func.TimerRequest) -> None:
    start_time = time.perf_counter()
    utc_timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    logging.info(f"snowflake_view_processor started at {utc_timestamp}.")

    envs = os.getenv("environment", "prd").strip().lower()
    if envs != "prd":
        logging.warning(f"Environment is '{envs}'. Skipping execution.")
        return

    dry_run = os.getenv("CLEANUP_DRY_RUN", "true").strip().lower() == "true"
    metameta_name = os.getenv("TARGET_METAMETA_NAME", "Snowflake_Combined_View_Job")
    max_workers = int(os.getenv("VALIDATION_MAX_WORKERS", "4"))
    days_threshold = int(os.getenv("STALE_DAYS_THRESHOLD", "90"))

    try:
        # 1. Parse Metadata
        try:
            metadata = get_metameta_dict(db_name=metameta_name)
            std_schema_map, stale_schema_map, excluded_views_map = extract_schema_configurations(metadata)
        except Exception as exc:
            logging.error(f"Failed to load metameta '{metameta_name}': {exc}")
            return

        # 2. Initialize Connection Pool
        init_connection_pool(pool_size=max_workers)
        stale_results = []
        validation_results = []

        try:
            stale_views_to_drop = []
            views_to_validate = []

            # Gather all unique databases configured
            all_dbs = set(std_schema_map.keys()).union(set(stale_schema_map.keys()))

            for db in all_dbs:
                db_exc = excluded_views_map.get(db, set())
                std_schemas = std_schema_map.get(db, set())
                stale_schemas = stale_schema_map.get(db, set())

                # A. Discover all views from standard schemas -> Direct validation
                if std_schemas:
                    std_views = fetch_standard_views(db, std_schemas, db_exc)
                    views_to_validate.extend(std_views)

                # B. Discover & classify views from stale_view_schemas
                if stale_schemas:
                    stale_list, active_list = fetch_stale_and_active_views(
                        target_db=db,
                        target_schemas=stale_schemas,
                        excluded_views=db_exc,
                        days_threshold=days_threshold
                    )
                    stale_views_to_drop.extend(stale_list)
                    views_to_validate.extend(active_list)

            # 3. Parallel Processing
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Step 1: Drop Stale Views
                if stale_views_to_drop:
                    logging.info(f"Processing {len(stale_views_to_drop)} stale views (dry_run={dry_run})...")
                    stale_results = list(
                        executor.map(lambda v: drop_single_stale_view(v, dry_run=dry_run), stale_views_to_drop)
                    )

                # Step 2: Validate Active & Standard Views
                if views_to_validate:
                    logging.info(f"Validating {len(views_to_validate)} views...")
                    validation_results = list(
                        executor.map(validate_single_view, views_to_validate)
                    )

        finally:
            close_connection_pool()

        # 4. Dispatch Email Report
        send_summary_email(
            metameta_name=metameta_name,
            stale_results=stale_results,
            validation_results=validation_results,
            dry_run=dry_run
        )

        elapsed = round(time.perf_counter() - start_time, 2)
        logging.info(f"Execution finished in {elapsed}s. Stale Dropped: {len(stale_results)} | Validated: {len(validation_results)}")

    except Exception as fatal_exc:
        logging.error(f"Fatal exception in scheduled view processor: {fatal_exc}", exc_info=True)
