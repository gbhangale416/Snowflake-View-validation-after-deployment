* **Early Returns Bypass Pool Cleanup & Final Summary**: If `all_views` is empty, the function exits (`return`) inside the `try` block. While the `finally: close_connection_pool()` block will execute, no summary log or email is triggered to indicate that 0 views were processed.
* **SQL Injection / String Escaping in Exclusion Lists**: In `fetch_views_for_db`, single quotes inside view or schema names (though rare) aren't escaped when building `'s'` or `'v'` strings in SQL `IN (...)` clauses. Using `.replace("'", "''")` prevents malformed queries.
* **Handling Database Case-Sensitivity**: Identifiers in Snowflake queries are wrapped in quotes `"{target_db}"` while `TABLE_SCHEMA` and `TABLE_NAME` comparisons use unquoted strings. If identifiers were created with mixed-case sensitive names, uppercase conversions (`.upper()`) might mismatch metadata schemas. Ensure catalog queries match how the tables were provisioned.

---

### Cleaned & Hardened Implementation

```python
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
# GLOBAL CONNECTION POOL (Strictly bounded to max_workers)
# =========================================================================
_conn_pool = queue.Queue()


def init_connection_pool(pool_size: int) -> None:
    """Initializes strictly pool_size Snowflake connections into the pool."""
    logging.info(
        f"Initializing connection pool with {pool_size} Snowflake connection(s)..."
    )
    created = 0
    try:
        for _ in range(pool_size):
            cs, ctx = get_snowflake_connection()
            _conn_pool.put((cs, ctx))
            created += 1
        logging.info(f"Successfully initialized {created} connection(s).")
    except Exception as exc:
        logging.error(
            f"Failed to fully initialize connection pool ({created}/{pool_size}): {exc}"
        )
        close_connection_pool()
        raise


def borrow_connection():
    """Borrows a connection from the pool (blocks until available)."""
    return _conn_pool.get()


def return_connection(conn_tuple):
    """Returns a connection back to the pool."""
    _conn_pool.put(conn_tuple)


def close_connection_pool() -> None:
    """Closes all connections in the pool safely."""
    while not _conn_pool.empty():
        try:
            cs, ctx = _conn_pool.get_nowait()
            cs.close()
            ctx.close()
        except Exception as e:
            logging.warning(f"Error closing pooled Snowflake connection: {e}")


# =========================================================================
# METADATA & EXCLUSIONS PARSER
# =========================================================================
def extract_all_db_schema_targets(metadata: dict):
    """Parses metameta dictionary to extract db_schema_map and excluded_views_map."""
    db_schema_map = {}
    excluded_views_map = {}

    entities = metadata.get("entities", [])
    if not isinstance(entities, list):
        entities = [entities] if entities else []

    if not entities:
        top_db = get_entity_key_value(
            "snowflake_database", None, metadata
        ) or get_entity_key_value("source_database", None, metadata)
        top_schema = (
            get_entity_key_value("snowflake_schemas", None, metadata)
            or get_entity_key_value("default_source_schema", None, metadata)
            or ""
        )
        if top_db:
            top_db_clean = str(top_db).strip().upper()
            db_schema_map.setdefault(top_db_clean, set())
            if str(top_schema).strip():
                db_schema_map[top_db_clean].add(str(top_schema).strip().upper())
            excluded_views_map.setdefault(top_db_clean, set())

    for ent in entities:
        db = get_entity_key_value(
            "snowflake_database", ent, metadata
        ) or get_entity_key_value("source_database", ent, metadata)
        if not db or not str(db).strip():
            continue

        db_clean = str(db).strip().upper()
        db_schema_map.setdefault(db_clean, set())
        excluded_views_map.setdefault(db_clean, set())

        # Target Schemas (Fixed typo fallback)
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

        # Per-Entity View Exclusions
        exc_views = (
            ent.get("exclude_views") or ent.get("excluded_views") or []
        )
        if not isinstance(exc_views, list):
            exc_views = [exc_views]

        for ev in exc_views:
            if ev and str(ev).strip():
                excluded_views_map[db_clean].add(str(ev).strip().upper())

    return db_schema_map, excluded_views_map


# =========================================================================
# VIEW DISCOVERY & VALIDATION
# =========================================================================
def fetch_views_for_db(
    target_db: str, target_schemas: set, excluded_views: set = None
) -> list:
    """Discovers views by borrowing a connection from the pool."""
    logging.info(f"Fetching views for Snowflake database: '{target_db}'...")
    views_found = []

    where_clauses = []
    schemas_list = [s.replace("'", "''") for s in target_schemas if s]
    if schemas_list:
        schema_list_str = ", ".join([f"'{s}'" for s in schemas_list])
        where_clauses.append(f"TABLE_SCHEMA IN ({schema_list_str})")

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
            where_clauses.append(
                f"TABLE_NAME NOT IN ({', '.join(plain_views)})"
            )
        if fq_views:
            where_clauses.append(
                f"(TABLE_SCHEMA || '.' || TABLE_NAME) NOT IN ({', '.join(fq_views)})"
            )

    where_stmt = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

    query = f"""
        SELECT
            TABLE_SCHEMA,
            TABLE_NAME
        FROM "{target_db}".INFORMATION_SCHEMA.VIEWS
        {where_stmt};
    """

    cs, ctx = borrow_connection()
    try:
        cs.execute(query)
        for row in cs.fetchall():
            views_found.append((target_db, row[0].upper(), row[1].upper()))
        logging.info(
            f"Discovered {len(views_found)} view(s) in Snowflake DB '{target_db}'."
        )
    except Exception as exc:
        logging.error(f"Failed to fetch views for DB '{target_db}': {exc}")
    finally:
        return_connection((cs, ctx))

    return views_found


def validate_single_view(view_tuple: tuple) -> tuple:
    """Executes 'SELECT * LIMIT 0' health check on a single view."""
    v_db, v_schema, v_view = view_tuple
    fq_name = f'"{v_db}"."{v_schema}"."{v_view}"'

    cs, ctx = borrow_connection()
    try:
        cs.execute(f"SELECT * FROM {fq_name} LIMIT 0")
        logging.info(f"OK - {fq_name}")
        return (v_db, v_schema, v_view, "OK", "")
    except Exception as exc:
        err_msg = str(exc).replace("\n", " ").replace("\r", " ")
        logging.error(f"FAILED - {fq_name}: {err_msg}")
        return (v_db, v_schema, v_view, "FAILED", err_msg)
    finally:
        return_connection((cs, ctx))


def generate_failed_csv_report(failed_results: list) -> str:
    """Generates a flat CSV report containing ONLY failed views."""
    output = io.StringIO()
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(
        ["Database", "Schema", "View Name", "Status", "Error Message"]
    )
    for row in failed_results:
        writer.writerow(row)
    return output.getvalue()


# =========================================================================
# EMAIL NOTIFICATIONS
# =========================================================================
def trigger_email_notification(
    metameta_name: str,
    total_views: int,
    passed_cnt: int,
    failed_cnt: int,
    failed_results: list = None,
    db_schema_map: dict = None,
    excluded_views_map: dict = None,
) -> None:
    """Prepares HTML body and dispatches alert email."""
    to_list = ["bhangaleg@careoregon.org", "kalea@careoregon.org"]
    cc_list = []

    is_success = failed_cnt == 0
    status_label = "PASSED" if is_success else "FAILED"
    subject = (
        f"Snowflake View Validation Report: {metameta_name} - {status_label}"
    )

    target_db_rows = ""
    if db_schema_map:
        for db_name, schemas_set in db_schema_map.items():
            schemas_str = (
                ", ".join(sorted(schemas_set))
                if schemas_set
                else "<em>All Schemas</em>"
            )
            exc_views = (
                sorted(excluded_views_map.get(db_name, []))
                if excluded_views_map
                else []
            )
            exc_str = ", ".join(exc_views) if exc_views else "<em>None</em>"

            target_db_rows += f"""
            <tr>
                <td style="padding: 8px 12px; border: 1px solid #dee2e6; background-color: #fcfcfc;"><b>{db_name}</b></td>
                <td style="padding: 8px 12px; border: 1px solid #dee2e6;">{schemas_str}</td>
                <td style="padding: 8px 12px; border: 1px solid #dee2e6; color: #666;">{exc_str}</td>
            </tr>
            """
    else:
        target_db_rows = '<tr><td colspan="3" style="padding: 8px 12px; border: 1px solid #dee2e6; color: #777;">No database mapping available</td></tr>'

    target_db_table = f"""
    <h3>Targeted Databases & Exclusions</h3>
    <table style="border-collapse: collapse; width: 100%; max-width: 750px; font-size: 13px; text-align: left; margin-bottom: 25px; border: 1px solid #dee2e6;">
        <thead>
            <tr style="background-color: #343a40; color: #ffffff;">
                <th style="padding: 8px 12px; border: 1px solid #343a40; width: 25%;">Database</th>
                <th style="padding: 8px 12px; border: 1px solid #343a40; width: 40%;">Target Schemas</th>
                <th style="padding: 8px 12px; border: 1px solid #343a40; width: 35%;">Excluded Views</th>
            </tr>
        </thead>
        <tbody>
            {target_db_rows}
        </tbody>
    </table>
    """

    summary_table = f"""
    <h3>Execution Summary</h3>
    <table style="border-collapse: collapse; width: 350px; margin-bottom: 20px; border: 1px solid #dee2e6;">
        <tr style="background-color: #f8f9fa;">
            <td style="padding: 8px 12px; border: 1px solid #dee2e6;"><b>Metameta Target</b></td>
            <td style="padding: 8px 12px; border: 1px solid #dee2e6;">{metameta_name}</td>
        </tr>
        <tr>
            <td style="padding: 8px 12px; border: 1px solid #dee2e6;"><b>Total Views Validated</b></td>
            <td style="padding: 8px 12px; border: 1px solid #dee2e6;">{total_views}</td>
        </tr>
        <tr style="background-color: #f8f9fa;">
            <td style="padding: 8px 12px; border: 1px solid #dee2e6;"><b>Passed Views</b></td>
            <td style="padding: 8px 12px; border: 1px solid #dee2e6; color: #2e7d32; font-weight: bold;">{passed_cnt}</td>
        </tr>
        <tr>
            <td style="padding: 8px 12px; border: 1px solid #dee2e6;"><b>Failed Views</b></td>
            <td style="padding: 8px 12px; border: 1px solid #dee2e6; color: {'#d32f2f' if failed_cnt > 0 else '#333'}; font-weight: bold;">{failed_cnt}</td>
        </tr>
    </table>
    """

    if is_success:
        detail_section = '<p style="color: #2e7d32; font-weight: bold;">All views are healthy and operating normally. No action required.</p>'
    else:
        failed_table_rows = ""
        for db, schema, view, status, err in failed_results or []:
            failed_table_rows += f"""
            <tr>
                <td style="padding: 8px 12px; border: 1px solid #dee2e6;">{db}</td>
                <td style="padding: 8px 12px; border: 1px solid #dee2e6;">{schema}</td>
                <td style="padding: 8px 12px; border: 1px solid #dee2e6;"><b>{view}</b></td>
                <td style="padding: 8px 12px; border: 1px solid #dee2e6; color: #d32f2f; font-weight: bold;">{status}</td>
                <td style="padding: 8px 12px; border: 1px solid #dee2e6; font-family: Consolas, monospace; font-size: 12px; color: #555;">{err}</td>
            </tr>
            """
        detail_section = f"""
        <h3>Failed Views Result Details</h3>
        <table style="border-collapse: collapse; width: 100%; font-size: 13px; text-align: left; border: 1px solid #dee2e6;">
            <thead>
                <tr style="background-color: #343a40; color: #ffffff;">
                    <th style="padding: 10px 12px; border: 1px solid #343a40;">Database</th>
                    <th style="padding: 10px 12px; border: 1px solid #343a40;">Schema</th>
                    <th style="padding: 10px 12px; border: 1px solid #343a40;">View Name</th>
                    <th style="padding: 10px 12px; border: 1px solid #343a40;">Status</th>
                    <th style="padding: 10px 12px; border: 1px solid #343a40;">Error Details</th>
                </tr>
            </thead>
            <tbody>
                {failed_table_rows}
            </tbody>
        </table>
        """

    header_color = "#2e7d32" if is_success else "#d32f2f"
    status_text = (
        "Execution Succeeded" if is_success else "Execution Encountered Failures"
    )

    html_body = f"""
    <html>
    <body style="font-family: Arial, sans-serif; color: #333; line-height: 1.6;">
        <h2 style="color: {header_color}; border-bottom: 2px solid {header_color}; padding-bottom: 8px;">
            Snowflake View Health Check {status_text}
        </h2>
        <p>Hello,</p>
        <p>The automated Snowflake view health check completed with <b>{failed_cnt} failure(s)</b> across <b>{total_views} view(s)</b>.</p>
        {summary_table}
        {target_db_table}
        {detail_section}
        <br>
        <p>Regards,<br><b>Automated Azure Function Validation Pipeline</b></p>
    </body>
    </html>
    """

    send_email(subject=subject, body=html_body, to=to_list, cc=cc_list)


# =========================================================================
# TIMER TRIGGER MAIN ENTRYPOINT
# =========================================================================
def main(mytimer: func.TimerRequest) -> None:
    start_time = time.perf_counter()
    utc_timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    if mytimer.past_due:
        logging.info("The timer trigger is running late!")

    logging.info(
        f"snowflake_view_validator_multi_thread - Scheduled run started at {utc_timestamp}."
    )

    # Optional environment gate check
    envs = os.getenv("environment", "prd").strip().lower()
    if envs != "prd":
        logging.warning(
            f"snowflake_view_validator - Environment is '{envs}'. Skipping run."
        )
        return

    metameta_name = os.getenv("TARGET_METAMETA_NAME", "View_validation")

    try:
        # 1. Retrieve Metadata & Per-Entity View Exclusions
        try:
            metadata = get_metameta_dict(db_name=metameta_name)
            logging.info(
                f"Successfully loaded '{metameta_name}' from Azure Blob Storage."
            )
            all_db_schema_map, excluded_views_map = (
                extract_all_db_schema_targets(metadata)
            )
        except Exception as exc:
            logging.error(
                f"Failed to fetch metameta file for '{metameta_name}': {exc}"
            )
            return

        if not all_db_schema_map:
            logging.error(
                f"No valid database targets found inside metameta file '{metameta_name}'."
            )
            return

        # 2. INITIALIZE CONNECTION POOL
        max_workers = int(os.getenv("VALIDATION_MAX_WORKERS", "4"))
        init_connection_pool(pool_size=max_workers)

        all_results = []
        try:
            # Discovery Phase
            all_views = []
            for db, schemas in all_db_schema_map.items():
                db_excluded_views = excluded_views_map.get(db, set())
                db_views = fetch_views_for_db(db, schemas, db_excluded_views)
                all_views.extend(db_views)

            if not all_views:
                logging.info(
                    f"No views discovered for metameta file '{metameta_name}'. Execution finished."
                )
            else:
                # Parallel Validation Phase
                logging.info(
                    f"Validating {len(all_views)} views across {max_workers} worker threads..."
                )
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    all_results = list(
                        executor.map(validate_single_view, all_views)
                    )

        finally:
            close_connection_pool()

        failed_results = [res for res in all_results if res[3] == "FAILED"]
        total_count = len(all_results)
        failed_count = len(failed_results)
        passed_count = total_count - failed_count

        logging.info(
            f"[SUMMARY] Total: {total_count} | Passed: {passed_count} | Failed: {failed_count}"
        )

        # 3. CSV Report Generation (Using Temp Directory)
        if failed_results:
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            filename = f"view_validation_{metameta_name}_{timestamp}.csv"
            csv_data = generate_failed_csv_report(failed_results)

            try:
                local_path = os.path.join(tempfile.gettempdir(), filename)
                with open(local_path, "w", newline="", encoding="utf-8") as f:
                    f.write(csv_data)
                logging.info(f"Local CSV report saved to: {local_path}")
            except Exception as local_file_err:
                logging.warning(
                    f"Unable to write temporary CSV copy: {local_file_err}"
                )

        # 4. Email Notification
        end_time = time.perf_counter()
        execution_time_seconds = round(end_time - start_time, 2)
        logging.info(
            f"Total execution finished in {execution_time_seconds} seconds."
        )

        try:
            trigger_email_notification(
                metameta_name=metameta_name,
                total_views=total_count,
                passed_cnt=passed_count,
                failed_cnt=failed_count,
                failed_results=failed_results,
                db_schema_map=all_db_schema_map,
                excluded_views_map=excluded_views_map,
            )
        except Exception as notif_err:
            logging.error(
                f"[NOTIFICATIONS] Notification dispatch failure: {notif_err}",
                exc_info=True,
            )

        # 5. Build Response Payload & Output to Logs
        is_success = failed_count == 0
        response_payload = {
            "metameta_name": metameta_name,
            "status": "PASSED" if is_success else "FAILED",
            "summary": {
                "total_views": total_count,
                "passed": passed_count,
                "failed": failed_count,
                "execution_time_seconds": execution_time_seconds,
            },
            "failed_views": failed_results,
        }

        logging.info(
            f"[EXECUTION_RESULT] Final Run Summary:\n{json.dumps(response_payload, indent=2)}"
        )

    except Exception as fatal_error:
        logging.error(
            f"[CRITICAL] Unhandled top-level exception in scheduled job: {fatal_error}",
            exc_info=True,
        )
