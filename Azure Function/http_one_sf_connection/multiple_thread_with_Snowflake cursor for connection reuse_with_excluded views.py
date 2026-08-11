import csv
import io
import json
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
import azure.functions as func

# Note: get_metameta_dict, get_entity_key_value, get_snowflake_connection,
# and send_email are assumed to be imported from helper modules.

# Thread-Local Storage and Connection Tracking
_thread_local = threading.local()
_open_connections = []
_conn_lock = threading.Lock()


def get_thread_snowflake_cursor():
    """Retrieves or creates a thread-local Snowflake cursor for connection reuse."""
    if not hasattr(_thread_local, "cursor"):
        cs, ctx = get_snowflake_connection()
        _thread_local.cursor = cs
        _thread_local.context = ctx

        with _conn_lock:
            _open_connections.append((cs, ctx))

    return _thread_local.cursor


def cleanup_thread_connections():
    """Safely closes all Snowflake connections across worker threads."""
    with _conn_lock:
        for cs, ctx in _open_connections:
            try:
                cs.close()
                ctx.close()
            except Exception as e:
                logging.warning(f"Error closing thread-local Snowflake connection: {e}")
        _open_connections.clear()


def extract_all_db_schema_targets(metadata):
    """
    Parses metameta dictionary and extracts:
    1. db_schema_map: DB -> Set of target schemas
    2. excluded_views_map: DB -> Set of excluded views (per entity)
    """
    db_schema_map = {}
    excluded_views_map = {}

    entities = metadata.get("entities", [])
    if not isinstance(entities, list):
        entities = [entities]

    if not entities:
        top_db = get_entity_key_value("destination_database", None, metadata) or get_entity_key_value("source_database", None, metadata)
        top_schema = get_entity_key_value("destination_schema", None, metadata) or get_entity_key_value("default_source_schema", None, metadata) or ""
        if top_db:
            top_db_clean = top_db.strip().upper()
            db_schema_map.setdefault(top_db_clean, set()).add(top_schema.strip().upper())
            excluded_views_map.setdefault(top_db_clean, set())

    for ent in entities:
        db = get_entity_key_value("destination_database", ent, metadata) or get_entity_key_value("source_database", ent, metadata)
        if not db or not str(db).strip():
            continue

        db_clean = str(db).strip().upper()
        db_schema_map.setdefault(db_clean, set())
        excluded_views_map.setdefault(db_clean, set())

        # Target Schemas
        schemas = (
            get_entity_key_value("destination_schemas", ent, metadata)
            or get_entity_key_value("destination_schema", ent, metadata)
            or get_entity_key_value("source_schema", ent, metadata)
            or ""
        )

        if isinstance(schemas, list):
            for s in schemas:
                if str(s).strip():
                    db_schema_map[db_clean].add(str(s).strip().upper())
        else:
            if str(schemas).strip():
                db_schema_map[db_clean].add(str(schemas).strip().upper())

        # --- PER-ENTITY VIEW EXCLUSIONS ONLY ---
        exc_views = ent.get("exclude_views", []) or ent.get("excluded_views", [])
        if not isinstance(exc_views, list):
            exc_views = [exc_views]

        for ev in exc_views:
            if str(ev).strip():
                excluded_views_map[db_clean].add(str(ev).strip().upper())

    return db_schema_map, excluded_views_map


def fetch_views_for_db(target_db, target_schemas, excluded_views=None):
    """
    Discovers views using target_db.INFORMATION_SCHEMA.VIEWS
    filtered directly by target schemas and per-entity excluded views.
    """
    logging.info(f"Fetching views for Snowflake database: '{target_db}'...")
    views_found = []
    cs, ctx = get_snowflake_connection()
    try:
        where_clauses = []

        # 1. Target Schemas IN clause
        schemas_list = [s for s in target_schemas if s]
        if schemas_list:
            schema_list_str = ", ".join([f"'{s}'" for s in schemas_list])
            where_clauses.append(f"TABLE_SCHEMA IN ({schema_list_str})")

        # 2. Excluded Views NOT IN clause
        if excluded_views:
            plain_views = []
            fq_views = []
            for v in excluded_views:
                if "." in v:
                    fq_views.append(f"'{v}'")  # Fully qualified: 'SCHEMA.VIEW'
                else:
                    plain_views.append(f"'{v}'")  # Plain: 'VIEW_NAME'

            if plain_views:
                plain_str = ", ".join(plain_views)
                where_clauses.append(f"TABLE_NAME NOT IN ({plain_str})")
            if fq_views:
                fq_str = ", ".join(fq_views)
                where_clauses.append(f"(TABLE_SCHEMA || '.' || TABLE_NAME) NOT IN ({fq_str})")

        where_stmt = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

        query = f"""
            SELECT
                TABLE_SCHEMA,
                TABLE_NAME
            FROM "{target_db}".INFORMATION_SCHEMA.VIEWS
            {where_stmt};
        """

        cs.execute(query)

        for row in cs.fetchall():
            v_schema = row[0].upper()
            v_name = row[1].upper()
            views_found.append((target_db, v_schema, v_name))

        logging.info(f"Discovered {len(views_found)} view(s) in Snowflake DB '{target_db}'.")
    except Exception as exc:
        logging.error(f"Failed to fetch views for DB '{target_db}': {exc}")
    finally:
        cs.close()
        ctx.close()

    return views_found


def validate_single_view(view_tuple):
    """Executes 'SELECT * LIMIT 0' health check on a single view using a thread-reused cursor."""
    v_db, v_schema, v_view = view_tuple
    fq_name = f'"{v_db}"."{v_schema}"."{v_view}"'

    try:
        cs = get_thread_snowflake_cursor()
        cs.execute(f"SELECT * FROM {fq_name} LIMIT 0")
        logging.info(f"OK - {fq_name}")
        return (v_db, v_schema, v_view, "OK", "")
    except Exception as exc:
        err_msg = str(exc).replace("\n", " ").replace("\r", " ")
        logging.error(f"FAILED - {fq_name}: {err_msg}")
        return (v_db, v_schema, v_view, "FAILED", err_msg)


def generate_failed_csv_report(failed_results):
    """Generates a flat CSV report containing ONLY failed views."""
    output = io.StringIO()
    writer = csv.writer(output, lineterminator='\n')
    writer.writerow(["Database", "Schema", "View Name", "Status", "Error Message"])
    for row in failed_results:
        writer.writerow(row)
    return output.getvalue()


def trigger_email_notification(metameta_name, total_views, passed_cnt, failed_cnt, failed_results=None, db_schema_map=None, excluded_views_map=None):
    """Prepares HTML body with summary, target DB/schema mapping & excluded views table, then calls send_email()."""
    to_env = os.environ.get("NOTIFICATION_EMAIL_TO", "bhangaleg@careoregon.org")
    cc_env = os.environ.get("NOTIFICATION_EMAIL_CC", "")

    to_list = [addr.strip() for addr in to_env.split(",") if addr.strip()]
    cc_list = [addr.strip() for addr in cc_env.split(",") if addr.strip()]

    is_success = failed_cnt == 0
    subject = f"Snowflake View Validation Report: {metameta_name} - {'PASSED' if is_success else 'FAILED'}"

    # Build Target DB & Schemas Table with View Exclusions
    target_db_rows = ""
    if db_schema_map:
        for db_name, schemas_set in db_schema_map.items():
            schemas_str = ", ".join(sorted(schemas_set)) if schemas_set else "<em>All Schemas</em>"
            
            # Format View Exclusions
            exc_views = sorted(excluded_views_map.get(db_name, [])) if excluded_views_map else []
            exc_str = ", ".join(exc_views) if exc_views else "<em>None</em>"

            target_db_rows += f"""
            <tr>
                <td style="padding: 8px 12px; border: 1px solid #dee2e6; background-color: #fcfcfc;"><b>{db_name}</b></td>
                <td style="padding: 8px 12px; border: 1px solid #dee2e6;">{schemas_str}</td>
                <td style="padding: 8px 12px; border: 1px solid #dee2e6; color: #666;">{exc_str}</td>
            </tr>
            """
    else:
        target_db_rows = """
        <tr>
            <td colspan="3" style="padding: 8px 12px; border: 1px solid #dee2e6; color: #777;">No database mapping available</td>
        </tr>
        """

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

    if is_success:
        html_body = f"""
        <html>
        <body style="font-family: Arial, sans-serif; color: #333; line-height: 1.6;">
            <h2 style="color: #2e7d32; border-bottom: 2px solid #2e7d32; padding-bottom: 8px;">
                Snowflake View Health Check Execution Succeeded
            </h2>
            <p>Hello,</p>
            <p>The automated Snowflake view health check completed successfully with <b>no validation errors</b>.</p>

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
                    <td style="padding: 8px 12px; border: 1px solid #dee2e6;">0</td>
                </tr>
            </table>

            {target_db_table}

            <p style="color: #2e7d32; font-weight: bold;">All views are healthy and operating normally. No action required.</p>
            <br>
            <p>Regards,<br><b>Automated Azure Function Validation Pipeline</b></p>
        </body>
        </html>
        """
    else:
        failed_table_rows = ""
        for db, schema, view, status, err in (failed_results or []):
            failed_table_rows += f"""
            <tr>
                <td style="padding: 8px 12px; border: 1px solid #dee2e6;">{db}</td>
                <td style="padding: 8px 12px; border: 1px solid #dee2e6;">{schema}</td>
                <td style="padding: 8px 12px; border: 1px solid #dee2e6;"><b>{view}</b></td>
                <td style="padding: 8px 12px; border: 1px solid #dee2e6; color: #d32f2f; font-weight: bold;">{status}</td>
                <td style="padding: 8px 12px; border: 1px solid #dee2e6; font-family: Consolas, monospace; font-size: 12px; color: #555;">{err}</td>
            </tr>
            """

        html_body = f"""
        <html>
        <body style="font-family: Arial, sans-serif; color: #333; line-height: 1.6;">
            <h2 style="color: #d32f2f; border-bottom: 2px solid #d32f2f; padding-bottom: 8px;">
                Snowflake View Health Check Execution Failed
            </h2>
            <p>Hello,</p>
            <p>The automated Snowflake view health check encountered <b>{failed_cnt} validation failure(s)</b>.</p>

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
                    <td style="padding: 8px 12px; border: 1px solid #dee2e6; color: #d32f2f; font-weight: bold;">{failed_cnt}</td>
                </tr>
            </table>

            {target_db_table}

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
            <br>
            <p>Regards,<br><b>Automated Azure Function Validation Pipeline</b></p>
        </body>
        </html>
        """

    send_email(subject=subject, body=html_body, to=to_list, cc=cc_list)


def main(req: func.HttpRequest) -> func.HttpResponse:
    start_time = time.perf_counter()
    logging.info('snowflake_view_validator() - Python HTTP trigger function processed a request.')

    try:
        # 1. Parameter Parsing
        metameta_name = req.params.get('metameta_name')
        if not metameta_name:
            try:
                req_body = req.get_json()
            except ValueError:
                pass
            else:
                if req_body:
                    metameta_name = req_body.get('metameta_name')

        if not metameta_name:
            return func.HttpResponse(
                json.dumps({"status": "ERROR", "message": "Please pass 'metameta_name' in the URL query string (e.g. ?metameta_name=View_validation)."}),
                status_code=400,
                mimetype="application/json"
            )

        # 2. Retrieve Metadata & Per-Entity View Exclusions
        try:
            metadata = get_metameta_dict(db_name=metameta_name)
            logging.info(f"Successfully loaded '{metameta_name}' from Azure Blob Storage.")
            
            all_db_schema_map, excluded_views_map = extract_all_db_schema_targets(metadata)
        except Exception as exc:
            logging.error(f"Failed to fetch metameta file for '{metameta_name}': {exc}")
            return func.HttpResponse(
                json.dumps({"status": "ERROR", "message": f"Failed to load metameta file '{metameta_name}': {str(exc)}"}),
                status_code=500,
                mimetype="application/json"
            )

        if not all_db_schema_map:
            return func.HttpResponse(
                json.dumps({"status": "ERROR", "message": f"No valid database targets found inside metameta file '{metameta_name}'."}),
                status_code=400,
                mimetype="application/json"
            )

        # 3. View Discovery via INFORMATION_SCHEMA with View Exclusions
        all_views = []
        for db, schemas in all_db_schema_map.items():
            db_excluded_views = excluded_views_map.get(db, set())
            db_views = fetch_views_for_db(db, schemas, db_excluded_views)
            all_views.extend(db_views)

        if not all_views:
            total_time = round(time.perf_counter() - start_time, 2)
            return func.HttpResponse(
                json.dumps({
                    "status": "SUCCESS",
                    "message": f"No views discovered for metameta file '{metameta_name}'.",
                    "summary": {"total_views": 0, "passed": 0, "failed": 0, "execution_time_seconds": total_time}
                }),
                status_code=200,
                mimetype="application/json"
            )

        # 4. Multi-Threaded View Validation
        max_workers = int(os.environ.get("VALIDATION_MAX_WORKERS", "10"))
        logging.info(f"Starting parallel validation for {len(all_views)} views using {max_workers} worker threads...")

        all_results = []
        failed_results = []

        try:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                all_results = list(executor.map(validate_single_view, all_views))
        finally:
            cleanup_thread_connections()

        for res in all_results:
            if res[3] == "FAILED":
                failed_results.append(res)

        total_count = len(all_results)
        failed_count = len(failed_results)
        passed_count = total_count - failed_count

        logging.info(f"[SUMMARY] Health Check Complete -> Total: {total_count} | Passed: {passed_count} | Failed: {failed_count}")

        # --- CSV Report Generation ---
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        filename = f"view_validation_{metameta_name}_{timestamp}.csv"
        csv_data = generate_failed_csv_report(failed_results)

        try:
            local_path = os.path.join(os.getcwd(), filename)
            with open(local_path, "w", newline="", encoding="utf-8") as f:
                f.write(csv_data)
            logging.info(f"Local CSV report saved to: {local_path}")
        except Exception as local_file_err:
            logging.warning(f"Unable to write local CSV report copy: {local_file_err}")

        # 5. Response & Notifications
        end_time = time.perf_counter()
        execution_time_seconds = round(end_time - start_time, 2)

        try:
            trigger_email_notification(
                metameta_name=metameta_name,
                total_views=total_count,
                passed_cnt=passed_count,
                failed_cnt=failed_count,
                failed_results=failed_results,
                db_schema_map=all_db_schema_map,
                excluded_views_map=excluded_views_map
            )
        except Exception as notif_err:
            logging.error(f"[NOTIFICATIONS] Notification dispatch failure: {notif_err}", exc_info=True)

        is_success = failed_count == 0
        response_payload = {
            "metameta_name": metameta_name,
            "status": "PASSED" if is_success else "FAILED",
            "summary": {
                "total_views": total_count,
                "passed": passed_count,
                "failed": failed_count,
                "execution_time_seconds": execution_time_seconds
            },
            "failed_views": failed_results
        }

        return func.HttpResponse(
            json.dumps(response_payload, indent=2),
            status_code=200 if is_success else 422,
            mimetype="application/json"
        )

    except Exception as fatal_error:
        logging.error(f"[CRITICAL] Unhandled top-level exception in main(): {fatal_error}", exc_info=True)
        return func.HttpResponse(
            json.dumps({
                "status": "CRITICAL_ERROR",
                "message": f"An unhandled internal server error occurred: {str(fatal_error)}"
            }, indent=2),
            status_code=500,
            mimetype="application/json"
        )
