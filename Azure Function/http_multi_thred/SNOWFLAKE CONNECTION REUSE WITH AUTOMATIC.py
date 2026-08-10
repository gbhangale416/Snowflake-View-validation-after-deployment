import json
import logging
import os
import io
import csv
import smtplib
import time
import threading
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from concurrent.futures import ThreadPoolExecutor, as_completed
import azure.functions as func
import requests

# Import existing utility functions from utility.py
from utility import (
    get_snowflake_connection,
    get_metameta_dict,
    get_entity_key_value
)

# =============================================================================
# THREAD-LOCAL SNOWFLAKE CONNECTION REUSE WITH AUTOMATIC RECONNECT
# =============================================================================
_thread_local = threading.local()

def get_thread_snowflake_cursor():
    """
    Reuses an existing Snowflake connection per worker thread.
    Automatically reconnects if the connection was dropped or invalidated.
    """
    try:
        if hasattr(_thread_local, "ctx") and _thread_local.ctx and not _thread_local.ctx.is_closed():
            return _thread_local.cs
    except Exception as exc:
        logging.warning(f"⚠️ [SNOWFLAKE] Thread connection stale/unusable. Reconnecting... Details: {exc}")

    try:
        logging.info("🔌 [SNOWFLAKE] Initializing thread-local connection...")
        _thread_local.cs, _thread_local.ctx = get_snowflake_connection()
        return _thread_local.cs
    except Exception as exc:
        logging.error(f"❌ [SNOWFLAKE] Failed to establish connection on thread {threading.get_ident()}: {exc}")
        raise exc


def close_thread_snowflake_connections():
    """Safely closes thread-local connection if active."""
    try:
        if hasattr(_thread_local, "ctx") and _thread_local.ctx and not _thread_local.ctx.is_closed():
            _thread_local.cs.close()
            _thread_local.ctx.close()
            logging.info("🔌 [SNOWFLAKE] Closed thread-local connection.")
    except Exception as exc:
        logging.warning(f"⚠️ [SNOWFLAKE] Error closing thread-local connection: {exc}")


# =============================================================================
# EXISTING EMAIL FUNCTION FROM UTILITY / SCRIPT
# =============================================================================
def send_email(subject, body, to, cc):
    logging.info("📧 [EMAIL] Executing send_email()...")
    try:
        envs = os.environ.get('envs', os.environ.get('ENVIRONMENT', 'dev')).lower()
        password = os.environ['WebMailAccountADFPassword']
        username = os.environ['WebMailAccountADFUsername']

        host = "smtp.office365.com"
        port = 587
        mail_from = "analyticsupporttest@careoregon.org"
        
        mimemsg = MIMEMultipart()
        mimemsg['from'] = mail_from
        mimemsg['to'] = ','.join(to) if isinstance(to, list) else to
        mimemsg['cc'] = ','.join(cc) if isinstance(cc, list) else cc
        mimemsg['subject'] = f"[{'PREPROD' if envs == 'uat' else envs.upper()}]: {subject}"
        mimemsg.attach(MIMEText(body, 'html'))
        
        connection = smtplib.SMTP(host=host, port=port)
        connection.starttls()
        connection.login(username, password)
        connection.send_message(mimemsg)
        connection.quit()
        logging.info("✅ [EMAIL] Email sent successfully.")

    except KeyError as missing_env:
        logging.error(f"❌ [EMAIL] Missing required environment variable: {missing_env}")
    except Exception as error:
        logging.error(f"❌ [EMAIL] Unexpected error while sending email: {error}", exc_info=True)


# =============================================================================
# HELPER: Send Microsoft Teams Notification via Webhook
# =============================================================================
def send_teams_notification(webhook_url, metameta_name, total_views, passed_cnt, failed_cnt, failed_list=None):
    """Posts an Adaptive Card / MessageCard notification to MS Teams."""
    logging.info("📢 [TEAMS] Preparing MS Teams webhook notification...")
    try:
        if not webhook_url:
            logging.warning("⚠️ [TEAMS] TEAMS_WEBHOOK_URL not configured. Skipping Teams notification.")
            return

        failed_list = failed_list or []
        is_success = failed_cnt == 0
        status_color = "00FF00" if is_success else "FF0000"
        status_title = "✅ View Validation Passed" if is_success else "❌ View Validation Failed"

        facts = [
            {"name": "Metadata Target:", "value": metameta_name},
            {"name": "Total Views Tested:", "value": str(total_views)},
            {"name": "Passed Views:", "value": str(passed_cnt)},
            {"name": "Failed Views:", "value": str(failed_cnt)}
        ]

        if is_success:
            failure_details = "\n\n🎉 All Snowflake views are healthy and validated successfully!"
        else:
            failure_details = "\n\n**Failed Views Summary:**\n"
            for item in failed_list[:5]:
                db, schema, view, _, err = item
                failure_details += f"* `{db}.{schema}.{view}` — {err[:80]}...\n"
            if failed_cnt > 5:
                failure_details += f"\n*...and {failed_cnt - 5} more. Check email report for full table.*"

        payload = {
            "@type": "MessageCard",
            "@context": "http://schema.org/extensions",
            "themeColor": status_color,
            "title": status_title,
            "text": f"Snowflake view validation execution completed for **{metameta_name}**.{failure_details}",
            "sections": [{
                "facts": facts
            }]
        }

        resp = requests.post(webhook_url, json=payload, timeout=10)
        if resp.status_code == 200:
            logging.info("✅ [TEAMS] Webhook notification delivered successfully.")
        else:
            logging.error(f"❌ [TEAMS] Failed to deliver webhook. HTTP {resp.status_code}: {resp.text}")
    except Exception as exc:
        logging.error(f"❌ [TEAMS] Exception during Teams webhook dispatch: {exc}", exc_info=True)


# =============================================================================
# HELPER: Build HTML Table Email & Trigger send_email()
# =============================================================================
def trigger_email_notification(metameta_name, total_views, passed_cnt, failed_cnt, failed_results=None):
    """Prepares HTML body with summary & inline results table, then calls send_email()."""
    try:
        to_env = os.environ.get("NOTIFICATION_EMAIL_TO", "analyticsupporttest@careoregon.org")
        cc_env = os.environ.get("NOTIFICATION_EMAIL_CC", "")

        to_list = [addr.strip() for addr in to_env.split(",") if addr.strip()]
        cc_list = [addr.strip() for addr in cc_env.split(",") if addr.strip()]

        is_success = failed_cnt == 0
        subject = f"Snowflake View Validation Report: {metameta_name} - {'PASSED' if is_success else 'FAILED'}"

        if is_success:
            html_body = f"""
            <html>
            <body style="font-family: Arial, sans-serif; color: #333; line-height: 1.6;">
                <h2 style="color: #2e7d32;">✅ Snowflake View Health Check Execution Succeeded!</h2>
                <p>Hello Data Team,</p>
                <p>The automated Snowflake view health check has completed successfully with no errors.</p>
                
                <h3>Execution Summary</h3>
                <table style="border-collapse: collapse; width: 350px; margin-bottom: 20px;">
                    <tr style="background-color: #f8f9fa;">
                        <td style="padding: 8px; border: 1px solid #dee2e6;"><b>Metameta Target</b></td>
                        <td style="padding: 8px; border: 1px solid #dee2e6;">{metameta_name}</td>
                    </tr>
                    <tr>
                        <td style="padding: 8px; border: 1px solid #dee2e6;"><b>Total Views Validated</b></td>
                        <td style="padding: 8px; border: 1px solid #dee2e6;">{total_views}</td>
                    </tr>
                    <tr style="background-color: #f8f9fa;">
                        <td style="padding: 8px; border: 1px solid #dee2e6;"><b>Passed Views</b></td>
                        <td style="padding: 8px; border: 1px solid #dee2e6; color: #2e7d32; font-weight: bold;">{passed_cnt}</td>
                    </tr>
                    <tr>
                        <td style="padding: 8px; border: 1px solid #dee2e6;"><b>Failed Views</b></td>
                        <td style="padding: 8px; border: 1px solid #dee2e6;">0</td>
                    </tr>
                </table>

                <p>All views are healthy and working properly. No action required.</p>
                <br>
                <p>Regards,<br><b>Automated Azure Function Validation Pipeline</b></p>
            </body>
            </html>
            """
        else:
            table_rows = ""
            for item in (failed_results or []):
                if isinstance(item, dict):
                    db = item.get("database", "")
                    schema = item.get("schema", "")
                    view = item.get("view", "")
                    status = "FAILED"
                    err = item.get("error", "")
                else:
                    db, schema, view, status, err = item

                table_rows += f"""
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
                <h2 style="color: #d32f2f;">❌ Snowflake View Health Check Execution Failed!</h2>
                <p>Hello Data Team,</p>
                <p>The automated Snowflake view health check encountered validation failures.</p>
                
                <h3>Execution Summary</h3>
                <table style="border-collapse: collapse; width: 350px; margin-bottom: 20px;">
                    <tr style="background-color: #f8f9fa;">
                        <td style="padding: 8px; border: 1px solid #dee2e6;"><b>Metameta Target</b></td>
                        <td style="padding: 8px; border: 1px solid #dee2e6;">{metameta_name}</td>
                    </tr>
                    <tr>
                        <td style="padding: 8px; border: 1px solid #dee2e6;"><b>Total Views Validated</b></td>
                        <td style="padding: 8px; border: 1px solid #dee2e6;">{total_views}</td>
                    </tr>
                    <tr style="background-color: #f8f9fa;">
                        <td style="padding: 8px; border: 1px solid #dee2e6;"><b>Passed Views</b></td>
                        <td style="padding: 8px; border: 1px solid #dee2e6;">{passed_cnt}</td>
                    </tr>
                    <tr>
                        <td style="padding: 8px; border: 1px solid #dee2e6;"><b>Failed Views</b></td>
                        <td style="padding: 8px; border: 1px solid #dee2e6; color: #d32f2f; font-weight: bold;">{failed_cnt}</td>
                    </tr>
                </table>

                <h3>Failed Views Result Table</h3>
                <table style="border-collapse: collapse; width: 100%; font-size: 13px; text-align: left;">
                    <thead>
                        <tr style="background-color: #343a40; color: #ffffff;">
                            <th style="padding: 10px 12px; border: 1px solid #343a40;">Database</th>
                            <th style="padding: 10px 12px; border: 1px solid #343a40;">Schema</th>
                            <th style="padding: 10px 12px; border: 1px solid #343a40;">View Name</th>
                            <th style="padding: 10px 12px; border: 1px solid #343a40;">Status</th>
                            <th style="padding: 10px 12px; border: 1px solid #343a40;">Error Message</th>
                        </tr>
                    </thead>
                    <tbody>
                        {table_rows}
                    </tbody>
                </table>
                <br>
                <p>Regards,<br><b>Automated Azure Function Validation Pipeline</b></p>
            </body>
            </html>
            """

        send_email(subject=subject, body=html_body, to=to_list, cc=cc_list)
    except Exception as exc:
        logging.error(f"❌ [EMAIL] trigger_email_notification() failed: {exc}", exc_info=True)


# =============================================================================
# EXTRACT & DISCOVER FUNCTIONS
# =============================================================================
def extract_all_db_schema_targets(metadata):
    """Parses metameta dictionary and extracts real Snowflake DB -> Set of target schemas."""
    db_schema_map = {}
    
    try:
        entities = metadata.get("entities", [])
        if not isinstance(entities, list):
            entities = [entities]

        if not entities:
            top_db = get_entity_key_value("destination_database", None, metadata) or get_entity_key_value("source_database", None, metadata)
            top_schema = get_entity_key_value("destination_schema", None, metadata) or get_entity_key_value("default_source_schema", None, metadata) or ""
            if top_db:
                db_schema_map.setdefault(top_db.strip().upper(), set()).add(top_schema.strip().upper())

        for ent in entities:
            db = get_entity_key_value("destination_database", ent, metadata) or get_entity_key_value("source_database", ent, metadata)
            if not db or not str(db).strip():
                continue

            db = str(db).strip().upper()
            schemas = (
                get_entity_key_value("destination_schemas", ent, metadata)
                or get_entity_key_value("destination_schema", ent, metadata)
                or get_entity_key_value("source_schema", ent, metadata)
                or ""
            )

            if isinstance(schemas, list):
                for s in schemas:
                    db_schema_map.setdefault(db, set()).add(str(s).strip().upper())
            else:
                db_schema_map.setdefault(db, set()).add(str(schemas).strip().upper())

        logging.info(f"📊 [METADATA] Extracted target mapping for {len(db_schema_map)} database(s).")
    except Exception as exc:
        logging.error(f"❌ [METADATA] Error parsing metadata targets: {exc}", exc_info=True)
        raise exc

    return db_schema_map


def fetch_views_for_db(target_db, target_schemas):
    """Batch discovers views inside target_db using thread-local connection."""
    logging.info(f"🔍 [DISCOVERY] Fetching views for Snowflake DB: '{target_db}'...")
    views_found = []

    try:
        cs = get_thread_snowflake_cursor()
        cs.execute(f'USE DATABASE "{target_db}"')
        cs.execute(f'SHOW VIEWS IN DATABASE "{target_db}"')
        columns = [col[0].lower() for col in cs.description]

        schema_idx = columns.index('schema_name') if 'schema_name' in columns else 1
        name_idx = columns.index('name') if 'name' in columns else 2

        for row in cs.fetchall():
            v_schema = row[schema_idx].upper()
            v_name = row[name_idx].upper()

            if v_schema in ('INFORMATION_SCHEMA', 'PUBLIC', 'DEPLOY'):
                continue

            if target_schemas and "" not in target_schemas:
                if v_schema in target_schemas:
                    views_found.append((target_db, v_schema, v_name))
            else:
                views_found.append((target_db, v_schema, v_name))

        logging.info(f"✅ [DISCOVERY] Discovered {len(views_found)} view(s) in Snowflake DB '{target_db}'.")
    except Exception as exc:
        logging.error(f"❌ [DISCOVERY] Failed to fetch views for DB '{target_db}': {exc}", exc_info=True)

    return views_found


def validate_single_view(view_tuple):
    """Executes 'SELECT * LIMIT 0' health check reusing persistent thread connection with single retry."""
    v_db, v_schema, v_view = view_tuple
    fq_name = f'"{v_db}"."{v_schema}"."{v_view}"'

    for attempt in range(2):
        try:
            cs = get_thread_snowflake_cursor()
            cs.execute(f"SELECT * FROM {fq_name} LIMIT 0")
            logging.info(f"✅ [VALIDATE] Passed: {fq_name}")
            return (v_db, v_schema, v_view, "OK", "")
        except Exception as exc:
            err_msg = str(exc).replace("\n", " ").replace("\r", " ")
            
            # Retry once if connection dropped
            if attempt == 0 and any(term in err_msg.lower() for term in ["closed", "connection", "socket", "session"]):
                logging.warning(f"⚠️ [VALIDATE] Connection lost during check of {fq_name}. Retrying once...")
                if hasattr(_thread_local, "ctx"):
                    _thread_local.ctx = None
                continue

            logging.error(f"❌ [VALIDATE] Failed: {fq_name} - Error: {err_msg}")
            return (v_db, v_schema, v_view, "FAILED", err_msg)


def generate_failed_csv_report(results):
    """Generates a flat CSV report containing failed views safely."""
    output = io.StringIO()
    writer = csv.writer(output, lineterminator='\n')

    writer.writerow(["Database", "Schema", "View Name", "Status", "Error Message"])

    try:
        for row in results:
            if isinstance(row, dict):
                writer.writerow([row.get("database", ""), row.get("schema", ""), row.get("view", ""), "FAILED", row.get("error", "")])
            elif len(row) >= 5 and row[3] == "FAILED":
                writer.writerow(row)
    except Exception as exc:
        logging.error(f"❌ [CSV] Error building CSV report content: {exc}", exc_info=True)

    return output.getvalue()


# =============================================================================
# MAIN TRIGGER FUNCTION
# =============================================================================
def main(req: func.HttpRequest) -> func.HttpResponse:
    start_time = time.perf_counter()
    logging.info("🚀 [TRIGGER] psaValidationTrigger() - Azure Function execution started.")

    try:
        # 1. Parse Input Parameters (Query Param or Body JSON)
        metameta_name = req.params.get('metameta_name')
        parallel_workers_param = req.params.get('parallel_workers') or req.params.get('PARALLEL_WORKERS')

        try:
            req_body = req.get_json()
            if isinstance(req_body, dict):
                if not metameta_name:
                    metameta_name = req_body.get('metameta_name')
                if parallel_workers_param is None:
                    parallel_workers_param = req_body.get('parallel_workers') or req_body.get('PARALLEL_WORKERS')
        except Exception:
            logging.info("ℹ️ [PARAMS] No valid JSON body supplied. Relying on query params/defaults.")

        if not metameta_name:
            metameta_name = "View_validation"

        # Determine PARALLEL_WORKERS:
        # - If provided as param -> convert to int
        # - If not provided -> default to 1 (or check environment variable fallback)
        if parallel_workers_param is not None:
            try:
                parallel_workers = int(parallel_workers_param)
            except (ValueError, TypeError):
                logging.warning("⚠️ [CONFIG] Invalid parallel_workers param provided. Defaulting to 1.")
                parallel_workers = 1
        else:
            try:
                parallel_workers = int(os.environ.get("PARALLEL_WORKERS", "1"))
            except ValueError:
                parallel_workers = 1

        # Guarantee parallel_workers is at least 1
        parallel_workers = max(1, parallel_workers)

        logging.info(f"📋 [PARAMS] Metameta Name: '{metameta_name}' | Parallel Workers: {parallel_workers}")
        teams_webhook_url = os.environ.get("TEAMS_WEBHOOK_URL")

        # 2. Retrieve Metadata & Map Targets
        try:
            metadata = get_metameta_dict(db_name=metameta_name)
            all_db_schema_map = extract_all_db_schema_targets(metadata)
        except Exception as exc:
            logging.error(f"❌ [FATAL] Failed to fetch/parse metameta file '{metameta_name}': {exc}", exc_info=True)
            return func.HttpResponse(
                json.dumps({"status": "ERROR", "message": f"Failed to load metameta file '{metameta_name}': {str(exc)}"}),
                status_code=500,
                mimetype="application/json"
            )

        if not all_db_schema_map:
            logging.error(f"❌ [FATAL] No database targets found inside metameta file '{metameta_name}'.")
            return func.HttpResponse(
                json.dumps({"status": "ERROR", "message": f"No valid database targets found inside metameta file '{metameta_name}'."}),
                status_code=400,
                mimetype="application/json"
            )

        # 3. Discover Views
        all_views = []
        try:
            logging.info(f"⚡ [EXECUTION] Starting view discovery (workers={parallel_workers})...")
            with ThreadPoolExecutor(max_workers=min(parallel_workers, len(all_db_schema_map))) as executor:
                futures = [
                    executor.submit(fetch_views_for_db, db, schemas)
                    for db, schemas in all_db_schema_map.items()
                ]
                for future in as_completed(futures):
                    all_views.extend(future.result())
        except Exception as exc:
            logging.error(f"❌ [DISCOVERY] Error during view discovery pool: {exc}", exc_info=True)

        logging.info(f"📊 [SUMMARY] Total views discovered across all target DBs: {len(all_views)}")

        if not all_views:
            return func.HttpResponse(
                json.dumps({
                    "status": "SUCCESS",
                    "message": f"No views discovered for metameta file '{metameta_name}'.",
                    "summary": {"total_views": 0, "passed": 0, "failed": 0}
                }),
                status_code=200,
                mimetype="application/json"
            )

        # 4. View Health Check Validation
        all_results = []
        failed_results = []

        try:
            logging.info(f"⚡ [EXECUTION] Starting view health checks (workers={parallel_workers}) for {len(all_views)} view(s)...")
            with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
                futures = [
                    executor.submit(validate_single_view, view_tuple)
                    for view_tuple in all_views
                ]
                for future in as_completed(futures):
                    res = future.result()
                    all_results.append(res)
                    if res[3] == "FAILED":
                        failed_results.append({
                            "database": res[0],
                            "schema": res[1],
                            "view": res[2],
                            "error": res[4]
                        })
        except Exception as exc:
            logging.error(f"❌ [VALIDATE] Error during view validation pool execution: {exc}", exc_info=True)

        total_count = len(all_results)
        failed_count = len(failed_results)
        passed_count = total_count - failed_count

        logging.info(f"📈 [SUMMARY] Health Check Complete -> Total: {total_count} | Passed: {passed_count} | Failed: {failed_count}")

        # 5. Save Local CSV Copy
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        filename = f"view_validation_{metameta_name}_{timestamp}.csv"
        csv_data = generate_failed_csv_report(failed_results)

        try:
            local_path = os.path.join(os.getcwd(), filename)
            with open(local_path, "w", newline="", encoding="utf-8") as f:
                f.write(csv_data)
            logging.info(f"📁 [CSV] Local report successfully written to: {local_path}")
        except Exception as local_file_err:
            logging.warning(f"⚠️ [CSV] Unable to write local CSV report copy: {local_file_err}")

        # 6. Performance & Execution Timing
        end_time = time.perf_counter()
        execution_time_seconds = round(end_time - start_time, 2)
        logging.info(f"⏱️ [PERFORMANCE] Validation execution completed in {execution_time_seconds} seconds.")

        # 7. Dispatch Notifications
        try:
            if failed_count > 0:
                send_teams_notification(
                    webhook_url=teams_webhook_url,
                    metameta_name=metameta_name,
                    total_views=total_count,
                    passed_cnt=passed_count,
                    failed_cnt=failed_count,
                    failed_list=[(r["database"], r["schema"], r["view"], "FAILED", r["error"]) for r in failed_results]
                )

                trigger_email_notification(
                    metameta_name=metameta_name,
                    total_views=total_count,
                    passed_cnt=passed_count,
                    failed_cnt=failed_count,
                    failed_results=failed_results
                )
            else:
                send_teams_notification(
                    webhook_url=teams_webhook_url,
                    metameta_name=metameta_name,
                    total_views=total_count,
                    passed_cnt=passed_count,
                    failed_cnt=0,
                    failed_list=[]
                )

                trigger_email_notification(
                    metameta_name=metameta_name,
                    total_views=total_count,
                    passed_cnt=passed_count,
                    failed_cnt=0,
                    failed_results=[]
                )
        except Exception as notif_err:
            logging.error(f"❌ [NOTIFICATIONS] Notification dispatch failure: {notif_err}", exc_info=True)

        # 8. Construct Final Response Payload
        is_success = failed_count == 0
        response_payload = {
            "metameta_name": metameta_name,
            "status": "PASSED" if is_success else "FAILED",
            "summary": {
                "total_views": total_count,
                "passed": passed_count,
                "failed": failed_count,
                "parallel_workers": parallel_workers,
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
        logging.error(f"💥 [CRITICAL] Unhandled top-level exception in main(): {fatal_error}", exc_info=True)
        return func.HttpResponse(
            json.dumps({
                "status": "CRITICAL_ERROR",
                "message": f"An unhandled internal server error occurred: {str(fatal_error)}"
            }, indent=2),
            status_code=500,
            mimetype="application/json"
        )
    finally:
        close_thread_snowflake_connections()
