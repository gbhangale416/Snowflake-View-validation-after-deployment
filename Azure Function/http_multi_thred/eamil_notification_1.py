import json
import logging
import os
import io
import csv
import smtplib
import time
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
# EXISTING EMAIL FUNCTION FROM UTILITY / SCRIPT
# =============================================================================
def send_email(subject, body, to, cc):
    logging.info("Start function ......send_email()...")
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
        logging.info("Email sent successfully")

    except Exception as error:
        logging.error(f"send_email()- Unexpected error: {error}")


# =============================================================================
# HELPER: Send Microsoft Teams Notification via Webhook
# =============================================================================
def send_teams_notification(webhook_url, metameta_name, total_views, passed_cnt, failed_cnt, failed_list=None):
    """Posts an Adaptive Card / MessageCard notification to MS Teams."""
    if not webhook_url:
        logging.warning("TEAMS_WEBHOOK_URL not configured. Skipping Teams notification.")
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
        for item in failed_list[:5]:  # Top 5 failures
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

    try:
        resp = requests.post(webhook_url, json=payload, timeout=10)
        if resp.status_code == 200:
            logging.info("Successfully sent Teams notification.")
        else:
            logging.error(f"Failed to send Teams notification: {resp.status_code} - {resp.text}")
    except Exception as exc:
        logging.error(f"Exception sending Teams notification: {exc}")


# =============================================================================
# HELPER: Build HTML Table Email & Trigger send_email()
# =============================================================================
def trigger_email_notification(metameta_name, total_views, passed_cnt, failed_cnt, failed_results=None):
    """Prepares HTML body with summary & inline results table, then calls send_email()."""
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
        for db, schema, view, status, err in (failed_results or []):
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

    # Call your send_email function
    send_email(subject=subject, body=html_body, to=to_list, cc=cc_list)


# =============================================================================
# EXTRACT & DISCOVER FUNCTIONS
# =============================================================================
def extract_all_db_schema_targets(metadata):
    """Parses metameta dictionary and extracts real Snowflake DB -> Set of target schemas."""
    db_schema_map = {}
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

    return db_schema_map


def fetch_views_for_db(target_db, target_schemas):
    """Batch discovers views inside target_db."""
    logging.info(f"Fetching views for Snowflake database: '{target_db}'...")
    views_found = []

    cs, ctx = get_snowflake_connection()

    try:
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

        logging.info(f"Discovered {len(views_found)} view(s) in Snowflake DB '{target_db}'.")
    except Exception as exc:
        logging.error(f"Failed to fetch views for DB '{target_db}': {exc}")
    finally:
        cs.close()
        ctx.close()

    return views_found


def validate_single_view(view_tuple):
    """Executes 'SELECT * LIMIT 0' health check."""
    v_db, v_schema, v_view = view_tuple
    fq_name = f'"{v_db}"."{v_schema}"."{v_view}"'

    cs, ctx = get_snowflake_connection()

    try:
        cs.execute(f"SELECT * FROM {fq_name} LIMIT 0")
        logging.info(f"✅ OK - {fq_name}")
        return (v_db, v_schema, v_view, "OK", "")
    except Exception as exc:
        err_msg = str(exc).replace("\n", " ").replace("\r", " ")
        logging.error(f"❌ FAILED - {fq_name}: {err_msg}")
        return (v_db, v_schema, v_view, "FAILED", err_msg)
    finally:
        cs.close()
        ctx.close()


def generate_failed_flat_csv_report(failed_results):
    """
    Generates a flat CSV report containing ONLY failed views.
    Format: Database, Schema, View Name, Status, Error Message
    """
    output = io.StringIO()
    writer = csv.writer(output, lineterminator='\n')

    # Header
    writer.writerow(["Database", "Schema", "View Name", "Status", "Error Message"])

    # Failed view rows
    for row in failed_results:
        writer.writerow(row)

    return output.getvalue()


# =============================================================================
# MAIN TRIGGER FUNCTION
# =============================================================================
def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('psaValidationTrigger() - Processing view validation request.')

    # 1. Parse Parameters
    metameta_name = req.params.get('metameta_name')
    if not metameta_name:
        try:
            req_body = req.get_json()
        except ValueError:
            req_body = {}
        if req_body:
            metameta_name = req_body.get('metameta_name')

    if not metameta_name:
        metameta_name = "View_validation"

    parallel_workers = int(os.environ.get("PARALLEL_WORKERS", 10))
    teams_webhook_url = os.environ.get("TEAMS_WEBHOOK_URL")

    # 2. Retrieve Metadata and Discover Views
    try:
        metadata = get_metameta_dict(db_name=metameta_name)
        all_db_schema_map = extract_all_db_schema_targets(metadata)
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

    all_views = []
    with ThreadPoolExecutor(max_workers=min(parallel_workers, len(all_db_schema_map))) as executor:
        futures = [
            executor.submit(fetch_views_for_db, db, schemas)
            for db, schemas in all_db_schema_map.items()
        ]
        for future in as_completed(futures):
            all_views.extend(future.result())

    if not all_views:
        return func.HttpResponse(
            json.dumps({
                "status": "SUCCESS",
                "message": f"No views discovered for metameta file '{metameta_name}'."
            }),
            status_code=200,
            mimetype="application/json"
        )

    # 3. Parallel View Validation
    all_results = []
    failed_results = []

    with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
        futures = [
            executor.submit(validate_single_view, view_tuple)
            for view_tuple in all_views
        ]
        for future in as_completed(futures):
            res = future.result()
            all_results.append(res)
            if res[3] == "FAILED":
                failed_results.append(res)

    total_count = len(all_results)
    failed_count = len(failed_results)
    passed_count = total_count - failed_count

    # 4. Handle Notifications & Response based on Outcome
    if failed_count > 0:
        # --- FAILURE PATH ---
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        filename = f"view_validation_flat_{metameta_name}_{timestamp}.csv"
        csv_data = generate_failed_flat_csv_report(failed_results)

        # SAVE REPORT LOCALLY (FOR LOCAL TESTING)
        try:
            local_path = os.path.join(os.getcwd(), filename)
            with open(local_path, "w", newline="", encoding="utf-8") as f:
                f.write(csv_data)
            logging.info(f"📁 Local CSV report saved to: {local_path}")
        except Exception as local_file_err:
            logging.warning(f"Unable to write local CSV report copy: {local_file_err}")

        # Send notifications
        send_teams_notification(
            webhook_url=teams_webhook_url,
            metameta_name=metameta_name,
            total_views=total_count,
            passed_cnt=passed_count,
            failed_cnt=failed_count,
            failed_list=failed_results
        )

        trigger_email_notification(
            metameta_name=metameta_name,
            total_views=total_count,
            passed_cnt=passed_count,
            failed_cnt=failed_count,
            failed_results=failed_results
        )

        return func.HttpResponse(
            body=csv_data,
            status_code=422,
            headers={
                "Content-Disposition": f"attachment; filename={filename}",
                "Content-Type": "text/csv; charset=utf-8"
            }
        )

    else:
        # --- SUCCESS PATH ---
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
            failed_cnt=0
        )

        return func.HttpResponse(
            json.dumps({
                "status": "SUCCESS",
                "message": f"All {total_count} views passed health check successfully.",
                "summary": {
                    "total_views": total_count,
                    "passed": passed_count,
                    "failed": 0
                }
            }, indent=2),
            status_code=200,
            mimetype="application/json"
        )
