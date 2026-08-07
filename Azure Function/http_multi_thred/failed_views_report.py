import json
import logging
import os
import io
import csv
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
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
# HELPER: Send Microsoft Teams Notification via Webhook
# =============================================================================
def send_teams_notification(webhook_url, metameta_name, total_views, passed_cnt, failed_cnt, failed_list):
    """Posts an Adaptive Card / MessageCard notification to MS Teams."""
    if not webhook_url:
        logging.warning("TEAMS_WEBHOOK_URL not configured. Skipping Teams notification.")
        return

    status_color = "00FF00" if failed_cnt == 0 else "FF0000"
    status_title = "✅ View Validation Passed" if failed_cnt == 0 else "❌ View Validation Failed"

    # Build facts list
    facts = [
        {"name": "Metadata Target:", "value": metameta_name},
        {"name": "Total Views Tested:", "value": str(total_views)},
        {"name": "Passed Views:", "value": str(passed_cnt)},
        {"name": "Failed Views:", "value": str(failed_cnt)}
    ]

    # Include top failure summaries if present
    failure_details = ""
    if failed_cnt > 0:
        failure_details = "\n\n**Failed Views Summary:**\n"
        for item in failed_list[:5]:  # Limit to first 5 in Teams card to keep it clean
            db, schema, view, _, err = item
            failure_details += f"* `{db}.{schema}.{view}` — {err[:80]}...\n"
        if failed_cnt > 5:
            failure_details += f"\n*...and {failed_cnt - 5} more. See attached CSV report for full list.*"

    payload = {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "themeColor": status_color,
        "title": status_title,
        "text": f"Snowflake view validation completed for **{metameta_name}**.{failure_details}",
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
# HELPER: Send Email Notification with CSV Attachment
# =============================================================================
def send_email_notification(metameta_name, total_views, passed_cnt, failed_cnt, csv_data, filename):
    """Sends email summary with the Failed Views CSV report attached."""
    smtp_server = os.environ.get("SMTP_SERVER")
    smtp_port = int(os.environ.get("SMTP_PORT", 587))
    smtp_user = os.environ.get("SMTP_USER")
    smtp_password = os.environ.get("SMTP_PASSWORD")
    email_to = os.environ.get("NOTIFICATION_EMAIL_TO")
    email_from = os.environ.get("NOTIFICATION_EMAIL_FROM", smtp_user)

    if not all([smtp_server, smtp_user, smtp_password, email_to]):
        logging.warning("SMTP configuration missing. Skipping Email notification.")
        return

    subject = f"[{'FAILED' if failed_cnt > 0 else 'PASSED'}] Snowflake View Validation Report: {metameta_name}"
    
    body = f"""
    Hello Data Team,

    The automated Snowflake view health check has completed.

    Summary:
    ---------------------------------------------------
    Metameta Name: {metameta_name}
    Total Views Validated: {total_views}
    Passed Views: {passed_cnt}
    Failed Views: {failed_cnt}
    ---------------------------------------------------

    {'PLEASE NOTE: Attached is the CSV report listing all FAILED views that require attention.' if failed_cnt > 0 else 'All views passed health check successfully!'}

    Regards,
    Automated Azure Function Validation Pipeline
    """

    msg = MIMEMultipart()
    msg['From'] = email_from
    msg['To'] = email_to
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    # Attach CSV only if failed views exist
    if failed_cnt > 0 and csv_data:
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(csv_data.encode('utf-8'))
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename="{filename}"')
        msg.attach(part)

    try:
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.sendmail(email_from, email_to.split(','), msg.as_string())
        server.quit()
        logging.info("Successfully sent Email notification.")
    except Exception as exc:
        logging.error(f"Failed to send email notification: {exc}")


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


def generate_failed_csv_report(failed_results):
    """
    Generates CSV content ONLY for failed views, grouped by database sections.
    """
    output = io.StringIO()
    writer = csv.writer(output)

    # Group failed results by Database: { "DB_NAME": [(schema, view, status, err), ...] }
    grouped_failures = {}
    for row in failed_results:
        v_db, v_schema, v_view, status, err = row
        grouped_failures.setdefault(v_db, []).append((v_schema, v_view, status, err))

    # Write each database block into CSV
    for idx, (db_name, db_views) in enumerate(grouped_failures.items()):
        if idx > 0:
            writer.writerow([])  # Blank lines between sections
            writer.writerow([])

        writer.writerow([f"=== DATABASE: {db_name} (FAILED VIEWS) ==="])
        writer.writerow(["Schema", "View Name", "Status", "Error Message"])

        for view_info in db_views:
            writer.writerow(view_info)

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

    # 4. Generate CSV for FAILED Views ONLY
    filename = f"failed_views_report_{metameta_name}.csv"
    csv_data = generate_failed_csv_report(failed_results) if failed_count > 0 else ""

    # 5. Send Teams & Email Notifications
    send_teams_notification(
        webhook_url=teams_webhook_url,
        metameta_name=metameta_name,
        total_views=total_count,
        passed_cnt=passed_count,
        failed_cnt=failed_count,
        failed_list=failed_results
    )

    send_email_notification(
        metameta_name=metameta_name,
        total_views=total_count,
        passed_cnt=passed_count,
        failed_cnt=failed_count,
        csv_data=csv_data,
        filename=filename
    )

    # 6. Return Response
    if failed_count > 0:
        return func.HttpResponse(
            body=csv_data,
            status_code=422,  # Return HTTP 422 Unprocessable Entity when validation failures exist
            headers={
                "Content-Disposition": f"attachment; filename={filename}",
                "Content-Type": "text/csv; charset=utf-8"
            }
        )
    else:
        # All views passed!
        return func.HttpResponse(
            body="Status,Message\nPASSED,All views validated successfully. No failures to report.\n",
            status_code=200,
            headers={
                "Content-Disposition": f"attachment; filename=success_{metameta_name}.csv",
                "Content-Type": "text/csv; charset=utf-8"
            }
        )
