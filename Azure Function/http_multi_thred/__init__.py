import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import azure.functions as func

# Import existing utility functions from utility.py
from utility import (
    get_snowflake_connection,
    get_metameta_dict,
    get_entity_key_value
)


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


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('psaValidationTrigger()- Python HTTP trigger function processed a request.')

    # -------------------------------------------------------------------------
    # 1. Parse Parameters (Matching your screenshot)
    # -------------------------------------------------------------------------
    metameta_name = req.params.get('metameta_name')
    if not metameta_name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            if req_body:
                metameta_name = req_body.get('metameta_name')

    # Fallback to default metadata identifier if not provided
    if not metameta_name:
        metameta_name = "View_validation"

    parallel_workers = int(os.environ.get("PARALLEL_WORKERS", 10))

    # -------------------------------------------------------------------------
    # 2. Retrieve Metadata and Discover Views
    # -------------------------------------------------------------------------
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
                "message": f"No views discovered for metameta file '{metameta_name}'.",
                "summary": {"total_views": 0, "passed": 0, "failed": 0}
            }),
            status_code=200,
            mimetype="application/json"
        )

    # -------------------------------------------------------------------------
    # 3. Parallel View Validation
    # -------------------------------------------------------------------------
    results = []
    failed_list = []

    with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
        futures = [
            executor.submit(validate_single_view, view_tuple)
            for view_tuple in all_views
        ]
        for future in as_completed(futures):
            res = future.result()
            results.append(res)
            if res[3] == "FAILED":
                failed_list.append({
                    "database": res[0],
                    "schema": res[1],
                    "view": res[2],
                    "error": res[4]
                })

    # -------------------------------------------------------------------------
    # 4. Construct Response
    # -------------------------------------------------------------------------
    is_success = len(failed_list) == 0
    response_payload = {
        "metameta_name": metameta_name,
        "status": "PASSED" if is_success else "FAILED",
        "summary": {
            "total_views": len(results),
            "passed": len(results) - len(failed_list),
            "failed": len(failed_list)
        },
        "failed_views": failed_list
    }

    return func.HttpResponse(
        json.dumps(response_payload, indent=2),
        status_code=200 if is_success else 422,
        mimetype="application/json"
    )
