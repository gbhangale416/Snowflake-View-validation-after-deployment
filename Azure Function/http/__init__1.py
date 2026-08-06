import json
import logging
import os
import azure.functions as func

# Import existing utility functions from utility.py
from utility import (
    get_snowflake_connection,
    get_metameta_dict,
    get_entity_key_value
)


##http://localhost:7071/api/snowflake_view_validator?metameta_name=View_validation
##

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
    """Discovers views using target_db.INFORMATION_SCHEMA.VIEWS filtered by target schemas."""
    logging.info(f"Fetching views for Snowflake database: '{target_db}'...")
    views_found = []

    cs, ctx = get_snowflake_connection()

    try:
        schemas_list = list(target_schemas)

        if schemas_list:
            schema_list_str = ", ".join([f"'{s}'" for s in schemas_list])
            where_clause = f"WHERE TABLE_SCHEMA IN ({schema_list_str})"
        else:
            where_clause = ""

        query = f"""
            SELECT 
                TABLE_SCHEMA, 
                TABLE_NAME 
            FROM "{target_db}".INFORMATION_SCHEMA.VIEWS 
            {where_clause};
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
    """Executes 'SELECT * LIMIT 0' health check on a single view."""
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
    # 1. Get metameta_name strictly from URL Query Parameter (or JSON Body)
    # -------------------------------------------------------------------------
    metameta_name = req.params.get('metameta_name')
    if not metameta_name:
        try:
            req_body = req.get_json()
            if req_body:
                metameta_name = req_body.get('metameta_name')
        except ValueError:
            pass

    # Return error if no metameta_name was supplied in the URL or Body
    if not metameta_name:
        return func.HttpResponse(
            json.dumps({"status": "ERROR", "message": "Please pass 'metameta_name' in the URL query string (e.g. ?metameta_name=View_validation)."}),
            status_code=400,
            mimetype="application/json"
        )

    # Clean suffix in case user passed "View_validation_metameta.json" or "View_validation.json"
    clean_name = metameta_name.replace("_metameta.json", "").replace(".json", "")

    # -------------------------------------------------------------------------
    # 2. Retrieve Metadata (Blob Storage -> Local Fallback)
    # -------------------------------------------------------------------------
    try:
        # First attempt: Fetch from Azure Blob Storage
        metadata = get_metameta_dict(db_name=clean_name)
        logging.info(f"Successfully loaded '{clean_name}' from Azure Blob Storage.")
    except Exception as blob_err:
        logging.warning(f"Blob Storage load failed for '{clean_name}' ({blob_err}). Checking local directory...")

        # Second attempt: Look for local file matching the parameter name
        possible_local_paths = [
            f"./{clean_name}_metameta.json",
            f"./{clean_name}.json",
            f"./{metameta_name}"
        ]

        found_path = None
        for path in possible_local_paths:
            if os.path.exists(path):
                found_path = path
                break

        if found_path:
            with open(found_path, "r", encoding="utf-8") as f:
                metadata = json.load(f)
            logging.info(f"Successfully loaded '{clean_name}' from local file '{found_path}'.")
        else:
            return func.HttpResponse(
                json.dumps({
                    "status": "ERROR", 
                    "message": f"Metameta file '{clean_name}' not found in Azure Blob Storage nor locally (checked: {possible_local_paths})."
                }),
                status_code=404,
                mimetype="application/json"
            )

    try:
        all_db_schema_map = extract_all_db_schema_targets(metadata)
    except Exception as exc:
        return func.HttpResponse(
            json.dumps({"status": "ERROR", "message": f"Failed to parse metameta JSON for '{clean_name}': {str(exc)}"}),
            status_code=500,
            mimetype="application/json"
        )

    # -------------------------------------------------------------------------
    # 3. View Discovery via INFORMATION_SCHEMA
    # -------------------------------------------------------------------------
    all_views = []
    for db, schemas in all_db_schema_map.items():
        db_views = fetch_views_for_db(db, schemas)
        all_views.extend(db_views)

    if not all_views:
        return func.HttpResponse(
            json.dumps({
                "status": "SUCCESS",
                "message": f"No views discovered for metameta file '{clean_name}'.",
                "summary": {"total_views": 0, "passed": 0, "failed": 0}
            }),
            status_code=200,
            mimetype="application/json"
        )

    # -------------------------------------------------------------------------
    # 4. Sequential View Validation
    # -------------------------------------------------------------------------
    results = []
    failed_list = []

    for view_tuple in all_views:
        res = validate_single_view(view_tuple)
        results.append(res)
        if res[3] == "FAILED":
            failed_list.append({
                "database": res[0],
                "schema": res[1],
                "view": res[2],
                "error": res[4]
            })

    # -------------------------------------------------------------------------
    # 5. Construct JSON Response
    # -------------------------------------------------------------------------
    is_success = len(failed_list) == 0
    response_payload = {
        "metameta_name": clean_name,
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
