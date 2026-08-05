import os
import sys
import csv
import io
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import azure.functions as func

# Import utility functions directly from utility.py
from utility import (
    get_snowflake_connection,
    get_metameta_dict,
    find_entity_meta_meta,
    get_entity_key_value
)

app = func.FunctionApp()


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def extract_db_schema_targets(metadata):
    """
    Parses metadata dictionary and returns a map of DB -> Set of target schemas.
    Supports single schema strings or lists of schemas per entity.
    """
    db_schema_map = {}
    entities = metadata.get("entities", [])

    if not isinstance(entities, list):
        entities = [entities]

    # Fall back if no entities array exists
    if not entities:
        top_db = get_entity_key_value("destination_database", None, metadata) or get_entity_key_value("source_database", None, metadata)
        top_schema = get_entity_key_value("destination_schema", None, metadata) or get_entity_key_value("default_source_schema", None, metadata) or ""
        if top_db:
            db_schema_map.setdefault(top_db.strip().upper(), set()).add(top_schema.strip().upper())

    # Extract db & target schemas per entity
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
    """
    Fast batch discovery of views in target_db using SHOW VIEWS.
    """
    logging.info(f"Batch fetching view metadata for database '{target_db}'...")
    views_to_validate = []

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

            # Skip system/internal schemas
            if v_schema in ('INFORMATION_SCHEMA', 'PUBLIC', 'DEPLOY'):
                continue

            # Filter by metadata schemas if provided
            if target_schemas and "" not in target_schemas:
                if v_schema in target_schemas:
                    views_to_validate.append((target_db, v_schema, v_name))
            else:
                views_to_validate.append((target_db, v_schema, v_name))

        logging.info(f"Discovered {len(views_to_validate)} view(s) in database '{target_db}'.")
    except Exception as exc:
        logging.error(f"Error fetching views for database {target_db}: {exc}")
    finally:
        cs.close()
        ctx.close()

    return views_to_validate


def validate_single_view(view_tuple):
    """
    Worker task to execute 'SELECT * LIMIT 0' health check on a view.
    """
    v_db, v_schema, v_view = view_tuple
    fq_name = f'"{v_db}"."{v_schema}"."{v_view}"'

    cs, ctx = get_snowflake_connection()

    try:
        cs.execute(f"SELECT * FROM {fq_name} LIMIT 0")
        logging.info(f"OK - {fq_name}")
        return (v_db, v_schema, v_view, "Full Scan", "OK", "")
    except Exception as exc:
        err_msg = str(exc).replace("\n", " ").replace("\r", " ")
        logging.error(f"FAILED - {fq_name}: {err_msg}")
        return (v_db, v_schema, v_view, "Full Scan", "FAILED", err_msg)
    finally:
        cs.close()
        ctx.close()


# =============================================================================
# AZURE TIMER TRIGGER FUNCTION
# =============================================================================

# Runs daily at 6:00 AM UTC (CRON: "0 0 6 * * *")
@app.timer_trigger(schedule="0 0 6 * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False)
def scheduled_view_validation(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is running past due!')

    logging.info("=== STARTING SCHEDULED SNOWFLAKE VIEW VALIDATION ===")

    # 1. Read Target Databases and Settings from App Settings
    target_dbs_str = os.environ.get("TARGET_DATABASES", "")
    if not target_dbs_str:
        logging.error("Missing 'TARGET_DATABASES' environment variable.")
        return

    target_dbs = [db.strip() for db in target_dbs_str.split(",") if db.strip()]
    max_workers = int(os.environ.get("PARALLEL_WORKERS", 10))

    # 2. Fetch Metameta from Azure Blob Storage per database
    all_db_schema_map = {}
    for db_name in target_dbs:
        try:
            metadata = get_metameta_dict(db_name=db_name)
            db_map = extract_db_schema_targets(metadata)
            all_db_schema_map.update(db_map)
        except Exception as exc:
            logging.error(f"Failed to load metameta JSON for DB '{db_name}': {exc}")

    if not all_db_schema_map:
        logging.error("No database target mappings loaded. Exiting validation.")
        return

    # 3. Discover Views Across All Databases Concurrently
    all_views = []
    with ThreadPoolExecutor(max_workers=min(max_workers, len(all_db_schema_map))) as executor:
        futures = [
            executor.submit(fetch_views_for_db, db, schemas)
            for db, schemas in all_db_schema_map.items()
        ]
        for future in as_completed(futures):
            all_views.extend(future.result())

    if not all_views:
        logging.info("No views discovered to validate across the specified scopes.")
        return

    logging.info(f"Queued total {len(all_views)} view(s) for validation across threads...")

    # 4. Validate Views Concurrently
    results = []
    failed_list = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(validate_single_view, view_tuple)
            for view_tuple in all_views
        ]
        for future in as_completed(futures):
            res = future.result()
            results.append(res)
            if res[4] == "FAILED":
                failed_list.append(res)

    # 5. Output Summary Results to Log Stream
    logging.info(
        f"=== VALIDATION COMPLETE: Total: {len(results)} | Passed: {len(results) - len(failed_list)} | Failed: {len(failed_list)} ==="
    )

    if failed_list:
        for f in failed_list:
            logging.error(f"BROKEN VIEW DETECTED: {f[0]}.{f[1]}.{f[2]} -> {f[5]}")
