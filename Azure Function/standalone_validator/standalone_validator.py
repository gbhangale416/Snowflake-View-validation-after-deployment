import os
import sys
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

# Import required utility functions directly from your utility.py file
from utility import (
    get_snowflake_connection,
    get_metameta_dict,
    get_entity_key_value
)

# Set up clean logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)


def extract_all_db_schema_targets(metadata):
    """
    Parses metameta dictionary and extracts real Snowflake DB -> Set of target schemas.
    Reads 'destination_database' or 'source_database' per entity.
    """
    db_schema_map = {}
    entities = metadata.get("entities", [])

    if not isinstance(entities, list):
        entities = [entities]

    # Fallback if no entities array exists
    if not entities:
        top_db = get_entity_key_value("destination_database", None, metadata) or get_entity_key_value("source_database", None, metadata)
        top_schema = get_entity_key_value("destination_schema", None, metadata) or get_entity_key_value("default_source_schema", None, metadata) or ""
        if top_db:
            db_schema_map.setdefault(top_db.strip().upper(), set()).add(top_schema.strip().upper())

    # Extract real Snowflake DB & target schemas per entity
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
    """Batch discovers all views inside target_db using SHOW VIEWS."""
    logger.info(f"Fetching views for Snowflake database: '{target_db}'...")
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

            # Ignore system schemas
            if v_schema in ('INFORMATION_SCHEMA', 'PUBLIC', 'DEPLOY'):
                continue

            # Filter by target schemas if specified in metameta
            if target_schemas and "" not in target_schemas:
                if v_schema in target_schemas:
                    views_found.append((target_db, v_schema, v_name))
            else:
                views_found.append((target_db, v_schema, v_name))

        logger.info(f"Discovered {len(views_found)} view(s) in Snowflake DB '{target_db}'.")
    except Exception as exc:
        logger.error(f"Failed to fetch views for DB '{target_db}': {exc}")
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
        logger.info(f"✅ OK - {fq_name}")
        return (v_db, v_schema, v_view, "OK", "")
    except Exception as exc:
        err_msg = str(exc).replace("\n", " ").replace("\r", " ")
        logger.error(f"❌ FAILED - {fq_name}: {err_msg}")
        return (v_db, v_schema, v_view, "FAILED", err_msg)
    finally:
        cs.close()
        ctx.close()


def main():
    # -------------------------------------------------------------------------
    # 1. Set environment variables locally (or rely on system env vars)
    # -------------------------------------------------------------------------
    os.environ["AzureBlobStorageConnectionString"] = os.environ.get(
        "AzureBlobStorageConnectionString", 
        "<YOUR_AZURE_BLOB_STORAGE_CONNECTION_STRING>"
    )
    os.environ["SnowflakeServiceUser"] = os.environ.get("SnowflakeServiceUser", "<YOUR_SNOWFLAKE_USER>")
    os.environ["SnowflakeServicePassword"] = os.environ.get("SnowflakeServicePassword", "<YOUR_SNOWFLAKE_PASSWORD>")
    os.environ["SnowflakeServiceWarehouse"] = os.environ.get("SnowflakeServiceWarehouse", "WH_GEN1_ELT_C4_XS_DEV_TEST")
    os.environ["authentication_type"] = "False"

    parallel_workers = 10
    file_identifier = "View_validation"  # Calls View_validation/View_validation_metameta.json in blob storage

    # -------------------------------------------------------------------------
    # 2. Fetch metameta file from Azure Blob Storage via get_metameta_dict()
    # -------------------------------------------------------------------------
    logger.info(f"Downloading '{file_identifier}_metameta.json' from Azure Blob Storage...")
    try:
        metadata = get_metameta_dict(db_name=file_identifier)
        all_db_schema_map = extract_all_db_schema_targets(metadata)
    except Exception as exc:
        logger.error(f"Failed to fetch or parse metameta for '{file_identifier}': {exc}")
        sys.exit(1)

    if not all_db_schema_map:
        logger.error("No valid database targets found inside metameta file. Exiting.")
        sys.exit(1)

    logger.info(f"Parsed scope for {len(all_db_schema_map)} Snowflake DB(s): {list(all_db_schema_map.keys())}")

    # -------------------------------------------------------------------------
    # 3. Discover views across all target databases in parallel
    # -------------------------------------------------------------------------
    all_views = []
    logger.info("Discovering views across databases concurrently...")

    with ThreadPoolExecutor(max_workers=min(parallel_workers, len(all_db_schema_map))) as executor:
        futures = [
            executor.submit(fetch_views_for_db, db, schemas)
            for db, schemas in all_db_schema_map.items()
        ]
        for future in as_completed(futures):
            all_views.extend(future.result())

    if not all_views:
        logger.info("No views discovered across target databases.")
        return

    logger.info(f"Queued {len(all_views)} total view(s) for validation across {parallel_workers} worker threads...\n")

    # -------------------------------------------------------------------------
    # 4. Validate all views concurrently
    # -------------------------------------------------------------------------
    results = []
    failed = []

    with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
        futures = [
            executor.submit(validate_single_view, view_tuple)
            for view_tuple in all_views
        ]
        for future in as_completed(futures):
            res = future.result()
            results.append(res)
            if res[3] == "FAILED":
                failed.append(res)

    # -------------------------------------------------------------------------
    # 5. Output Summary Report
    # -------------------------------------------------------------------------
    logger.info("=" * 60)
    logger.info(f"SUMMARY: Total Scanned: {len(results)} | Passed: {len(results) - len(failed)} | Failed: {len(failed)}")
    logger.info("=" * 60)

    if failed:
        logger.error("BROKEN VIEWS SUMMARY:")
        for db, schema, view, status, err in failed:
            logger.error(f" -> {db}.{schema}.{view}: {err}")


if __name__ == "__main__":
    main()
