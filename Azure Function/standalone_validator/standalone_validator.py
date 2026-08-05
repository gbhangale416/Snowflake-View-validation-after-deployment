import os
import sys
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

# Import required utility functions directly from utility.py
from utility import (
    get_snowflake_connection,
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


def load_master_metameta(file_path: str) -> dict:
    """Reads the master metameta JSON file from disk."""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Metameta file not found at path: '{file_path}'")

    logger.info(f"Loading master metameta file: {file_path}")
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)


def extract_all_db_schema_targets(metadata):
    """
    Parses the metameta dictionary and extracts real Snowflake DB -> Set of target schemas.
    Reads 'destination_database' or 'source_database' per entity.
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
    """Executes 'SELECT * LIMIT 0' on a single view."""
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
    # 1. Local Configuration & Specific File Name
    # -------------------------------------------------------------------------
    METAMETA_FILE = "./View_validation_metameta.json"
    parallel_workers = 10

    # Ensure credentials are present
    os.environ["SnowflakeServiceUser"] = os.environ.get("SnowflakeServiceUser", "YOUR_USER")
    os.environ["SnowflakeServicePassword"] = os.environ.get("SnowflakeServicePassword", "YOUR_PASSWORD")
    os.environ["SnowflakeServiceWarehouse"] = os.environ.get("SnowflakeServiceWarehouse", "WH_GEN1_ELT_C4_XS_DEV_TEST")
    os.environ["authentication_type"] = "False"

    # -------------------------------------------------------------------------
    # 2. Parse Real Snowflake Databases & Schemas from View_validation_metameta.json
    # -------------------------------------------------------------------------
    try:
        metadata = load_master_metameta(METAMETA_FILE)
        all_db_schema_map = extract_all_db_schema_targets(metadata)
    except Exception as exc:
        logger.error(f"Failed to process '{METAMETA_FILE}': {exc}")
        sys.exit(1)

    if not all_db_schema_map:
        logger.error("No valid database targets found inside JSON. Exiting.")
        sys.exit(1)

    logger.info(f"Targeting {len(all_db_schema_map)} Snowflake DBs: {list(all_db_schema_map.keys())}")

    # -------------------------------------------------------------------------
    # 3. Discover Views Across All Databases Concurrently
    # -------------------------------------------------------------------------
    all_views = []
    logger.info("Discovering views in Snowflake...")

    with ThreadPoolExecutor(max_workers=min(parallel_workers, len(all_db_schema_map))) as executor:
        futures = [
            executor.submit(fetch_views_for_db, db, schemas)
            for db, schemas in all_db_schema_map.items()
        ]
        for future in as_completed(futures):
            all_views.extend(future.result())

    if not all_views:
        logger.info("No views discovered in target databases.")
        return

    logger.info(f"Queued {len(all_views)} total view(s) for validation across {parallel_workers} threads...\n")

    # -------------------------------------------------------------------------
    # 4. Validate All Views Concurrently
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
