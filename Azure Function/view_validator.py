import argparse
import csv
import json
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from utility import get_snowflake_connection

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


def check_snowsql_pwd():
    """Ensures SNOWSQL_PWD environment variable is present."""
    if "SNOWSQL_PWD" not in os.environ:
        logger.error("The SNOWSQL_PWD environment variable has not been defined")
        sys.exit(2)


def load_metadata(metadata_file):
    """
    Parses metameta JSON file and extracts database -> set of schemas mapping.
    Example return: {"FINANCE_DB": {"PAYROLL", "GENERAL_LEDGER"}, "SALES_DB": {"ORDERS"}}
    """
    if not os.path.exists(metadata_file):
        raise FileNotFoundError(f"Metadata file not found at: {metadata_file}")

    with open(metadata_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    db_schema_map = {}
    entities = data.get("entities", [])

    if not entities:
        top_db = data.get("destination_database") or data.get("source_database")
        top_schema = data.get("destination_schema") or data.get("default_source_schema") or ""
        if top_db and top_db.strip():
            db_schema_map.setdefault(top_db.strip().upper(), set()).add(top_schema.strip().upper())

    for entity in entities:
        db = entity.get("destination_database") or entity.get("source_database")
        if not db or not db.strip():
            continue
        
        db = db.strip().upper()
        schemas = (
            entity.get("destination_schemas") 
            or entity.get("destination_schema") 
            or entity.get("source_schema") 
            or ""
        )

        if isinstance(schemas, list):
            for s in schemas:
                db_schema_map.setdefault(db, set()).add(str(s).strip().upper())
        else:
            db_schema_map.setdefault(db, set()).add(str(schemas).strip().upper())

    return db_schema_map


def fetch_views_for_db(conn_params, target_db, target_schemas):
    """
    Fast batch discovery of views using SHOW VIEWS in target_db.
    Filters out system schemas and matches target schemas if provided.
    """
    logger.info(f"Batch fetching view metadata for database '{target_db}'...")
    views_to_validate = []

    conn = get_snowflake_connection(**conn_params, database=target_db)
    cs = conn.cursor()

    try:
        cs.execute(f'SHOW VIEWS IN DATABASE "{target_db}"')
        columns = [col[0].lower() for col in cs.description]
        
        # Identify column indices dynamically from SHOW VIEWS
        schema_idx = columns.index('schema_name') if 'schema_name' in columns else 1
        name_idx = columns.index('name') if 'name' in columns else 2

        for row in cs.fetchall():
            v_schema = row[schema_idx].upper()
            v_name = row[name_idx].upper()

            # Ignore system/deployment schemas
            if v_schema in ('INFORMATION_SCHEMA', 'PUBLIC', 'DEPLOY'):
                continue

            # If specific schemas are defined (and not empty ""), filter by them
            if target_schemas and "" not in target_schemas:
                if v_schema in target_schemas:
                    views_to_validate.append((target_db, v_schema, v_name))
            else:
                views_to_validate.append((target_db, v_schema, v_name))

        logger.info(f"Discovered {len(views_to_validate)} view(s) in database '{target_db}'.")
    except Exception as exc:
        logger.error(f"Error fetching views for database {target_db}: {exc}")
    finally:
        cs.close()
        conn.close()

    return views_to_validate


def validate_single_view(conn_params, view_tuple):
    """Worker task: Validates a single view over a dedicated thread connection."""
    v_db, v_schema, v_view = view_tuple
    fq_name = f'"{v_db}"."{v_schema}"."{v_view}"'

    conn = get_snowflake_connection(**conn_params, database=v_db)
    cs = conn.cursor()

    try:
        cs.execute(f"SELECT * FROM {fq_name} LIMIT 0")
        logger.info(f"OK - {fq_name}")
        return (v_db, v_schema, v_view, "Full Scan", "OK", "")
    except Exception as exc:
        err_msg = str(exc).replace("\n", " ").replace("\r", " ")
        logger.error(f"FAILED - {fq_name}: {err_msg}")
        return (v_db, v_schema, v_view, "Full Scan", "FAILED", err_msg)
    finally:
        cs.close()
        conn.close()


def create_csv_report(results, report_file):
    """Saves output results into CSV."""
    os.makedirs(os.path.dirname(os.path.abspath(report_file)), exist_ok=True)
    with open(report_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["View_database", "Schema", "View", "Validation_Type", "Status", "Error"])
        writer.writerows(results)
    logger.info(f"CSV validation report written to: {report_file}")


def view_validator(snowflake_account, snowflake_user, snowflake_role, snowflake_warehouse,
                   metadata_file, report_file, max_workers=10):
    """Main parallelized validation entrypoint."""
    check_snowsql_pwd()

    # Load Database -> Schemas Mapping
    db_schema_map = load_metadata(metadata_file)
    logger.info(f"Loaded target scope from metadata across {len(db_schema_map)} database(s).")

    if not db_schema_map:
        logger.error("No valid database targets found in metadata JSON.")
        sys.exit(1)

    conn_params = {
        "user": snowflake_user,
        "account": snowflake_account,
        "role": snowflake_role,
        "warehouse": snowflake_warehouse,
        "authenticator": "snowflake",
        "password": os.environ["SNOWSQL_PWD"]
    }

    # Step 1: Discover all views across databases in parallel
    all_views = []
    with ThreadPoolExecutor(max_workers=min(max_workers, len(db_schema_map))) as executor:
        futures = [
            executor.submit(fetch_views_for_db, conn_params, db, schemas)
            for db, schemas in db_schema_map.items()
        ]
        for future in as_completed(futures):
            all_views.extend(future.result())

    if not all_views:
        logger.info("No views discovered to validate across the defined scope.")
        return

    logger.info(f"Total views queued for validation: {len(all_views)}")

    # Step 2: Validate views concurrently in parallel
    results = []
    failed = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(validate_single_view, conn_params, view_tuple)
            for view_tuple in all_views
        ]
        for future in as_completed(futures):
            res = future.result()
            results.append(res)
            if res[4] == "FAILED":
                failed.append(res)

    # Step 3: Write report
    create_csv_report(results, report_file)
    logger.info(f"Validation Summary: {len(results) - len(failed)} OK, {len(failed)} FAILED.")

    if failed:
        for v_db, v_schema, v_view, _, _, err in failed:
            logger.error(f"BROKEN VIEW: {v_db}.{v_schema}.{v_view} -> {err}")
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog='view_validator.py',
        description='High-performance parallel view validator for Snowflake.',
        formatter_class=argparse.RawTextHelpFormatter,
    )

    parser.add_argument('-a', '--snowflake-account', type=str, required=True, help='Snowflake account')
    parser.add_argument('-u', '--snowflake-user', type=str, required=True, help='Snowflake user')
    parser.add_argument('-r', '--snowflake-role', type=str, required=True, help='Snowflake role')
    parser.add_argument('-w', '--snowflake-warehouse', type=str, required=True, help='Snowflake warehouse')
    parser.add_argument('-m', '--metadata-file', type=str, required=True, help='Path to metadata JSON file')
    parser.add_argument('-rf', '--report-file', type=str, required=True, help='Path for output CSV report')
    parser.add_argument('-p', '--parallel-workers', type=int, default=10, help='Max concurrent threads (default: 10)')

    args = parser.parse_args()

    view_validator(
        snowflake_account=args.snowflake_account,
        snowflake_user=args.snowflake_user,
        snowflake_role=args.snowflake_role,
        snowflake_warehouse=args.snowflake_warehouse,
        metadata_file=args.metadata_file,
        report_file=args.report_file,
        max_workers=args.parallel_workers
    )
