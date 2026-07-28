"""
Unified Cross-Database Snowdeploy Table Extraction and View Validation Tool.

In a single pass, this script:
  1. Queries CHANGE_HISTORY for a build and extracts touched table names from applied scripts.
  2. Identifies dependent views across specified (or all) Snowflake databases via account-wide object dependencies,
     validates them using SELECT LIMIT 0, and writes a CSV report.

Usage:
    SNOWSQL_PWD=... python deploy_validator.py \
        -a <account> -u <user> -r <role> -w <warehouse> -d <database> \
        -c <change_history_table> -b <build_id> -rf <report_file> \
        -vdb DB1 DB2 DB3
"""

import argparse
import csv
import logging
import os
import re
import sys

import snowflake.connector

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

TABLE_PATTERNS = [
    re.compile(r'CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([\w."]+)', re.IGNORECASE),
    re.compile(r'ALTER\s+TABLE\s+([\w."]+)', re.IGNORECASE),
    re.compile(r'DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?([\w."]+)', re.IGNORECASE),
]


# =============================================================================
# SHARED UTILITIES & CONNECTION HANDLER
# =============================================================================

def parse_table_name(name):
    """Returns (schema, table) from a possibly-qualified name; schema None if unqualified."""
    parts = [p.strip('"') for p in name.split(".")]
    if len(parts) >= 2:
        return parts[-2], parts[-1]
    return None, parts[-1]


def check_snowsql_pwd():
    """Ensures SNOWSQL_PWD environment variable is present."""
    if "SNOWSQL_PWD" not in os.environ:
        logger.error("The SNOWSQL_PWD environment variable has not been defined")
        sys.exit(2)


def get_snowflake_connection():
    """Establishes and returns a Snowflake connection using environment variables."""
    snowflake_connection = snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        authenticator=os.environ["SNOWFLAKE_AUTHENTICATOR"],
        password=os.environ["SNOWSQL_PWD"]
    )
    return snowflake_connection


# =============================================================================
# EXTRACTION LOGIC
# =============================================================================

def get_change_history_table_details(snowflake_database, change_history_table_override):
    """Mirrors snowdeploy.py's get_change_history_table_details()."""
    details = {
        "database_name": snowflake_database,
        "schema_name": "DEPLOY",
        "table_name": "CHANGE_HISTORY",
    }
    if change_history_table_override:
        parts = change_history_table_override.strip().split(".")
        if len(parts) == 1:
            details["table_name"] = parts[0].upper()
        elif len(parts) == 2:
            details["schema_name"] = parts[0].upper()
            details["table_name"] = parts[1].upper()
        elif len(parts) == 3:
            details["database_name"] = parts[0].upper()
            details["schema_name"] = parts[1].upper()
            details["table_name"] = parts[2].upper()
        else:
            raise ValueError(f"Invalid change history table name: {change_history_table_override}")
    return details


def get_applied_script_paths(cs, change_history_table, build_id):
    """Returns all script paths applied in this build without filtering on script_type."""
    fq_table = '"{database_name}"."{schema_name}"."{table_name}"'.format(**change_history_table)
    try:
        cs.execute(
            f"""
            SELECT DISTINCT SCRIPT_PATH
            FROM {fq_table}
            WHERE BUILD_ID = %(build_id)s
              AND STATUS = 'Success'
            """,
            {"build_id": build_id},
        )
        rows = cs.fetchall()
    except Exception as exc:
        logger.warning(f"Could not query change history table {fq_table}: {exc}")
        return []

    return [path for (path,) in rows if path]


def extract_deployed_tables(script_paths):
    """Extracts table names touched by CREATE/ALTER/DROP TABLE statements."""
    tables = set()
    for path in script_paths:
        if not os.path.exists(path):
            logger.warning(f"{path} not found on disk - skipping")
            continue
        with open(path, "r") as f:
            content = f.read()
        for pattern in TABLE_PATTERNS:
            for m in pattern.finditer(content):
                tables.add(m.group(1).strip('"').upper())
    return tables


def run_extract_tables(cs, args):
    """Extracts deployed tables for a build and returns them as a sorted list."""
    logger.info("--- STARTING TABLE EXTRACTION ---")

    change_history_table = get_change_history_table_details(args.snowflake_database, args.change_history_table)
    logger.info(f"Change history table: {change_history_table['database_name']}."
                f"{change_history_table['schema_name']}.{change_history_table['table_name']}")

    script_paths = get_applied_script_paths(cs, change_history_table, args.build_id)
    logger.info(f"Total scripts applied for build {args.build_id}: {len(script_paths)}")

    deployed_tables = sorted(extract_deployed_tables(script_paths))
    logger.info(f"Tables touched by this deployment to {args.snowflake_database}: {', '.join(deployed_tables) or '(none)'}")

    return deployed_tables


# =============================================================================
# CROSS-DATABASE VALIDATION & REPORTING LOGIC
# =============================================================================

def find_impacted_views_cross_db(cs, deployed_database, deployed_tables, target_databases=None):
    """Identifies dependent views across specified (or all) databases using Object Dependencies."""
    impacted = {}  # (view_db, view_schema, view_name) -> set of referenced tables

    # Normalize target databases to uppercase set for fast lookup
    target_dbs_set = {db.upper() for db in target_databases} if target_databases else None

    for full_name in deployed_tables:
        schema, table = parse_table_name(full_name)
        try:
            query = """
                SELECT referencing_database, referencing_schema, referencing_object_name
                FROM snowflake.account_usage.object_dependencies
                WHERE referenced_object_domain = 'TABLE'
                  AND referencing_object_domain = 'VIEW'
                  AND UPPER(referenced_database) = UPPER(%(database)s)
                  AND UPPER(referenced_object_name) = UPPER(%(table)s)
            """
            params = {"database": deployed_database, "table": table}

            if schema:
                query += " AND UPPER(referenced_schema) = UPPER(%(schema)s)"
                params["schema"] = schema

            cs.execute(query, params)

            for r_db, r_schema, r_view in cs.fetchall():
                # Check if the referencing database is in the specified database list
                if target_dbs_set and r_db.upper() not in target_dbs_set:
                    continue
                impacted.setdefault((r_db, r_schema, r_view), set()).add(full_name)

        except Exception as exc:
            logger.warning(f"Lineage lookup skipped for {full_name}: {str(exc).splitlines()[0]}")

    return impacted


def validate_views_cross_db(cs, impacted):
    """Validates views across databases with SELECT LIMIT 0."""
    results = []
    failed = []
    total = len(impacted)
    
    for idx, ((v_db, v_schema, v_view), sources) in enumerate(sorted(impacted.items()), start=1):
        fq_name = f'"{v_db}"."{v_schema}"."{v_view}"'
        logger.info(f"[{idx}/{total}] Checking {v_db}.{v_schema}.{v_view} ...")
        source_desc = "; ".join(sorted(sources))
        try:
            cs.execute(f"SELECT * FROM {fq_name} LIMIT 0")
            results.append((v_db, v_schema, v_view, source_desc, "OK", ""))
            logger.info(f"[{idx}/{total}] OK - {v_db}.{v_schema}.{v_view}")
        except Exception as exc:
            err_msg = str(exc).replace("\n", " ").replace("\r", " ")
            results.append((v_db, v_schema, v_view, source_desc, "FAILED", err_msg))
            failed.append((v_db, v_schema, v_view, err_msg))
            logger.error(f"[{idx}/{total}] FAILED - {v_db}.{v_schema}.{v_view}: {err_msg}")

    return results, failed


def create_csv_report(results, report_file):
    """Creates and saves the validation CSV report."""
    os.makedirs(os.path.dirname(report_file), exist_ok=True)
    with open(report_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["view_database", "schema", "view", "deployed_tables_referenced", "status", "error"])
        writer.writerows(results)
    logger.info(f"CSV validation report successfully written to: {report_file}")


def run_validate_views(cs, args, deployed_tables):
    """Validates cross-database views impacting the deployed tables and generates report."""
    logger.info("--- STARTING VIEW VALIDATION ---")

    if not deployed_tables:
        logger.info(f"No tables recorded for {args.snowflake_database} in this build - nothing to check")
        return

    logger.info(f"Tables deployed to {args.snowflake_database}: {', '.join(deployed_tables)}")
    
    if args.validate_databases:
        logger.info(f"Filtering view discovery to databases: {', '.join(args.validate_databases)}")
    else:
        logger.info("No specific database filter applied; scanning across ALL databases.")

    impacted = find_impacted_views_cross_db(
        cs, 
        args.snowflake_database, 
        deployed_tables, 
        target_databases=args.validate_databases
    )
    
    if not impacted:
        logger.info("No matching views reference the deployed tables.")
        return

    results, failed = validate_views_cross_db(cs, impacted)

    # Generate CSV Report
    create_csv_report(results, args.report_file)

    logger.info(f"{len(impacted)} view(s) reference tables deployed in this build "
                f"({len(impacted) - len(failed)} OK, {len(failed)} FAILED).")
    
    for v_db, schema, view, err in failed:
        logger.error(f"BROKEN VIEW: {v_db}.{schema}.{view} -> {err}")

    if failed:
        sys.exit(1)


# =============================================================================
# CLI PARSER & MAIN ENTRYPOINT
# =============================================================================

def parse_args():
    parser = argparse.ArgumentParser(
        prog='deploy_validator.py',
        description='Extract deployed tables and validate affected views in specific databases.',
        formatter_class=argparse.RawTextHelpFormatter,
    )
    # Snowflake Connection Arguments
    parser.add_argument('-a', '--snowflake-account', type=str, required=True,
                        help='The name of the snowflake account (e.g. abc123.east-us-2.azure)')
    parser.add_argument('-u', '--snowflake-user', type=str, required=True,
                        help='The name of the snowflake user (e.g. DEPLOYER)')
    parser.add_argument('-r', '--snowflake-role', type=str, required=True,
                        help='The name of the role to use (e.g. DEPLOYER_ROLE)')
    parser.add_argument('-w', '--snowflake-warehouse', type=str, required=True,
                        help='The name of the warehouse to use (e.g. DEPLOYER_WAREHOUSE)')
    parser.add_argument('-d', '--snowflake-database', type=str, required=True,
                        help='The target database name where scripts were applied (e.g. COEDW)')

    # Extraction Arguments
    parser.add_argument('-c', '--change-history-table', type=str, required=True,
                        help='Same value passed as -c to snowdeploy.py (e.g. SNOWCHANGE.CHANGE_HISTORY)')
    parser.add_argument('-b', '--build-id', type=str, required=True,
                        help='Same value passed as -b to snowdeploy.py (id of the current build)')

    # Validation Arguments
    parser.add_argument('-rf', '--report-file', type=str, required=True,
                        help='Full path to write the CSV validation report to')
    parser.add_argument('-vdb', '--validate-databases', type=str, nargs='*', default=None,
                        help='List of specific database names to check views in (e.g. -vdb DB1 DB2 DB3). If omitted, checks all databases.')

    return parser.parse_args()


def main():
    args = parse_args()
    check_snowsql_pwd()

    # Populate standard environment variables expected by get_snowflake_connection()
    os.environ["SNOWFLAKE_ACCOUNT"] = args.snowflake_account
    os.environ["SNOWFLAKE_USER"] = args.snowflake_user
    os.environ["SNOWFLAKE_ROLE"] = args.snowflake_role
    os.environ["SNOWFLAKE_WAREHOUSE"] = args.snowflake_warehouse
    os.environ["SNOWFLAKE_DATABASE"] = args.snowflake_database
    os.environ["SNOWFLAKE_AUTHENTICATOR"] = 'snowflake'

    logger.info("Getting Snowflake Connection via get_snowflake_connection()")
    conn = get_snowflake_connection()
    cs = conn.cursor()

    try:
        # 1. Extract touched tables (in-memory)
        deployed_tables = run_extract_tables(cs, args)
        
        # 2. Validate impacted views for specified databases and generate CSV
        run_validate_views(cs, args, deployed_tables)
    finally:
        cs.close()
        logger.info("Closing Snowflake Connection")
        conn.close()


if __name__ == "__main__":
    main()
