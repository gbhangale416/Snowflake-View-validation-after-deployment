import argparse
import csv
import logging
import os
import re
import sys
from collections import defaultdict
from utility import get_snowflake_connection

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

# Base list of keywords to exclude if present in database name
DEFAULT_EXCLUDED_DB_KEYWORDS = [
    "SANDBOX",
    "PREPROD",
    "CLONE",
    "UAT",
    "COMMON_UTILITY",
    "CONNECTORS_SECRET",
    "CO_AI_AGENTS",
    "DEMO",
    "DROPPED",
    "EVENTS_DB",
    "RBAC_GEN",
    "STREAMLIT_APPS",
    "USER",
    "UTIL"
]


def get_excluded_keywords(target_env=None):
    """
    Dynamically builds the excluded database keywords list.
    If target_env is DEV_TEST (or contains DEV/TEST), exclude DEV and TEST from keyword filtering.
    """
    excluded = list(DEFAULT_EXCLUDED_DB_KEYWORDS)
    env_str = str(target_env).upper() if target_env else ""

    # Only include "DEV" and "TEST" in the exclusion list if we are NOT validating DEV/TEST
    if "DEV" not in env_str and "TEST" not in env_str:
        excluded.extend(["DEV", "TEST"])

    return excluded


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
    """
    Extracts table names touched by CREATE/ALTER/DROP TABLE statements.
    Returns a dictionary mapping table name -> set of script paths that modified it.
    """
    tables_to_scripts = {}
    for path in script_paths:
        if not os.path.exists(path):
            logger.warning(f"{path} not found on disk - skipping")
            continue
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
        for pattern in TABLE_PATTERNS:
            for m in pattern.finditer(content):
                tbl_name = m.group(1).strip('"').upper()
                if tbl_name not in tables_to_scripts:
                    tables_to_scripts[tbl_name] = set()
                tables_to_scripts[tbl_name].add(path)
    return tables_to_scripts


def run_extract_tables(cs, snowflake_database, change_history_table, build_id):
    """Extracts deployed tables for a build and returns the mapping dict."""
    logger.info("--- STARTING TABLE EXTRACTION ---")

    change_history_table_details = get_change_history_table_details(snowflake_database, change_history_table)
    logger.info(f"Change history table: {change_history_table_details['database_name']}."
                f"{change_history_table_details['schema_name']}.{change_history_table_details['table_name']}")

    script_paths = get_applied_script_paths(cs, change_history_table_details, build_id)
    logger.info(f"Total scripts applied for build {build_id}: {len(script_paths)}")

    deployed_tables_map = extract_deployed_tables(script_paths)
    sorted_tables = sorted(deployed_tables_map.keys())
    logger.info(f"Tables touched by this deployment to {snowflake_database}: {', '.join(sorted_tables) or '(none)'}")

    print(f"deployed tables: {sorted_tables}")

    return deployed_tables_map


# =============================================================================
# CROSS-DATABASE VALIDATION & REPORTING LOGIC
# =============================================================================

def find_impacted_views_cross_db(cs, deployed_database, deployed_tables_map, target_env=None):
    """Identifies dependent views across specified (or all) databases in a single query."""
    if not deployed_tables_map:
        return {}

    impacted = {}  # (view_db, view_schema, view_name) -> set of referenced tables

    # Map full table names to their parsed (schema, table_short_name)
    parsed_tables = {full_name: parse_table_name(full_name) for full_name in deployed_tables_map.keys()}
    table_short_names = list({t_name.upper() for _, t_name in parsed_tables.values()})

    # Dynamically build excluded keywords pattern based on target environment
    excluded_keywords = get_excluded_keywords(target_env)
    exclude_pattern = f".*({'|'.join(re.escape(kw) for kw in excluded_keywords)}).*"

    # Construct parameter placeholders for table names
    table_placeholders = [f"%(tbl_{i})s" for i in range(len(table_short_names))]
    params = {f"tbl_{i}": name for i, name in enumerate(table_short_names)}
    params["database"] = deployed_database
    params["exclude_pattern"] = exclude_pattern

    # 1. Primary Lineage Query: Active metadata dependencies
    query = f"""
        SELECT referencing_database, referencing_schema, referencing_object_name,
               referenced_schema, referenced_object_name
        FROM snowflake.account_usage.object_dependencies
        WHERE referenced_object_domain = 'TABLE'
          AND referencing_object_domain = 'VIEW'
          AND UPPER(referenced_database) = UPPER(%(database)s)
          AND UPPER(referenced_object_name) IN ({', '.join(table_placeholders)})
          AND NOT REGEXP_LIKE(referencing_database, %(exclude_pattern)s, 'i')
    """

    try:
        cs.execute(query, params)
        for r_db, r_schema, r_view, ref_schema, ref_table in cs.fetchall():
            ref_schema_upper = ref_schema.upper() if ref_schema else None
            ref_table_upper = ref_table.upper()

            # Match returned dependency back to the deployed_tables list
            for full_name, (expected_schema, expected_table) in parsed_tables.items():
                if expected_table.upper() == ref_table_upper:
                    if expected_schema is None or expected_schema.upper() == ref_schema_upper:
                        impacted.setdefault((r_db, r_schema, r_view), set()).add(full_name)

    except Exception as exc:
        logger.warning(f"Lineage query failed: {str(exc).splitlines()[0]}")

    # 2. Fallback Query: Handles DROP TABLE statements missing from active metadata
    tables_found_in_deps = {tbl for sources in impacted.values() for tbl in sources}
    missing_tables = set(deployed_tables_map.keys()) - tables_found_in_deps

    if missing_tables:
        logger.info(f"Fallback Check: Searching View definitions for dropped/unmatched tables: {sorted(missing_tables)}")
        for full_name in sorted(missing_tables):
            exp_schema, exp_table = parsed_tables[full_name]
            
            fallback_query = """
                SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME
                FROM SNOWFLAKE.ACCOUNT_USAGE.VIEWS
                WHERE DELETED IS NULL
                  AND REGEXP_LIKE(VIEW_DEFINITION, %(tbl_pattern)s, 'i')
                  AND NOT REGEXP_LIKE(TABLE_CATALOG, %(exclude_pattern)s, 'i')
            """
            
            fallback_params = {
                "tbl_pattern": f"\\b{re.escape(exp_table)}\\b",
                "exclude_pattern": exclude_pattern,
            }
            
            try:
                cs.execute(fallback_query, fallback_params)
                for r_db, r_schema, r_view in cs.fetchall():
                    impacted.setdefault((r_db, r_schema, r_view), set()).add(full_name)
                    logger.info(f"Fallback matched dropped table '{full_name}' to view: {r_db}.{r_schema}.{r_view}")
            except Exception as exc:
                logger.warning(f"Fallback view definition query failed for {full_name}: {str(exc).splitlines()[0]}")

    print(f"impacted views: {impacted}")

    return impacted


def validate_views_cross_db(cs, impacted, deployed_tables_map):
    """Validates views across databases with SELECT LIMIT 0."""
    results = []
    failed = []
    total = len(impacted)

    for idx, ((v_db, v_schema, v_view), sources) in enumerate(sorted(impacted.items()), start=1):
        fq_name = f'"{v_db}"."{v_schema}"."{v_view}"'
        logger.info(f"[{idx}/{total}] Checking {v_db}.{v_schema}.{v_view} ...")
        source_tables_desc = "; ".join(sorted(sources))
        
        # Collect all associated script paths that touched any of the referenced tables
        script_paths = set()
        for src_tbl in sources:
            script_paths.update(deployed_tables_map.get(src_tbl, []))
        script_paths_desc = "; ".join(sorted(script_paths)) or "N/A"

        try:
            cs.execute(f"SELECT * FROM {fq_name} LIMIT 0")
            results.append((v_db, script_paths_desc, v_schema, v_view, source_tables_desc, "OK", ""))
            logger.info(f"[{idx}/{total}] OK - {v_db}.{v_schema}.{v_view}")
        except Exception as exc:
            err_msg = str(exc).replace("\n", " ").replace("\r", " ")
            results.append((v_db, script_paths_desc, v_schema, v_view, source_tables_desc, "FAILED", err_msg))
            failed.append((v_db, v_schema, v_view, err_msg))
            logger.error(f"[{idx}/{total}] FAILED - {v_db}.{v_schema}.{v_view}: {err_msg}")

    return results, failed


def create_csv_report(results, report_file):
    """Creates and saves the validation CSV report."""
    os.makedirs(os.path.dirname(os.path.abspath(report_file)), exist_ok=True)
    with open(report_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["View_database", "Script_Path", "Schema", "View", "Deployed_tables_referenced", "Status", "Error"])
        writer.writerows(results)
    logger.info(f"CSV validation report successfully written to: {report_file}")


def run_validate_views(cs, snowflake_database, report_file, deployed_tables_map, target_env=None):
    """Validates cross-database views impacting the deployed tables and generates report."""
    logger.info("--- STARTING CROSS-DATABASE VIEW VALIDATION ---")

    if not deployed_tables_map:
        logger.info(f"No tables recorded for {snowflake_database} in this build - nothing to check")
        return

    logger.info(f"Tables deployed to {snowflake_database}: {', '.join(sorted(deployed_tables_map.keys()))}")
    impacted = find_impacted_views_cross_db(
        cs,
        snowflake_database,
        deployed_tables_map,
        target_env=target_env
    )

    if not impacted:
        logger.info("No views reference the deployed tables across any database.")
        return

    results, failed = validate_views_cross_db(cs, impacted, deployed_tables_map)

    # Generate CSV Report
    create_csv_report(results, report_file)

    logger.info(f"{len(impacted)} view(s) across all databases reference tables deployed in this build "
                f"({len(impacted) - len(failed)} OK, {len(failed)} FAILED).")

    for v_db, schema, view, err in failed:
        logger.error(f"BROKEN VIEW: {v_db}.{schema}.{view} -> {err}")

    if failed:
        sys.exit(1)


# =============================================================================
# MAIN EXECUTOR & CLI PARSER
# =============================================================================

def view_validator(snowflake_account, snowflake_user, snowflake_role, snowflake_warehouse,
                   snowflake_database, change_history_table, build_id, report_file, target_env=None):
    """Main execution entrypoint function for deployment validation."""
    check_snowsql_pwd()

    # Populate standard environment variables expected by get_snowflake_connection()
    os.environ["SNOWFLAKE_ACCOUNT"] = snowflake_account
    os.environ["SNOWFLAKE_USER"] = snowflake_user
    os.environ["SNOWFLAKE_ROLE"] = snowflake_role
    os.environ["SNOWFLAKE_WAREHOUSE"] = snowflake_warehouse
    os.environ["SNOWFLAKE_DATABASE"] = snowflake_database
    os.environ["SNOWFLAKE_AUTHENTICATOR"] = 'snowflake'

    logger.info("Getting Snowflake Connection via get_snowflake_connection()")
    conn = get_snowflake_connection(
        user=snowflake_user,
        account=snowflake_account,
        role=snowflake_role,
        warehouse=snowflake_warehouse,
        database=snowflake_database,
        authenticator=os.environ["SNOWFLAKE_AUTHENTICATOR"],
        password=os.environ["SNOWSQL_PWD"]
    )
    cs = conn.cursor()

    try:
        # 1. Extract touched tables mapping (table_name -> set of script_paths)
        deployed_tables_map = run_extract_tables(cs, snowflake_database, change_history_table, build_id)

        # 2. Validate impacted views for specified databases and generate CSV
        run_validate_views(cs, snowflake_database, report_file, deployed_tables_map, target_env=target_env)
    finally:
        cs.close()
        logger.info("Closing Snowflake Connection")
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog='view_validator.py',
        description='Extract deployed tables and validate affected cross-database views in Snowflake.',
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
                        help='The target database name (e.g. COEDW)')

    # Extraction Arguments
    parser.add_argument('-c', '--change-history-table', type=str, required=True,
                        help='Same value passed as -c to snowdeploy.py (e.g. SNOWCHANGE.CHANGE_HISTORY)')
    parser.add_argument('-b', '--build-id', type=str, required=True,
                        help='Same value passed as -b to snowdeploy.py (id of the current build)')

    # Validation Arguments
    parser.add_argument('-rf', '--report-file', type=str, required=True,
                        help='Full path to write the CSV validation report to')
    parser.add_argument('-e', '--target-env', type=str, required=False, default=None,
                        help='Target environment (e.g. DEV_TEST, PROD, PREPROD)')

    args = parser.parse_args()

    view_validator(
        snowflake_account=args.snowflake_account,
        snowflake_user=args.snowflake_user,
        snowflake_role=args.snowflake_role,
        snowflake_warehouse=args.snowflake_warehouse,
        snowflake_database=args.snowflake_database,
        change_history_table=args.change_history_table,
        build_id=args.build_id,
        report_file=args.report_file,
        target_env=args.target_env
    )
