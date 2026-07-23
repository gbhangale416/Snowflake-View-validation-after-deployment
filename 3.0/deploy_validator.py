"""
Unified Snowdeploy Table Extraction and View Validation CLI.

Provides two main commands:
  - extract: Reads CHANGE_HISTORY for a build, extracts touched tables from applied scripts,
             and writes them to a JSON manifest.
  - validate: Reads a JSON manifest of deployed tables, checks for dependent views in Snowflake,
              validates them using SELECT LIMIT 0, and writes a CSV report.

Usage:
    SNOWSQL_PWD=... python deploy_validator.py extract \\
        -a <account> -u <user> -r <role> -w <warehouse> -d <database> \\
        -c <change_history_table> -b <build_id> -o <output_file>

    SNOWSQL_PWD=... python deploy_validator.py validate \\
        -a <account> -u <user> -r <role> -w <warehouse> -d <database> \\
        -tf <tables_file> -rf <report_file>
"""

import argparse
import csv
import json
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
# SHARED UTILITIES
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


def run_extract_tables(args):
    """Extracts deployed tables for a build and saves to a JSON file."""
    check_snowsql_pwd()

    conn = snowflake.connector.connect(
        account=args.snowflake_account,
        user=args.snowflake_user,
        password=os.environ["SNOWSQL_PWD"],
        role=args.snowflake_role,
        warehouse=args.snowflake_warehouse,
        database=args.snowflake_database,
    )
    cs = conn.cursor()

    try:
        change_history_table = get_change_history_table_details(args.snowflake_database, args.change_history_table)
        logger.info(f"Change history table: {change_history_table['database_name']}."
                    f"{change_history_table['schema_name']}.{change_history_table['table_name']}")

        script_paths = get_applied_script_paths(cs, change_history_table, args.build_id)
        logger.info(f"Total scripts applied for build {args.build_id}: {len(script_paths)}")

        deployed_tables = sorted(extract_deployed_tables(script_paths))
        logger.info(f"Tables touched by this deployment to {args.snowflake_database}: {', '.join(deployed_tables) or '(none)'}")

        os.makedirs(os.path.dirname(args.output_file), exist_ok=True)
        with open(args.output_file, "w") as f:
            json.dump({"database": args.snowflake_database, "build_id": args.build_id, "tables": deployed_tables}, f, indent=2)

        logger.info(f"Wrote {args.output_file}")
    finally:
        cs.close()
        conn.close()


# =============================================================================
# VALIDATION LOGIC
# =============================================================================

def find_impacted_views(cs, database, deployed_tables):
    """Identifies views referencing deployed tables using lineage and text scanning fallback."""
    impacted = {}  # (schema, view) -> set of (table, method)

    # Method 1: account_usage lineage (authoritative, can lag)
    for full_name in deployed_tables:
        schema, table = parse_table_name(full_name)
        try:
            query = """
                SELECT referencing_schema, referencing_object_name
                FROM snowflake.account_usage.object_dependencies
                WHERE referenced_object_domain = 'TABLE'
                  AND referencing_object_domain = 'VIEW'
                  AND UPPER(referenced_database) = UPPER(%(database)s)
                  AND UPPER(referenced_object_name) = UPPER(%(table)s)
            """
            params = {"database": database, "table": table}
            if schema:
                query += " AND UPPER(referenced_schema) = UPPER(%(schema)s)"
                params["schema"] = schema
            cs.execute(query, params)
            for r_schema, r_view in cs.fetchall():
                impacted.setdefault((r_schema, r_view), set()).add((full_name, "lineage"))
        except Exception as exc:
            logger.warning(f"Lineage lookup skipped for {full_name}: {str(exc).splitlines()[0]}")

    # Method 2: text scan fallback (catches lineage lag)
    table_short_names = {parse_table_name(t)[1] for t in deployed_tables}
    if table_short_names:
        patterns = {t: re.compile(r"\b" + re.escape(t) + r"\b", re.IGNORECASE) for t in table_short_names}
        cs.execute(
            f"""
            SELECT table_schema, table_name, view_definition
            FROM "{database}".information_schema.views
            WHERE table_schema != 'INFORMATION_SCHEMA'
            """
        )
        for v_schema, v_name, definition in cs.fetchall():
            if not definition:
                continue
            for short_name, pattern in patterns.items():
                if pattern.search(definition):
                    matched_full = next(
                        (t for t in deployed_tables if parse_table_name(t)[1] == short_name), short_name
                    )
                    impacted.setdefault((v_schema, v_name), set()).add((matched_full, "text scan"))

    return impacted


def validate_views(cs, database, impacted):
    """Validates views with SELECT LIMIT 0."""
    results = []
    failed = []
    total = len(impacted)
    for idx, ((schema, view), sources) in enumerate(sorted(impacted.items()), start=1):
        fq_name = f'"{database}"."{schema}"."{view}"'
        logger.info(f"[{idx}/{total}] Checking {database}.{schema}.{view} ...")
        source_desc = "; ".join(f"{t} ({m})" for t, m in sorted(sources))
        try:
            cs.execute(f"SELECT * FROM {fq_name} LIMIT 0")
            results.append((database, schema, view, source_desc, "OK", ""))
            logger.info(f"[{idx}/{total}] OK - {database}.{schema}.{view}")
        except Exception as exc:
            err_msg = str(exc).replace("\n", " ").replace("\r", " ")
            results.append((database, schema, view, source_desc, "FAILED", err_msg))
            failed.append((schema, view, err_msg))
            logger.error(f"[{idx}/{total}] FAILED - {database}.{schema}.{view}: {err_msg}")
    return results, failed


def run_validate_views(args):
    """Loads deployed tables and validates impacted views."""
    check_snowsql_pwd()

    if not os.path.exists(args.tables_file):
        logger.info(f"{args.tables_file} not found - nothing to validate")
        sys.exit(0)

    with open(args.tables_file) as f:
        manifest = json.load(f)
    deployed_tables = manifest.get("tables", [])

    if not deployed_tables:
        logger.info(f"No tables recorded for {args.snowflake_database} in this build - nothing to check")
        sys.exit(0)

    logger.info(f"Tables deployed to {args.snowflake_database}: {', '.join(deployed_tables)}")

    conn = snowflake.connector.connect(
        account=args.snowflake_account,
        user=args.snowflake_user,
        password=os.environ["SNOWSQL_PWD"],
        role=args.snowflake_role,
        warehouse=args.snowflake_warehouse,
        database=args.snowflake_database,
    )
    cs = conn.cursor()

    try:
        impacted = find_impacted_views(cs, args.snowflake_database, deployed_tables)
        if not impacted:
            logger.info("No views reference the deployed tables.")
            sys.exit(0)

        results, failed = validate_views(cs, args.snowflake_database, impacted)

        os.makedirs(os.path.dirname(args.report_file), exist_ok=True)
        with open(args.report_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["database", "schema", "view", "deployed_tables_referenced", "status", "error"])
            writer.writerows(results)

        logger.info(f"{len(impacted)} view(s) reference tables deployed in this build "
                    f"({len(impacted) - len(failed)} OK, {len(failed)} FAILED). Report: {args.report_file}")
        for schema, view, err in failed:
            logger.error(f"BROKEN VIEW: {args.snowflake_database}.{schema}.{view} -> {err}")

        if failed:
            sys.exit(1)
    finally:
        cs.close()
        conn.close()


# =============================================================================
# CLI PARSER
# =============================================================================

def parse_args():
    parser = argparse.ArgumentParser(
        prog='deploy_validator.py',
        description='Extract deployed tables and validate affected views in Snowflake.',
        formatter_class=argparse.RawTextHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest='command', required=True, help='Action to perform')

    # Shared Snowflake Connection Arguments
    common_args = argparse.ArgumentParser(add_help=False)
    common_args.add_argument('-a', '--snowflake-account', type=str, required=True,
                             help='The name of the snowflake account (e.g. abc123.east-us-2.azure)')
    common_args.add_argument('-u', '--snowflake-user', type=str, required=True,
                             help='The name of the snowflake user (e.g. DEPLOYER)')
    common_args.add_argument('-r', '--snowflake-role', type=str, required=True,
                             help='The name of the role to use (e.g. DEPLOYER_ROLE)')
    common_args.add_argument('-w', '--snowflake-warehouse', type=str, required=True,
                             help='The name of the warehouse to use (e.g. DEPLOYER_WAREHOUSE)')
    common_args.add_argument('-d', '--snowflake-database', type=str, required=True,
                             help='The target database name (e.g. COEDW)')

    # Subcommand: Extract
    extract_parser = subparsers.add_parser('extract', parents=[common_args], help='Extract touched tables from deployed scripts.')
    extract_parser.add_argument('-c', '--change-history-table', type=str, required=True,
                                help='Same value passed as -c to snowdeploy.py (e.g. SNOWCHANGE.CHANGE_HISTORY)')
    extract_parser.add_argument('-b', '--build-id', type=str, required=True,
                                help='Same value passed as -b to snowdeploy.py (id of the current build)')
    extract_parser.add_argument('-o', '--output-file', type=str, required=True,
                                help='Full path to write the JSON manifest to')

    # Subcommand: Validate
    validate_parser = subparsers.add_parser('validate', parents=[common_args], help='Validate views referencing deployed tables.')
    validate_parser.add_argument('-tf', '--tables-file', type=str, required=True,
                                 help='Path to the JSON file written by extract command')
    validate_parser.add_argument('-rf', '--report-file', type=str, required=True,
                                 help='Full path to write the CSV validation report to')

    return parser.parse_args()


def main():
    args = parse_args()
    if args.command == 'extract':
        run_extract_tables(args)
    elif args.command == 'validate':
        run_validate_views(args)


if __name__ == "__main__":
    main()