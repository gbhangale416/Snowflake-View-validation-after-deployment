"""
Runs in the VALIDATE stage's deployment job, on a fresh agent that has
automatically downloaded the pipeline artifact published by the DEPLOY
stage. It does not touch CHANGE_HISTORY or any script files -- it only
needs the pre-computed table list written by
extract_deployed_tables_for_build.py.

For one target database, this script:
  1. Loads the deployed table list from the tables file (JSON).
  2. Finds every view referencing those tables via
     SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES (authoritative, can lag)
     with a text-scan of view_definition as an immediate fallback.
  3. Validates each impacted view with SELECT ... LIMIT 0.
  4. Writes a CSV report.

Arguments mirror snowdeploy.py's own flag convention. As in
snowdeploy.py, the password is deliberately NOT a CLI argument -- it's
read from the SNOWSQL_PWD environment variable to avoid secrets showing
up in process listings or pipeline logs.

Usage:
    SNOWSQL_PWD=... python validate_impacted_views.py \\
        -a <account> -u <user> -r <role> -w <warehouse> -d <database> \\
        -tf <tables_file> -rf <report_file>

Exit codes:
    0 -> nothing broken (or no deployed tables / no impacted views found)
    1 -> at least one impacted view failed to resolve
    2 -> configuration / connection error
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


def parse_args():
    parser = argparse.ArgumentParser(
        prog='python validate_impacted_views.py',
        description='Find and validate views that reference tables deployed in this build.',
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument('-a', '--snowflake-account', type=str, required=True,
                         help='The name of the snowflake account (e.g. abc123.east-us-2.azure)')
    parser.add_argument('-u', '--snowflake-user', type=str, required=True,
                         help='The name of the snowflake user (e.g. DEPLOYER)')
    parser.add_argument('-r', '--snowflake-role', type=str, required=True,
                         help='The name of the role to use (e.g. DEPLOYER_ROLE)')
    parser.add_argument('-w', '--snowflake-warehouse', type=str, required=True,
                         help='The name of the warehouse to use (e.g. DEPLOYER_WAREHOUSE)')
    parser.add_argument('-d', '--snowflake-database', type=str, required=True,
                         help='The name of the database that was deployed to (e.g. COEDW)')
    parser.add_argument('-tf', '--tables-file', type=str, required=True,
                         help='Path to the JSON file written by extract_deployed_tables_for_build.py')
    parser.add_argument('-rf', '--report-file', type=str, required=True,
                         help='Full path to write the CSV validation report to')
    return parser.parse_args()


def parse_table_name(name):
    """Returns (schema, table) from a possibly-qualified name; schema None if unqualified."""
    parts = [p.strip('"') for p in name.split(".")]
    if len(parts) >= 2:
        return parts[-2], parts[-1]
    return None, parts[-1]


def find_impacted_views(cs, database, deployed_tables):
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


def main():
    args = parse_args()

    if "SNOWSQL_PWD" not in os.environ:
        logger.error("The SNOWSQL_PWD environment variable has not been defined")
        sys.exit(2)

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

    impacted = find_impacted_views(cs, args.snowflake_database, deployed_tables)
    if not impacted:
        logger.info("No views reference the deployed tables.")
        cs.close()
        conn.close()
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

    cs.close()
    conn.close()

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
