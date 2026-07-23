"""
Runs in the DEPLOY stage, on the same agent/VM that just ran snowdeploy.py,
so SCRIPT_PATH values recorded in CHANGE_HISTORY are guaranteed to still
be on disk at their original absolute paths.

For one target database, this script:
  1. Queries CHANGE_HISTORY for every row with BUILD_ID = this build's id
     (STATUS = 'Success'), split into v_files / r_files by SCRIPT_TYPE.
  2. Reads each of those files and regex-extracts table names from any
     CREATE/ALTER/DROP TABLE statements found (v_files + r_files combined
     -- SCRIPT_TYPE isn't used as a filter since its meaning in this
     codebase is ambiguous; scanning everything applied sidesteps that).
  3. Writes the distinct table list to a JSON file, to be published as a
     pipeline artifact and consumed later by validate_impacted_views.py
     in the VALIDATE stage (which runs on a different agent and has no
     access to these original file paths).

Arguments mirror snowdeploy.py's own flag convention. As in
snowdeploy.py, the password is deliberately NOT a CLI argument -- it's
read from the SNOWSQL_PWD environment variable to avoid secrets showing
up in process listings or pipeline logs.

Usage:
    SNOWSQL_PWD=... python extract_deployed_tables_for_build.py \\
        -a <account> -u <user> -r <role> -w <warehouse> -d <database> \\
        -c <change_history_table> -b <build_id> -o <output_file>
"""

import argparse
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


def parse_args():
    parser = argparse.ArgumentParser(
        prog='python extract_deployed_tables_for_build.py',
        description='Extract table names touched by this build\'s deployment and write them as JSON.',
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
                         help='The name of the database that was just deployed to (e.g. COEDW)')
    parser.add_argument('-c', '--change-history-table', type=str, required=True,
                         help='Same value passed as -c to snowdeploy.py (e.g. SNOWCHANGE.CHANGE_HISTORY)')
    parser.add_argument('-b', '--build-id', type=str, required=True,
                         help='Same value passed as -b to snowdeploy.py (id of the current build)')
    parser.add_argument('-o', '--output-file', type=str, required=True,
                         help='Full path to write the JSON manifest to')
    return parser.parse_args()


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
    """Returns (v_files, r_files): script paths applied in this build,
    split by SCRIPT_TYPE ('V' vs 'R') as recorded in CHANGE_HISTORY."""
    fq_table = '"{database_name}"."{schema_name}"."{table_name}"'.format(**change_history_table)
    try:
        cs.execute(
            f"""
            SELECT DISTINCT SCRIPT_PATH, SCRIPT_TYPE
            FROM {fq_table}
            WHERE BUILD_ID = %(build_id)s
              AND STATUS = 'Success'
            """,
            {"build_id": build_id},
        )
        rows = cs.fetchall()
    except Exception as exc:
        logger.warning(f"Could not query change history table {fq_table}: {exc}")
        return [], []

    v_files = [path for path, script_type in rows if path and script_type == "V"]
    r_files = [path for path, script_type in rows if path and script_type == "R"]
    return v_files, r_files


def extract_deployed_tables(script_paths):
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


def main():
    args = parse_args()

    if "SNOWSQL_PWD" not in os.environ:
        logger.error("The SNOWSQL_PWD environment variable has not been defined")
        sys.exit(2)

    conn = snowflake.connector.connect(
        account=args.snowflake_account,
        user=args.snowflake_user,
        password=os.environ["SNOWSQL_PWD"],
        role=args.snowflake_role,
        warehouse=args.snowflake_warehouse,
        database=args.snowflake_database,
    )
    cs = conn.cursor()

    change_history_table = get_change_history_table_details(args.snowflake_database, args.change_history_table)
    logger.info(f"Change history table: {change_history_table['database_name']}."
                f"{change_history_table['schema_name']}.{change_history_table['table_name']}")

    v_files, r_files = get_applied_script_paths(cs, change_history_table, args.build_id)
    logger.info(f"V scripts applied: {len(v_files)}, R scripts applied: {len(r_files)}")

    deployed_tables = sorted(extract_deployed_tables(v_files + r_files))
    logger.info(f"Tables touched by this deployment to {args.snowflake_database}: {', '.join(deployed_tables) or '(none)'}")

    os.makedirs(os.path.dirname(args.output_file), exist_ok=True)
    with open(args.output_file, "w") as f:
        json.dump({"database": args.snowflake_database, "build_id": args.build_id, "tables": deployed_tables}, f, indent=2)

    logger.info(f"Wrote {args.output_file}")

    cs.close()
    conn.close()


if __name__ == "__main__":
    main()
