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

Required environment variables:
    SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWSQL_PWD
    SNOWFLAKE_ROLE, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE
    CHANGE_HISTORY_TABLE   same value passed as -c to snowdeploy.py
    BUILD_ID               same value passed as -b to snowdeploy.py
    OUTPUT_FILE             full path to write the JSON manifest to
"""

import json
import os
import re
import sys

import snowflake.connector

TABLE_PATTERNS = [
    re.compile(r'CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([\w."]+)', re.IGNORECASE),
    re.compile(r'ALTER\s+TABLE\s+([\w."]+)', re.IGNORECASE),
    re.compile(r'DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?([\w."]+)', re.IGNORECASE),
]


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
        print(f"WARNING: could not query change history table {fq_table}: {exc}")
        return [], []

    v_files = [path for path, script_type in rows if path and script_type == "V"]
    r_files = [path for path, script_type in rows if path and script_type == "R"]
    return v_files, r_files


def extract_deployed_tables(script_paths):
    tables = set()
    for path in script_paths:
        if not os.path.exists(path):
            print(f"  WARNING: {path} not found on disk - skipping")
            continue
        with open(path, "r") as f:
            content = f.read()
        for pattern in TABLE_PATTERNS:
            for m in pattern.finditer(content):
                tables.add(m.group(1).strip('"').upper())
    return tables


def main():
    required_env = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWSQL_PWD",
                     "SNOWFLAKE_DATABASE", "BUILD_ID", "OUTPUT_FILE"]
    missing = [v for v in required_env if not os.environ.get(v)]
    if missing:
        print(f"Missing required environment variables: {', '.join(missing)}")
        sys.exit(2)

    database = os.environ["SNOWFLAKE_DATABASE"]
    build_id = os.environ["BUILD_ID"]
    output_file = os.environ["OUTPUT_FILE"]
    change_history_table_override = os.environ.get("CHANGE_HISTORY_TABLE")

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWSQL_PWD"],
        role=os.environ.get("SNOWFLAKE_ROLE"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE"),
        database=database,
    )
    cs = conn.cursor()

    change_history_table = get_change_history_table_details(database, change_history_table_override)
    print(f"Change history table: {change_history_table['database_name']}."
          f"{change_history_table['schema_name']}.{change_history_table['table_name']}")

    v_files, r_files = get_applied_script_paths(cs, change_history_table, build_id)
    print(f"V scripts applied: {len(v_files)}, R scripts applied: {len(r_files)}")

    deployed_tables = sorted(extract_deployed_tables(v_files + r_files))
    print(f"Tables touched by this deployment to {database}: {', '.join(deployed_tables) or '(none)'}")

    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, "w") as f:
        json.dump({"database": database, "build_id": build_id, "tables": deployed_tables}, f, indent=2)

    print(f"Wrote {output_file}")

    cs.close()
    conn.close()


if __name__ == "__main__":
    main()
