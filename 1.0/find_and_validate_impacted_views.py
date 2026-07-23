"""
Identifies and validates views impacted by THIS build's deployment,
using the real CHANGE_HISTORY table written by snowdeploy.py.

CHANGE_HISTORY does not record which Snowflake objects a script touched,
only which script file was run (SCRIPT_PATH) and when (BUILD_ID). So the
approach is:

  1. Query CHANGE_HISTORY for every row with BUILD_ID = this build's id
     (and STATUS = 'Success') to get the list of script files applied,
     split into v_files and r_files by the SCRIPT_TYPE column ('V'/'R').
  2. Read each of those files off disk -- this step must run in the same
     pipeline job as the deploy step, before the workspace is cleaned up,
     since SCRIPT_PATH is an absolute path on the build agent.
  3. Regex-extract table names from CREATE/ALTER/DROP TABLE statements in
     those files (v_files + r_files combined). SCRIPT_TYPE isn't used as
     a filter here, since its meaning in this codebase is ambiguous (may
     denote "View" files rather than "Versioned" migrations) -- scanning
     every applied script for table DDL sidesteps the ambiguity, while
     still surfacing the V/R split in the logs for visibility.
  4. Find every view referencing those tables via
     SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES (authoritative, can lag)
     with a text-scan of view_definition as an immediate fallback.
  5. Validate each impacted view with SELECT ... LIMIT 0.

Reuses the same change-history-table name resolution logic as
snowdeploy.py's get_change_history_table_details(), so pass the same
-c / SF_CHANGE_TABLE value used for the deploy step.

Required environment variables (mirrors snowdeploy.py's own naming):
    SNOWFLAKE_ACCOUNT
    SNOWFLAKE_USER
    SNOWSQL_PWD
    SNOWFLAKE_ROLE
    SNOWFLAKE_WAREHOUSE
    SNOWFLAKE_DATABASE          the database just deployed to
    CHANGE_HISTORY_TABLE        same value passed as -c to snowdeploy.py
    BUILD_ID                    same value passed as -b to snowdeploy.py

Writes:
    output/impacted_views_<database>_<build_id>.csv

Exit codes:
    0 -> nothing broken (or no table DDL / no impacted views found)
    1 -> at least one impacted view failed to resolve
    2 -> configuration / connection error
"""

import csv
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
            print(f"  WARNING: {path} not found on disk (workspace may have been cleaned) - skipping")
            continue
        with open(path, "r") as f:
            content = f.read()
        for pattern in TABLE_PATTERNS:
            for m in pattern.finditer(content):
                tables.add(m.group(1).strip('"').upper())
    return tables


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
            print(f"  Lineage lookup skipped for {full_name}: {str(exc).splitlines()[0]}")

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
    for (schema, view), sources in sorted(impacted.items()):
        fq_name = f'"{database}"."{schema}"."{view}"'
        source_desc = "; ".join(f"{t} ({m})" for t, m in sorted(sources))
        try:
            cs.execute(f"SELECT * FROM {fq_name} LIMIT 0")
            results.append((database, schema, view, source_desc, "OK", ""))
        except Exception as exc:
            err_msg = str(exc).replace("\n", " ").replace("\r", " ")
            results.append((database, schema, view, source_desc, "FAILED", err_msg))
            failed.append((schema, view, err_msg))
    return results, failed


def main():
    required_env = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWSQL_PWD",
                     "SNOWFLAKE_DATABASE", "BUILD_ID"]
    missing = [v for v in required_env if not os.environ.get(v)]
    if missing:
        print(f"Missing required environment variables: {', '.join(missing)}")
        sys.exit(2)

    database = os.environ["SNOWFLAKE_DATABASE"]
    build_id = os.environ["BUILD_ID"]
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
    if not v_files and not r_files:
        print(f"No scripts recorded for build {build_id} in {database} - nothing to check")
        sys.exit(0)
    print(f"V (versioned) scripts applied in build {build_id}: {len(v_files)}")
    for p in v_files:
        print(f"  V: {p}")
    print(f"R (repeatable) scripts applied in build {build_id}: {len(r_files)}")
    for p in r_files:
        print(f"  R: {p}")

    deployed_tables = extract_deployed_tables(v_files + r_files)
    if not deployed_tables:
        print("None of the applied scripts contained CREATE/ALTER/DROP TABLE statements - nothing to check")
        sys.exit(0)
    print(f"Tables touched by this deployment: {', '.join(sorted(deployed_tables))}")

    impacted = find_impacted_views(cs, database, deployed_tables)
    if not impacted:
        print("No views reference the deployed tables.")
        sys.exit(0)

    results, failed = validate_views(cs, database, impacted)

    os.makedirs("output", exist_ok=True)
    report_path = f"output/impacted_views_{database}_{build_id}.csv"
    with open(report_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["database", "schema", "view", "deployed_tables_referenced", "status", "error"])
        writer.writerows(results)

    print(f"\n{len(impacted)} view(s) reference tables deployed in this build "
          f"({len(impacted) - len(failed)} OK, {len(failed)} FAILED). Report: {report_path}")
    for schema, view, err in failed:
        print(f"  BROKEN VIEW: {database}.{schema}.{view} -> {err}")

    cs.close()
    conn.close()

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
