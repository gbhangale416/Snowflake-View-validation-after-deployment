"""
Runs in the VALIDATE stage's deployment job, on a fresh agent that has
automatically downloaded the pipeline artifact published by the DEPLOY
stage. It does not touch CHANGE_HISTORY or any script files -- it only
needs the pre-computed table list written by
extract_deployed_tables_for_build.py.

For one target database, this script:
  1. Loads the deployed table list from TABLES_FILE (JSON).
  2. Finds every view referencing those tables via
     SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES (authoritative, can lag)
     with a text-scan of view_definition as an immediate fallback.
  3. Validates each impacted view with SELECT ... LIMIT 0.
  4. Writes a CSV report.

Required environment variables:
    SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWSQL_PWD
    SNOWFLAKE_ROLE, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE
    TABLES_FILE     path to the JSON file written by
                    extract_deployed_tables_for_build.py
    REPORT_FILE     full path to write the CSV report to

Exit codes:
    0 -> nothing broken (or no deployed tables / no impacted views found)
    1 -> at least one impacted view failed to resolve
    2 -> configuration / connection error
"""

import csv
import json
import os
import re
import sys

import snowflake.connector


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
                     "SNOWFLAKE_DATABASE", "TABLES_FILE", "REPORT_FILE"]
    missing = [v for v in required_env if not os.environ.get(v)]
    if missing:
        print(f"Missing required environment variables: {', '.join(missing)}")
        sys.exit(2)

    database = os.environ["SNOWFLAKE_DATABASE"]
    tables_file = os.environ["TABLES_FILE"]
    report_file = os.environ["REPORT_FILE"]

    if not os.path.exists(tables_file):
        print(f"{tables_file} not found - nothing to validate")
        sys.exit(0)

    with open(tables_file) as f:
        manifest = json.load(f)
    deployed_tables = manifest.get("tables", [])

    if not deployed_tables:
        print(f"No tables recorded for {database} in this build - nothing to check")
        sys.exit(0)

    print(f"Tables deployed to {database}: {', '.join(deployed_tables)}")

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWSQL_PWD"],
        role=os.environ.get("SNOWFLAKE_ROLE"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE"),
        database=database,
    )
    cs = conn.cursor()

    impacted = find_impacted_views(cs, database, deployed_tables)
    if not impacted:
        print("No views reference the deployed tables.")
        cs.close()
        conn.close()
        sys.exit(0)

    results, failed = validate_views(cs, database, impacted)

    os.makedirs(os.path.dirname(report_file), exist_ok=True)
    with open(report_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["database", "schema", "view", "deployed_tables_referenced", "status", "error"])
        writer.writerows(results)

    print(f"\n{len(impacted)} view(s) reference tables deployed in this build "
          f"({len(impacted) - len(failed)} OK, {len(failed)} FAILED). Report: {report_file}")
    for schema, view, err in failed:
        print(f"  BROKEN VIEW: {database}.{schema}.{view} -> {err}")

    cs.close()
    conn.close()

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
