"""
Validates every view across every database in the Snowflake account by
attempting a zero-row SELECT against each one. Snowflake does not mark
views "invalid" at DDL time -- a view can be created successfully but
later break if an underlying table, column, or referenced object
changes. This script catches that class of failure by actually
querying each view.

Database scope:
  - If SNOWFLAKE_DATABASES is set (comma-separated), only those
    databases are checked.
  - Otherwise, every database visible to the connecting role is
    discovered via SHOW DATABASES and checked.

Exit code:
  0  -> all views resolved successfully
  1  -> one or more views failed (see view_validation_report.csv)
  2  -> missing configuration / connection setup problem
"""

import csv
import os
import sys

import snowflake.connector


def get_target_databases(cs):
    override = os.environ.get("SNOWFLAKE_DATABASES")
    if override:
        return [d.strip() for d in override.split(",") if d.strip()]

    cs.execute("SHOW DATABASES")
    rows = cs.fetchall()
    # SHOW DATABASES columns: ... "name" is typically column index 1
    col_names = [c[0].lower() for c in cs.description]
    name_idx = col_names.index("name")
    return [row[name_idx] for row in rows]


def main():
    required_env = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"]
    missing = [v for v in required_env if not os.environ.get(v)]
    if missing:
        print(f"Missing required environment variables: {', '.join(missing)}")
        sys.exit(2)

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ.get("SNOWFLAKE_ROLE"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE"),
    )
    cs = conn.cursor()

    databases = get_target_databases(cs)
    if not databases:
        print("No databases found to check.")
        sys.exit(2)

    print(f"Databases to check: {', '.join(databases)}")

    results = []
    failed = []
    total_views = 0

    for database in databases:
        try:
            cs.execute(
                f"""
                SELECT table_schema, table_name
                FROM "{database}".information_schema.views
                WHERE table_schema != 'INFORMATION_SCHEMA'
                ORDER BY table_schema, table_name
                """
            )
            views = cs.fetchall()
        except Exception as exc:
            # e.g. no access to this database's information_schema
            err_msg = str(exc).replace("\n", " ").replace("\r", " ")
            print(f"  Skipping database {database}: {err_msg}")
            continue

        total_views += len(views)

        for schema, view in views:
            fq_name = f'"{database}"."{schema}"."{view}"'
            try:
                cs.execute(f"SELECT * FROM {fq_name} LIMIT 0")
                results.append((database, schema, view, "OK", ""))
            except Exception as exc:
                err_msg = str(exc).replace("\n", " ").replace("\r", " ")
                results.append((database, schema, view, "FAILED", err_msg))
                failed.append((database, schema, view, err_msg))

    with open("view_validation_report.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["database", "schema", "view", "status", "error"])
        writer.writerows(results)

    print(f"Checked {total_views} views across {len(databases)} database(s): "
          f"{total_views - len(failed)} OK, {len(failed)} FAILED")
    for database, schema, view, err in failed:
        print(f"  BROKEN VIEW: {database}.{schema}.{view} -> {err}")

    cs.close()
    conn.close()

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
