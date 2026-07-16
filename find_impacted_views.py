"""
Identifies every view that references a table deployed in this run.

Two detection methods are combined:
  1. Lineage lookup against SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES
     -- Snowflake's own dependency graph. Authoritative, but can lag
     up to ~3 hours behind a just-completed deployment.
  2. A text scan of information_schema.views.view_definition for each
     deployed table's name -- gives immediate results even before the
     account_usage lineage view has caught up. This is a heuristic
     (regex word-boundary match) so it can occasionally produce a
     false positive, e.g. a view whose SQL happens to mention the same
     word in a comment or string literal.

Usage:
    python find_impacted_views.py <deployed_tables.txt>

Requires SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES access, which
means the connecting role needs IMPORTED PRIVILEGES on the SNOWFLAKE
database (grant imported privileges on database snowflake to role ...).
If that privilege is missing, method 1 is skipped with a warning and
method 2 alone still produces results.

Outputs:
    output/impacted_views.csv       -- database, schema, view, deployed_table, method
    output/impacted_views_list.txt  -- plain "db.schema.view" per line
"""

import csv
import os
import re
import sys

import snowflake.connector


def parse_qualified_name(name):
    parts = [p.strip('"') for p in name.split(".")]
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    elif len(parts) == 2:
        return None, parts[0], parts[1]
    return None, None, parts[0]


def get_databases(cs):
    override = os.environ.get("SNOWFLAKE_DATABASES")
    if override:
        return [d.strip() for d in override.split(",") if d.strip()]
    cs.execute("SHOW DATABASES")
    col_names = [c[0].lower() for c in cs.description]
    name_idx = col_names.index("name")
    return [row[name_idx] for row in cs.fetchall()]


def main():
    required_env = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"]
    missing = [v for v in required_env if not os.environ.get(v)]
    if missing:
        print(f"Missing required environment variables: {', '.join(missing)}")
        sys.exit(2)

    tables_file = sys.argv[1] if len(sys.argv) > 1 else "deployed_tables.txt"
    if not os.path.exists(tables_file):
        print(f"{tables_file} not found - nothing to check")
        sys.exit(0)

    with open(tables_file) as f:
        deployed_tables = [line.strip() for line in f if line.strip()]

    if not deployed_tables:
        print("No tables were deployed in this run - nothing to check")
        sys.exit(0)

    print(f"Deployed tables to check: {', '.join(deployed_tables)}")

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ.get("SNOWFLAKE_ROLE"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE"),
    )
    cs = conn.cursor()

    impacted = set()  # (database, schema, view, deployed_table, method)

    # --- Method 1: account_usage lineage (authoritative, can lag) ---
    for table in deployed_tables:
        db, schema, tbl = parse_qualified_name(table)
        try:
            query = """
                SELECT referencing_database, referencing_schema, referencing_object_name
                FROM snowflake.account_usage.object_dependencies
                WHERE referenced_object_domain = 'TABLE'
                  AND referencing_object_domain = 'VIEW'
                  AND UPPER(referenced_object_name) = UPPER(%s)
            """
            params = [tbl]
            if schema:
                query += " AND UPPER(referenced_schema) = UPPER(%s)"
                params.append(schema)
            if db:
                query += " AND UPPER(referenced_database) = UPPER(%s)"
                params.append(db)
            cs.execute(query, params)
            for r_db, r_schema, r_view in cs.fetchall():
                impacted.add((r_db, r_schema, r_view, table, "lineage (account_usage)"))
        except Exception as exc:
            print(f"  Lineage lookup skipped for {table}: {str(exc).splitlines()[0]}")

    # --- Method 2: text scan fallback, for immediate feedback ---
    databases = get_databases(cs)
    table_short_names = {parse_qualified_name(t)[2].upper() for t in deployed_tables}
    patterns = {t: re.compile(r"\b" + re.escape(t) + r"\b", re.IGNORECASE) for t in table_short_names}

    for database in databases:
        try:
            cs.execute(f"""
                SELECT table_schema, table_name, view_definition
                FROM "{database}".information_schema.views
                WHERE table_schema != 'INFORMATION_SCHEMA'
            """)
            rows = cs.fetchall()
        except Exception:
            continue

        for schema, view, definition in rows:
            if not definition:
                continue
            for short_name, pattern in patterns.items():
                if pattern.search(definition):
                    matched_full = next(
                        (t for t in deployed_tables if parse_qualified_name(t)[2].upper() == short_name),
                        short_name,
                    )
                    already_via_lineage = any(
                        d == database and s == schema and v == view and mt == matched_full
                        for d, s, v, mt, _ in impacted
                    )
                    if not already_via_lineage:
                        impacted.add((database, schema, view, matched_full, "text scan (view_definition)"))

    impacted = sorted(impacted)

    os.makedirs("output", exist_ok=True)
    with open("output/impacted_views.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["database", "schema", "view", "deployed_table", "detection_method"])
        writer.writerows(impacted)

    with open("output/impacted_views_list.txt", "w") as f:
        for d, s, v, _, _ in impacted:
            f.write(f"{d}.{s}.{v}\n")

    print(f"\nFound {len(impacted)} view(s) referencing deployed tables.")
    for d, s, v, t, m in impacted:
        print(f"  {d}.{s}.{v}  <- {t}  [{m}]")

    cs.close()
    conn.close()


if __name__ == "__main__":
    main()
