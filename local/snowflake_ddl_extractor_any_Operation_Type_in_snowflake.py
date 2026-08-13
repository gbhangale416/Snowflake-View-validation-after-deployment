import csv
import logging
import sys
from pathlib import Path

import sqlglot
from sqlglot import exp

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def get_sql_files(target_path) -> list[str]:
    """Resolves a file, folder, or list of paths into a list of valid .sql file paths."""
    sql_files = set()

    if isinstance(target_path, (list, tuple, set)):
        for p in target_path:
            sql_files.update(get_sql_files(p))
        return sorted(list(sql_files))

    path = Path(target_path)

    if not path.exists():
        logger.warning(f"Path does not exist: {target_path}")
        return []

    if path.is_file() and path.suffix.lower() == ".sql":
        sql_files.add(str(path.resolve()))
    elif path.is_dir():
        for file in path.rglob("*.sql"):
            sql_files.add(str(file.resolve()))

    return sorted(list(sql_files))


def parse_table_components(table_expr: exp.Table, current_db: str = "", current_schema: str = "") -> dict[str, str]:
    """Extracts Database, Schema, and Table name components from a SQLGlot Table object."""
    db = table_expr.catalog.replace('"', "").upper() if table_expr.catalog else current_db
    schema = table_expr.db.replace('"', "").upper() if table_expr.db else current_schema
    table = table_expr.name.replace('"', "").upper() if table_expr.name else ""

    parts = [p for p in [db, schema, table] if p]
    fqn = ".".join(parts)

    return {
        "database": db,
        "schema": schema,
        "table_name": table,
        "fqn": fqn,
    }


def detect_operation_type(statement: exp.Expression, dialect: str = "snowflake") -> str:
    """Detects the specific operation type (CREATE, ALTER sub-action, DROP, INSERT, MERGE, etc.)."""
    sql_str = statement.sql(dialect=dialect).upper()

    # 1. CREATE Operations
    if isinstance(statement, exp.Create):
        kind = str(statement.args.get("kind", "")).upper() or "TABLE"
        if not kind.endswith("TABLE"):
            kind = f"{kind} TABLE".strip()

        is_replace = "OR REPLACE" in sql_str
        is_ctas = bool(statement.find(exp.Select))

        prefix = "CREATE OR REPLACE" if is_replace else "CREATE"
        suffix = " (CTAS)" if is_ctas else ""
        return f"{prefix} {kind}{suffix}"

    # 2. DROP Operations
    elif isinstance(statement, exp.Drop):
        return "DROP TABLE"

    # 3. ALTER Operations (Detailed Sub-actions)
    elif isinstance(statement, exp.Alter):
        if "SWAP WITH" in sql_str or "SWAP" in sql_str:
            return "ALTER TABLE (SWAP WITH)"
        elif "RENAME TO" in sql_str or "RENAME" in sql_str:
            return "ALTER TABLE (RENAME TO)"
        elif "ADD COLUMN" in sql_str or "ADD " in sql_str or "ADD (" in sql_str:
            return "ALTER TABLE (ADD COLUMN)"
        elif "DROP COLUMN" in sql_str or "DROP " in sql_str:
            return "ALTER TABLE (DROP COLUMN)"
        elif "MODIFY COLUMN" in sql_str or "ALTER COLUMN" in sql_str or "MODIFY" in sql_str:
            return "ALTER TABLE (MODIFY COLUMN)"
        elif "SET" in sql_str:
            return "ALTER TABLE (SET PROPERTIES)"
        return "ALTER TABLE"

    # 4. DML Operations
    elif isinstance(statement, exp.Insert):
        return "INSERT INTO"
    elif isinstance(statement, exp.Update):
        return "UPDATE"
    elif isinstance(statement, exp.Delete):
        return "DELETE FROM"
    elif isinstance(statement, exp.Merge):
        return "MERGE INTO"
    elif isinstance(statement, exp.Copy):
        return "COPY INTO"

    # 5. Maintenance & Auxiliary Statements
    elif isinstance(statement, exp.TruncateTable):
        return "TRUNCATE TABLE"
    elif isinstance(statement, exp.Comment):
        return "COMMENT ON TABLE"
    elif "UNDROP" in sql_str:
        return "UNDROP TABLE"

    return "OTHER OPERATION"


def extract_tables_from_ast(
    statement: exp.Expression,
    current_db: str = "",
    current_schema: str = "",
    dialect: str = "snowflake"
) -> list[dict[str, str]]:
    """Extracts DDL/DML target tables and identifies their specific operation type."""
    target_tables = []
    kind = str(statement.args.get("kind", "")).upper()
    op_type = detect_operation_type(statement, dialect=dialect)

    # -------------------------------------------------------------------------
    # 1. Recursive Parsing for Procedures & Dynamic SQL Strings inside blocks
    # -------------------------------------------------------------------------
    if "PROCEDURE" in kind or isinstance(statement, (exp.Create, exp.Procedure, exp.Command)):
        for literal in statement.find_all(exp.Literal):
            literal_str = literal.this
            if isinstance(literal_str, str) and any(
                kw in literal_str.upper() for kw in ["CREATE", "ALTER", "DROP", "INSERT", "UPDATE", "MERGE", "TRUNCATE", "TABLE"]
            ):
                try:
                    for inner_stmt in sqlglot.parse(literal_str, read=dialect):
                        if inner_stmt:
                            target_tables.extend(
                                extract_tables_from_ast(inner_stmt, current_db, current_schema, dialect)
                            )
                except Exception:
                    pass

    # -------------------------------------------------------------------------
    # 2. DDL Statements (CREATE / DROP / UNDROP)
    # -------------------------------------------------------------------------
    if isinstance(statement, (exp.Create, exp.Drop)):
        is_table = any(k in kind for k in ["TABLE", "DYNAMIC", "ICEBERG", "EXTERNAL", "HYBRID", "EVENT"]) or not kind
        if is_table:
            target = statement.this
            if isinstance(target, exp.Schema):
                target = target.this

            if isinstance(target, exp.Table):
                details = parse_table_components(target, current_db, current_schema)
                if details["table_name"]:
                    details["operation"] = op_type
                    target_tables.append(details)

    # -------------------------------------------------------------------------
    # 3. DDL Alter Statements (ALTER TABLE, RENAME TO, SWAP WITH, ADD COLUMN)
    # -------------------------------------------------------------------------
    elif isinstance(statement, exp.Alter):
        is_table = any(k in kind for k in ["TABLE", "DYNAMIC", "ICEBERG", "EXTERNAL", "HYBRID", "EVENT"]) or not kind
        if is_table:
            for table_expr in statement.find_all(exp.Table):
                details = parse_table_components(table_expr, current_db, current_schema)
                if details["table_name"]:
                    details["operation"] = op_type
                    target_tables.append(details)

    # -------------------------------------------------------------------------
    # 4. Truncate & Comment Statements
    # -------------------------------------------------------------------------
    elif isinstance(statement, (exp.TruncateTable, exp.Comment)):
        for table_expr in statement.find_all(exp.Table):
            details = parse_table_components(table_expr, current_db, current_schema)
            if details["table_name"]:
                details["operation"] = op_type
                target_tables.append(details)

    # -------------------------------------------------------------------------
    # 5. DML Statements (INSERT INTO, MERGE INTO, UPDATE, DELETE FROM, COPY INTO)
    # -------------------------------------------------------------------------
    elif isinstance(statement, (exp.Insert, exp.Merge, exp.Update, exp.Delete, exp.Copy)):
        target = statement.this
        if isinstance(target, exp.Schema):
            target = target.this

        if isinstance(target, exp.Table):
            details = parse_table_components(target, current_db, current_schema)
            if details["table_name"]:
                details["operation"] = op_type
                target_tables.append(details)

    return target_tables


def extract_deployed_tables(path_or_paths, dialect: str = "snowflake") -> list[dict[str, str]]:
    """Extracts table details from SQL files, tracking USE statements, DDLs, DMLs, and Procedure bodies."""
    script_paths = get_sql_files(path_or_paths)

    if not script_paths:
        logger.info("No .sql files found to process.")
        return []

    records = []

    for path in script_paths:
        try:
            with open(path, "r", encoding="utf-8") as f:
                content = f.read()

            current_db = ""
            current_schema = ""

            for statement in sqlglot.parse(content, read=dialect):
                if not statement:
                    continue

                # Maintain active USE DATABASE / USE SCHEMA state per file
                if isinstance(statement, exp.Use):
                    kind = str(statement.args.get("kind", "")).upper()
                    target_str = statement.this.sql(dialect=dialect).replace('"', "").upper() if statement.this else ""

                    if "DATABASE" in kind or "DB" in kind:
                        current_db = target_str
                    elif "SCHEMA" in kind:
                        current_schema = target_str
                    else:
                        parts = target_str.split(".")
                        if len(parts) == 2:
                            current_db, current_schema = parts[0], parts[1]
                        elif len(parts) == 1:
                            current_schema = parts[0]
                    continue

                # Extract target tables with active context & operation types
                extracted = extract_tables_from_ast(statement, current_db, current_schema, dialect)
                for item in extracted:
                    item["script_path"] = path
                    records.append(item)

        except Exception as exc:
            logger.error(f"Failed to parse SQL file {path}: {exc}")

    return records


def create_csv_report(records: list[dict[str, str]], report_file: str) -> None:
    """Generates and writes a detailed CSV report containing Database, Schema, Table Name, Operation Type, and Script Path."""
    output_path = Path(report_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Deduplicate exact duplicate records
    unique_records = [dict(t) for t in {tuple(d.items()) for d in records}]

    with open(output_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Database", "Schema", "Table_Name", "Fully_Qualified_Name", "Operation_Type", "Script_Path"])

        sorted_records = sorted(
            unique_records,
            key=lambda x: (x["database"], x["schema"], x["table_name"], x["operation"], x["script_path"]),
        )

        for rec in sorted_records:
            writer.writerow([
                rec["database"],
                rec["schema"],
                rec["table_name"],
                rec["fqn"],
                rec["operation"],
                rec["script_path"],
            ])

    logger.info(f"CSV report successfully created at: {output_path.resolve()}")


# ==============================================================================
# EXECUTION
# ==============================================================================
if __name__ == "__main__":
    folder_path = r"./path/to/your/sql_folder"
    csv_report_path = r"./output/deployed_tables_report.csv"

    if len(sys.argv) > 1:
        folder_path = sys.argv[1]
    if len(sys.argv) > 2:
        csv_report_path = sys.argv[2]

    logger.info(f"Scanning directory: {folder_path} ...")
    results = extract_deployed_tables(folder_path, dialect="snowflake")

    if results:
        create_csv_report(results, csv_report_path)

        print(f"\nFound {len(results)} table operation(s) across SQL files:\n")
        print("-" * 110)
        for r in results:
            print(
                f"DB: {r['database'] or 'N/A':<12} | "
                f"Schema: {r['schema'] or 'N/A':<12} | "
                f"Table: {r['table_name']:<18} | "
                f"Operation: {r['operation']:<28} | "
                f"Script: {r['script_path']}"
            )
        print("-" * 110)
    else:
        print("\nNo matching table operations (DDL/DML) were found.")
