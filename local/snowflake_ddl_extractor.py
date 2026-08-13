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


def parse_table_components(table_expr: exp.Table) -> dict[str, str]:
    """Extracts Database, Schema, and Table name components from a SQLGlot Table object."""
    db = table_expr.catalog.replace('"', "").upper() if table_expr.catalog else ""
    schema = table_expr.db.replace('"', "").upper() if table_expr.db else ""
    table = table_expr.name.replace('"', "").upper() if table_expr.name else ""

    # Build full qualified name depending on present components
    parts = [p for p in [db, schema, table] if p]
    fqn = ".".join(parts)

    return {
        "database": db,
        "schema": schema,
        "table_name": table,
        "fqn": fqn,
    }


def extract_target_tables(statement: exp.Expression) -> list[dict[str, str]]:
    """Extracts target table details modified by DDL statements (CREATE, ALTER, DROP, UNDROP)."""
    target_tables = []
    kind = str(statement.args.get("kind", "")).upper()

    # Handle CREATE / DROP / UNDROP DDLs
    if isinstance(statement, (exp.Create, exp.Drop)):
        if "TABLE" in kind or not kind:
            target = statement.this
            if isinstance(target, exp.Schema):
                target = target.this

            if isinstance(target, exp.Table):
                details = parse_table_components(target)
                if details["table_name"]:
                    target_tables.append(details)

    # Handle ALTER TABLE DDLs (e.g. ALTER TABLE, SWAP WITH, RENAME TO)
    elif isinstance(statement, exp.Alter):
        if "TABLE" in kind or not kind:
            for table_expr in statement.find_all(exp.Table):
                details = parse_table_components(table_expr)
                if details["table_name"]:
                    target_tables.append(details)

    return target_tables


def extract_deployed_tables(path_or_paths, dialect: str = "snowflake") -> list[dict[str, str]]:
    """Extracts table details touched by CREATE/ALTER/DROP TABLE statements using AST parsing.

    Returns:
      list of dicts containing database, schema, table_name, fqn, and script_path
    """
    script_paths = get_sql_files(path_or_paths)

    if not script_paths:
        logger.info("No .sql files found to process.")
        return []

    records = []

    for path in script_paths:
        try:
            with open(path, "r", encoding="utf-8") as f:
                content = f.read()

            # AST parse using SQLGlot
            for statement in sqlglot.parse(content, read=dialect):
                if not statement:
                    continue

                extracted = extract_target_tables(statement)
                for item in extracted:
                    item["script_path"] = path
                    records.append(item)

        except Exception as exc:
            logger.error(f"Failed to parse SQL file {path}: {exc}")

    return records


def create_csv_report(records: list[dict[str, str]], report_file: str) -> None:
    """Generates and writes a detailed CSV validation report containing Database, Schema, and Table Name."""
    output_path = Path(report_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        # Header columns
        writer.writerow(["Database", "Schema", "Table_Name", "Fully_Qualified_Name", "Script_Path"])

        # Write rows sorted by FQN and Script Path
        sorted_records = sorted(
            records,
            key=lambda x: (x["database"], x["schema"], x["table_name"], x["script_path"]),
        )

        for rec in sorted_records:
            writer.writerow([
                rec["database"],
                rec["schema"],
                rec["table_name"],
                rec["fqn"],
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
        # Generate CSV Report
        create_csv_report(results, csv_report_path)

        # Print Summary to Console
        print(f"\nFound {len(results)} table DDL operations across SQL files:\n")
        print("-" * 80)
        for r in results:
            print(
                f"DB: {r['database'] or 'N/A':<15} | "
                f"Schema: {r['schema'] or 'N/A':<15} | "
                f"Table: {r['table_name']:<20} | "
                f"Script: {r['script_path']}"
            )
        print("-" * 80)
    else:
        print("\nNo matching DDL statements (CREATE/ALTER/DROP TABLE) were found.")
