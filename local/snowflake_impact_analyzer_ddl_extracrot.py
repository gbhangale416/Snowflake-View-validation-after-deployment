import csv
import logging
import sys
from pathlib import Path

import sqlglot
from sqlglot import TokenType, exp

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


def parse_raw_fqn(raw_name: str) -> dict[str, str]:
    """Parses a 1, 2, or 3-part Snowflake identifier into Database, Schema, and Table components."""
    parts = [p.strip().strip('"').upper() for p in raw_name.split(".")]
    db, schema, table = "", "", ""

    if len(parts) == 3:
        db, schema, table = parts[0], parts[1], parts[2]
    elif len(parts) == 2:
        schema, table = parts[0], parts[1]
    elif len(parts) == 1:
        table = parts[0]

    fqn = ".".join([p for p in [db, schema, table] if p])

    return {
        "database": db,
        "schema": schema,
        "table_name": table,
        "fqn": fqn,
    }


def parse_table_components(table_expr: exp.Table) -> dict[str, str]:
    """Extracts Database, Schema, and Table components from a SQLGlot AST Table node."""
    db = table_expr.catalog.replace('"', "").upper() if table_expr.catalog else ""
    schema = table_expr.db.replace('"', "").upper() if table_expr.db else ""
    table = table_expr.name.replace('"', "").upper() if table_expr.name else ""

    parts = [p for p in [db, schema, table] if p]
    fqn = ".".join(parts)

    return {
        "database": db,
        "schema": schema,
        "table_name": table,
        "fqn": fqn,
    }


def extract_target_tables_ast(statement: exp.Expression) -> list[dict[str, str]]:
    """Primary extraction via SQLGlot AST parsing."""
    target_tables = []
    kind = str(statement.args.get("kind", "")).upper()

    if isinstance(statement, (exp.Create, exp.Drop)):
        if "TABLE" in kind or not kind:
            target = statement.this
            if isinstance(target, exp.Schema):
                target = target.this
            if isinstance(target, exp.Table):
                details = parse_table_components(target)
                if details["table_name"]:
                    target_tables.append(details)

    elif isinstance(statement, exp.Alter):
        if "TABLE" in kind or not kind:
            for table_expr in statement.find_all(exp.Table):
                details = parse_table_components(table_expr)
                if details["table_name"]:
                    target_tables.append(details)

    return target_tables


def extract_target_tables_tokens(content: str, dialect: str = "snowflake") -> list[dict[str, str]]:
    """Fallback extraction using pure SQLGlot Lexical Tokenizer (0% Regex)."""
    tables = []
    try:
        tokens = sqlglot.tokenize(content, read=dialect)
    except Exception as exc:
        logger.error(f"Tokenization failed: {exc}")
        return []

    num_tokens = len(tokens)
    ddl_triggers = {TokenType.CREATE, TokenType.ALTER, TokenType.DROP, TokenType.UNDROP}

    i = 0
    while i < num_tokens:
        token = tokens[i]

        if token.token_type in ddl_triggers:
            j = i + 1

            while j < num_tokens and tokens[j].token_type in {
                TokenType.OR,
                TokenType.REPLACE,
                TokenType.TRANSIENT,
                TokenType.TEMPORARY,
                TokenType.VOLATILE,
                TokenType.EXTERNAL,
                TokenType.VAR,
            }:
                if tokens[j].token_type == TokenType.TABLE or tokens[j].text.upper() == "TABLE":
                    break
                j += 1

            if j < num_tokens and (tokens[j].token_type == TokenType.TABLE or tokens[j].text.upper() == "TABLE"):
                k = j + 1

                if k < num_tokens and tokens[k].token_type == TokenType.IF:
                    while k < num_tokens and tokens[k].token_type in {
                        TokenType.IF,
                        TokenType.NOT,
                        TokenType.EXISTS,
                    }:
                        k += 1

                name_parts = []
                while k < num_tokens and tokens[k].token_type in {
                    TokenType.VAR,
                    TokenType.IDENTIFIER,
                    TokenType.DOT,
                }:
                    name_parts.append(tokens[k].text)
                    k += 1

                raw_fqn = "".join(name_parts).strip(".")
                if raw_fqn:
                    tables.append(parse_raw_fqn(raw_fqn))

        i += 1

    return tables


def extract_deployed_tables(path_or_paths, dialect: str = "snowflake") -> tuple[list[dict[str, str]], dict]:
    """Extracts target table details and records file processing stats.

    Returns:
      tuple: (records, stats_dict)
    """
    script_paths = get_sql_files(path_or_paths)

    stats = {
        "total_files": len(script_paths),
        "successful_files": 0,
        "failed_files": [],
    }

    if not script_paths:
        logger.info("No .sql files found to process.")
        return [], stats

    records = []

    for path in script_paths:
        try:
            with open(path, "r", encoding="utf-8") as f:
                content = f.read()

            extracted_in_file = []

            # 1. Try AST parsing
            try:
                statements = sqlglot.parse(content, read=dialect, error_level=None)
                for statement in statements:
                    if statement:
                        extracted_in_file.extend(extract_target_tables_ast(statement))
            except Exception:
                pass

            # 2. Fallback to Tokenizer
            if not extracted_in_file and content.strip():
                extracted_in_file = extract_target_tables_tokens(content, dialect=dialect)

            for item in extracted_in_file:
                item["script_path"] = path
                records.append(item)

            # Record success
            stats["successful_files"] += 1

        except Exception as exc:
            logger.error(f"Failed to process SQL file {path}: {exc}")
            stats["failed_files"].append({"path": path, "error": str(exc)})

    return records, stats


def create_csv_report(records: list[dict[str, str]], report_file: str) -> None:
    """Writes Database, Schema, Table Name, FQN, and Script Path to CSV."""
    output_path = Path(report_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Database", "Schema", "Table_Name", "Fully_Qualified_Name", "Script_Path"])

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

    logger.info(f"CSV report created at: {output_path.resolve()}")


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
    results, stats = extract_deployed_tables(folder_path, dialect="snowflake")

    # Display File Stats Summary
    failed_count = len(stats["failed_files"])
    print("\n" + "=" * 80)
    print(" FILE PROCESSING SUMMARY")
    print("=" * 80)
    print(f" Total SQL Files Scanned : {stats['total_files']}")
    print(f" Successfully Processed  : {stats['successful_files']}")
    print(f" Failed Files            : {failed_count}")
    print("=" * 80)

    # Print details if any file failed
    if failed_count > 0:
        print("\n❌ FAILED FILES DETAILS:")
        print("-" * 80)
        for fail in stats["failed_files"]:
            print(f" File  : {fail['path']}")
            print(f" Error : {fail['error']}")
            print("-" * 80)

    # Display Table Results Summary
    if results:
        create_csv_report(results, csv_report_path)

        print(f"\nFound {len(results)} table DDL operations across SQL files:\n")
        print("-" * 80)
        for r in results:
            print(
                f"DB: {r['database'] or 'N/A':<15} | "
                f"Schema: {r['schema'] or 'N/A':<15} | "
                f"Table: {r['table_name']:<25} | "
                f"Script: {r['script_path']}"
            )
        print("-" * 80)
    else:
        print("\nNo matching DDL statements (CREATE/ALTER/DROP TABLE) were found.")
