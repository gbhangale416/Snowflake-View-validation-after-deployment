import logging
import os
import re
import sys
from pathlib import Path

# Configure basic logging to see warnings/errors
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def get_sql_files(target_path) -> list[str]:
    """Resolves a file, folder, or list of paths into a list of valid .sql file paths."""
    sql_files = set()

    # Handle if target_path is a list/set of paths
    if isinstance(target_path, (list, tuple, set)):
        for p in target_path:
            sql_files.update(get_sql_files(p))
        return sorted(list(sql_files))

    path = Path(target_path)

    if not path.exists():
        logger.warning(f"Path does not exist: {target_path}")
        return []

    # If it's a single .sql file
    if path.is_file() and path.suffix.lower() == ".sql":
        sql_files.add(str(path.resolve()))

    # If it's a directory, recursively find all .sql files
    elif path.is_dir():
        for file in path.rglob("*.sql"):
            sql_files.add(str(file.resolve()))

    return sorted(list(sql_files))


def strip_comments(sql_code: str) -> str:
    """Removes single-line (-- ...) and multi-line (/* ... */) comments from SQL code."""
    sql_code = re.sub(r"--.*$", "", sql_code, flags=re.MULTILINE)
    sql_code = re.sub(r"/\*.*?\*/", "", sql_code, flags=re.DOTALL)
    return sql_code


TABLE_IDENTIFIER = r'(?:[a-zA-Z0-9_$".-]+|"[^"]+"+)'

TABLE_PATTERNS = [
    re.compile(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?"
        r"(?:(?:TRANSIENT|TEMPORARY|TEMP|VOLATILE|EXTERNAL|ICEBERG|HYBRID)\s+)?"
        r"TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"
        f"({TABLE_IDENTIFIER})",
        re.IGNORECASE,
    ),
    re.compile(
        r"ALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?" f"({TABLE_IDENTIFIER})",
        re.IGNORECASE,
    ),
    re.compile(
        r"DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?" f"({TABLE_IDENTIFIER})",
        re.IGNORECASE,
    ),
]


def extract_deployed_tables(path_or_paths):
    """Extracts table names touched by CREATE/ALTER/DROP TABLE statements.

    Accepts:
      - Folder path (str or Path) -> Scans all .sql files inside
      - Single file path (str or Path) -> Scans that single file
      - List of file/folder paths -> Scans all resolved .sql files

    Returns:
      dict: mapping table_name -> set of script paths
    """
    # 1. Automatically resolve local directory or list of paths into .sql file paths
    script_paths = get_sql_files(path_or_paths)

    if not script_paths:
        logger.info("No .sql files found to process.")
        return {}

    tables_to_scripts = {}

    # 2. Read and parse each .sql file
    for path in script_paths:
        try:
            with open(path, "r", encoding="utf-8") as f:
                content = f.read()

            clean_content = strip_comments(content)

            for pattern in TABLE_PATTERNS:
                for m in pattern.finditer(clean_content):
                    tbl_name = m.group(1).strip().rstrip(";").strip('"').upper()
                    if tbl_name:
                        tables_to_scripts.setdefault(tbl_name, set()).add(path)

        except Exception as exc:
            logger.error(f"Failed to read SQL file {path}: {exc}")

    return tables_to_scripts


# ==============================================================================
# EXECUTION
# ==============================================================================
if __name__ == "__main__":
    # Specify your SQL folder path here:
    folder_path = r"./path/to/your/sql_folder"

    # Alternatively, accept command-line arguments: python script.py /path/to/folder
    if len(sys.argv) > 1:
        folder_path = sys.argv[1]

    print(f"\nScanning directory: {folder_path} ...\n")
    results = extract_deployed_tables(folder_path)

    # Display results
    if results:
        print(f"Found {len(results)} tables touched across SQL files:\n")
        print("-" * 60)
        for table_name, file_paths in results.items():
            print(f"Table: {table_name}")
            for file_path in file_paths:
                print(f"  └── {file_path}")
        print("-" * 60)
    else:
        print("No matching SQL statements (CREATE/ALTER/DROP TABLE) were found.")
