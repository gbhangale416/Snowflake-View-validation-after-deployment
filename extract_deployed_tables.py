"""
Scans the DDL files that were part of this deployment (as listed in a
changed-files manifest) and extracts the names of tables that were
created, altered, or dropped. Writes the distinct list to
output/deployed_tables.txt for use by find_impacted_views.py.

Usage:
    python extract_deployed_tables.py <changed_files_list.txt>
"""

import os
import re
import sys

TABLE_PATTERNS = [
    re.compile(r'CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([\w."]+)', re.IGNORECASE),
    re.compile(r'ALTER\s+TABLE\s+([\w."]+)', re.IGNORECASE),
    re.compile(r'DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?([\w."]+)', re.IGNORECASE),
]


def extract_tables_from_file(path):
    with open(path, "r") as f:
        content = f.read()
    tables = set()
    for pattern in TABLE_PATTERNS:
        for m in pattern.finditer(content):
            name = m.group(1).strip('"')
            tables.add(name.upper())
    return tables


def main():
    if len(sys.argv) < 2:
        print("Usage: extract_deployed_tables.py <changed_files_list.txt>")
        sys.exit(2)

    changed_files_list = sys.argv[1]
    if not os.path.exists(changed_files_list):
        print(f"{changed_files_list} not found")
        sys.exit(2)

    with open(changed_files_list) as f:
        files = [line.strip() for line in f if line.strip()]

    all_tables = set()
    for fpath in files:
        if not fpath.endswith(".sql") or not os.path.exists(fpath):
            continue
        tables = extract_tables_from_file(fpath)
        if tables:
            print(f"{fpath}: {', '.join(sorted(tables))}")
        all_tables.update(tables)

    os.makedirs("output", exist_ok=True)
    with open("output/deployed_tables.txt", "w") as out:
        for t in sorted(all_tables):
            out.write(t + "\n")

    print(f"\nTotal distinct tables touched by this deployment: {len(all_tables)}")


if __name__ == "__main__":
    main()
