"""Azure Function for Snowflake View Validation.

Refactored to address: SQL Security, Performance & Connections, Bug Fixes,
Maintainability, and Configuration Handling.
"""

from contextlib import contextmanager
import json
import logging
import os
from typing import Any, Dict, List, NamedTuple, Optional, Set, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import azure.functions as func

# Import existing utility functions from utility.py
from utility import (
    get_snowflake_connection,
    get_metameta_dict,
    get_entity_key_value
)

# -----------------------------------------------------------------------------
# Configuration & Constants
# -----------------------------------------------------------------------------
DEFAULT_METAMETA_NAME = "View_validation"
DEFAULT_PARALLEL_WORKERS = 10
EXCLUDED_SCHEMAS = {"INFORMATION_SCHEMA", "PUBLIC", "DEPLOY"}


# -----------------------------------------------------------------------------
# Maintainability: Strong Type Models
# -----------------------------------------------------------------------------
class ViewTarget(NamedTuple):
    """Represents a discovered Snowflake view target."""
    database: str
    schema: str
    name: str

    @property
    def fully_qualified_name(self) -> str:
        """Safely quotes and returns fully qualified identifier."""
        return f"{quote_identifier(self.database)}.{quote_identifier(self.schema)}.{quote_identifier(self.name)}"


class ValidationResult(NamedTuple):
    """Represents the health check output of a single view."""
    database: str
    schema: str
    view: str
    status: str  # "OK" or "FAILED"
    error: str

    def to_dict(self) -> Dict[str, str]:
        return {
            "database": self.database,
            "schema": self.schema,
            "view": self.view,
            "error": self.error,
        }


# -----------------------------------------------------------------------------
# SQL Security & Configuration Utilities
# -----------------------------------------------------------------------------
def quote_identifier(identifier: str) -> str:
    """Safely escapes internal double-quotes and quotes a Snowflake identifier."""
    escaped = str(identifier).replace('"', '""')
    return f'"{escaped}"'


def get_env_int(key: str, default: int) -> int:
    """Safely parses an integer environment variable with fallback."""
    val = os.environ.get(key)
    if val is None:
        return default
    try:
        return int(val)
    except ValueError:
        logging.warning(f"Invalid integer for env var '{key}': '{val}'. Falling back to default ({default}).")
        return default


def extract_param(req: func.HttpRequest, param_name: str) -> Optional[str]:
    """Safely reads a parameter from query params or JSON request body."""
    val = req.params.get(param_name)
    if val:
        return val

    try:
        req_body = req.get_json()
        if isinstance(req_body, dict):
            return req_body.get(param_name)
    except (ValueError, TypeError):
        pass

    return None


# -----------------------------------------------------------------------------
# Performance: Connection Context Manager
# -----------------------------------------------------------------------------
@contextmanager
def snowflake_connection():
    """Manages acquisition and cleanup of Snowflake cursor/connection resources."""
    cs, ctx = get_snowflake_connection()
    try:
        yield cs, ctx
    finally:
        try:
            cs.close()
        except Exception as exc:
            logging.debug(f"Error closing cursor: {exc}")
        try:
            ctx.close()
        except Exception as exc:
            logging.debug(f"Error closing connection context: {exc}")


# -----------------------------------------------------------------------------
# Core Discovery & Validation
# -----------------------------------------------------------------------------
def extract_all_db_schema_targets(metadata: Dict[str, Any]) -> Dict[str, Set[str]]:
    """Parses metameta dictionary and extracts DB -> Set[Schemas]."""
    db_schema_map: Dict[str, Set[str]] = {}
    entities = metadata.get("entities", [])

    if not isinstance(entities, list):
        entities = [entities]

    # Fallback to top-level metadata values if entities list is empty
    if not entities:
        top_db = get_entity_key_value("destination_database", None, metadata) or get_entity_key_value("source_database", None, metadata)
        top_schema = get_entity_key_value("destination_schema", None, metadata) or get_entity_key_value("default_source_schema", None, metadata) or ""

        if top_db and str(top_db).strip():
            db_key = str(top_db).strip().upper()
            schema_val = str(top_schema).strip().upper() if top_schema else ""
            db_schema_map.setdefault(db_key, set()).add(schema_val)

    for ent in entities:
        if not isinstance(ent, dict):
            continue

        db = get_entity_key_value("destination_database", ent, metadata) or get_entity_key_value("source_database", ent, metadata)
        if not db or not str(db).strip():
            continue

        db_key = str(db).strip().upper()
        schemas = (
            get_entity_key_value("destination_schemas", ent, metadata)
            or get_entity_key_value("destination_schema", ent, metadata)
            or get_entity_key_value("source_schema", ent, metadata)
        )

        target_set = db_schema_map.setdefault(db_key, set())

        # Bug Fix: Guard against None converting into string "NONE"
        if isinstance(schemas, list):
            for s in schemas:
                if s is not None and str(s).strip():
                    target_set.add(str(s).strip().upper())
        elif schemas is not None and str(schemas).strip():
            target_set.add(str(schemas).strip().upper())
        else:
            target_set.add("")

    return db_schema_map


def fetch_views_for_db(target_db: str, target_schemas: Set[str]) -> List[ViewTarget]:
    """Batch discovers views inside target_db."""
    logging.info(f"Fetching views for Snowflake database: '{target_db}'...")
    views_found: List[ViewTarget] = []

    try:
        with snowflake_connection() as (cs, _):
            safe_db = quote_identifier(target_db)
            cs.execute(f"USE DATABASE {safe_db}")
            cs.execute(f"SHOW VIEWS IN DATABASE {safe_db}")

            if not cs.description:
                return views_found

            columns = [col[0].lower() for col in cs.description]
            schema_idx = columns.index('schema_name') if 'schema_name' in columns else 1
            name_idx = columns.index('name') if 'name' in columns else 2

            for row in cs.fetchall():
                v_schema = str(row[schema_idx]).upper()
                v_name = str(row[name_idx]).upper()

                if v_schema in EXCLUDED_SCHEMAS:
                    continue

                if not target_schemas or "" in target_schemas or v_schema in target_schemas:
                    views_found.append(ViewTarget(database=target_db, schema=v_schema, name=v_name))

        logging.info(f"Discovered {len(views_found)} view(s) in Snowflake DB '{target_db}'.")
    except Exception as exc:
        logging.error(f"Failed to fetch views for DB '{target_db}': {exc}", exc_info=True)

    return views_found


def validate_single_view(target: ViewTarget) -> ValidationResult:
    """Executes 'SELECT * FROM <view> LIMIT 0' health check."""
    fq_name = target.fully_qualified_name

    try:
        with snowflake_connection() as (cs, _):
            cs.execute(f"SELECT * FROM {fq_name} LIMIT 0")
            logging.info(f"✅ OK - {fq_name}")
            return ValidationResult(
                database=target.database,
                schema=target.schema,
                view=target.name,
                status="OK",
                error=""
            )
    except Exception as exc:
        err_msg = str(exc).replace("\n", " ").replace("\r", " ")
        logging.error(f"❌ FAILED - {fq_name}: {err_msg}")
        return ValidationResult(
            database=target.database,
            schema=target.schema,
            view=target.name,
            status="FAILED",
            error=err_msg
        )


# -----------------------------------------------------------------------------
# Main Entrypoint
# -----------------------------------------------------------------------------
def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('psaValidationTrigger() - Python HTTP trigger function processed a request.')

    # Configuration Handling
    metameta_name = extract_param(req, 'metameta_name') or DEFAULT_METAMETA_NAME
    parallel_workers = get_env_int("PARALLEL_WORKERS", DEFAULT_PARALLEL_WORKERS)

    # Fetch Metadata
    try:
        metadata = get_metameta_dict(db_name=metameta_name)
        all_db_schema_map = extract_all_db_schema_targets(metadata)
    except Exception as exc:
        logging.error(f"Failed to fetch metameta file for '{metameta_name}': {exc}", exc_info=True)
        return func.HttpResponse(
            json.dumps({"status": "ERROR", "message": f"Failed to load metameta file '{metameta_name}': {str(exc)}"}),
            status_code=500,
            mimetype="application/json"
        )

    if not all_db_schema_map:
        return func.HttpResponse(
            json.dumps({"status": "ERROR", "message": f"No valid database targets found inside metameta file '{metameta_name}'."}),
            status_code=400,
            mimetype="application/json"
        )

    # Discover Views (Parallel)
    all_views: List[ViewTarget] = []
    db_workers = max(1, min(parallel_workers, len(all_db_schema_map)))

    with ThreadPoolExecutor(max_workers=db_workers) as executor:
        futures = {
            executor.submit(fetch_views_for_db, db, schemas): db
            for db, schemas in all_db_schema_map.items()
        }
        for future in as_completed(futures):
            try:
                all_views.extend(future.result())
            except Exception as exc:
                db_name = futures[future]
                logging.error(f"Error fetching views for database '{db_name}': {exc}", exc_info=True)

    if not all_views:
        return func.HttpResponse(
            json.dumps({
                "status": "SUCCESS",
                "message": f"No views discovered for metameta file '{metameta_name}'.",
                "summary": {"total_views": 0, "passed": 0, "failed": 0}
            }),
            status_code=200,
            mimetype="application/json"
        )

    # Validate Views (Parallel)
    results: List[ValidationResult] = []
    failed_list: List[Dict[str, str]] = []
    val_workers = max(1, min(parallel_workers, len(all_views)))

    with ThreadPoolExecutor(max_workers=val_workers) as executor:
        futures = {
            executor.submit(validate_single_view, view_target): view_target
            for view_target in all_views
        }
        for future in as_completed(futures):
            try:
                res = future.result()
                results.append(res)
                if res.status == "FAILED":
                    failed_list.append(res.to_dict())
            except Exception as exc:
                target = futures[future]
                logging.error(f"Unhandled validation exception for '{target.fully_qualified_name}': {exc}", exc_info=True)
                failed_res = ValidationResult(
                    database=target.database,
                    schema=target.schema,
                    view=target.name,
                    status="FAILED",
                    error=f"Uncaught thread exception: {str(exc)}"
                )
                results.append(failed_res)
                failed_list.append(failed_res.to_dict())

    # Construct Response
    is_success = len(failed_list) == 0
    response_payload = {
        "metameta_name": metameta_name,
        "status": "PASSED" if is_success else "FAILED",
        "summary": {
            "total_views": len(results),
            "passed": len(results) - len(failed_list),
            "failed": len(failed_list)
        },
        "failed_views": failed_list
    }

    return func.HttpResponse(
        json.dumps(response_payload, indent=2),
        status_code=200 if is_success else 422,
        mimetype="application/json"
    )
