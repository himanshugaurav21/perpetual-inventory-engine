"""Dual-mode auth: CLI profile (local dev) or service principal (deployed)."""

import os
from databricks.sdk import WorkspaceClient

IS_DATABRICKS_APP = bool(os.environ.get("DATABRICKS_APP_NAME"))

_workspace_client: WorkspaceClient | None = None


def get_workspace_client() -> WorkspaceClient:
    global _workspace_client
    if _workspace_client is None:
        if IS_DATABRICKS_APP:
            _workspace_client = WorkspaceClient()
        else:
            profile = os.environ.get("DATABRICKS_PROFILE", "DEFAULT")
            _workspace_client = WorkspaceClient(profile=profile)
    return _workspace_client


def get_workspace_host() -> str:
    if IS_DATABRICKS_APP:
        host = os.environ.get("DATABRICKS_HOST", "")
        if host and not host.startswith("http"):
            host = f"https://{host}"
        return host
    return get_workspace_client().config.host


def get_oauth_token() -> str:
    w = get_workspace_client()
    if w.config.token:
        return w.config.token
    auth_headers = w.config.authenticate()
    if auth_headers and "Authorization" in auth_headers:
        return auth_headers["Authorization"].replace("Bearer ", "")
    raise RuntimeError("Unable to obtain Databricks auth token")


def get_warehouse_id() -> str:
    wh_id = (
        os.environ.get("DATABRICKS_WAREHOUSE_ID", "") or
        os.environ.get("SQL_WAREHOUSE_ID", "") or
        os.environ.get("sql_warehouse_ID", "") or
        os.environ.get("SQL_WAREHOUSE_WAREHOUSE_ID", "")
    )
    if not wh_id:
        raise RuntimeError("DATABRICKS_WAREHOUSE_ID not set.")
    return wh_id


def get_catalog() -> str:
    return os.environ.get("PI_CATALOG", "perpetual_inventory_engine")


def refresh_databricks_token():
    if IS_DATABRICKS_APP:
        os.environ["DATABRICKS_TOKEN"] = get_oauth_token()
