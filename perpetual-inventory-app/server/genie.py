"""Databricks Genie API client for natural language inventory queries."""

import json
import os
import time
from server.config import get_workspace_host, get_oauth_token, get_warehouse_id


GENIE_SPACE_ID = os.environ.get("GENIE_SPACE_ID", "")


def is_genie_configured() -> bool:
    return bool(GENIE_SPACE_ID)


def ask_genie(question: str, timeout_seconds: int = 90) -> dict:
    """Start a Genie conversation and poll for results."""
    if not GENIE_SPACE_ID:
        return {"error": "Genie Space ID not configured."}

    host = get_workspace_host()
    token = get_oauth_token()

    import urllib.request

    def _api(method, path, body=None):
        url = f"{host}{path}"
        data = json.dumps(body).encode() if body else None
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("Authorization", f"Bearer {token}")
        req.add_header("Content-Type", "application/json")
        with urllib.request.urlopen(req, timeout=60) as resp:
            return json.loads(resp.read())

    try:
        # Start conversation via the correct endpoint
        conv = _api("POST", f"/api/2.0/genie/spaces/{GENIE_SPACE_ID}/start-conversation",
                     {"content": question})

        conversation_id = conv.get("conversation_id", "")
        message_id = conv.get("message_id", "")

        # Also check nested message object
        msg_obj = conv.get("message", {})
        if not conversation_id:
            conversation_id = msg_obj.get("conversation_id", "")
        if not message_id:
            message_id = msg_obj.get("id", msg_obj.get("message_id", ""))

        if not conversation_id or not message_id:
            return {"error": "Failed to start Genie conversation", "raw": str(conv)[:300]}

        # Poll for result
        t_start = time.time()
        while time.time() - t_start < timeout_seconds:
            time.sleep(3)
            result = _api("GET",
                          f"/api/2.0/genie/spaces/{GENIE_SPACE_ID}/conversations/{conversation_id}/messages/{message_id}")

            status = result.get("status", "")

            if status == "COMPLETED":
                return _parse_genie_result(result, question, conversation_id)
            elif status in ("FAILED", "CANCELLED"):
                # Check for error details
                error_msg = "Genie query failed"
                for att in result.get("attachments", []):
                    if att.get("query", {}).get("error"):
                        error_msg = att["query"]["error"]
                return {"error": error_msg, "status": status, "source": "genie"}

        return {"error": "Genie query timed out after " + str(timeout_seconds) + "s",
                "conversation_id": conversation_id, "source": "genie"}

    except Exception as e:
        return {"error": f"Genie API error: {str(e)}", "source": "genie"}


def ask_genie_followup(conversation_id: str, question: str, timeout_seconds: int = 90) -> dict:
    """Send a follow-up question to an existing Genie conversation."""
    if not GENIE_SPACE_ID:
        return {"error": "Genie Space ID not configured."}

    host = get_workspace_host()
    token = get_oauth_token()

    import urllib.request

    def _api(method, path, body=None):
        url = f"{host}{path}"
        data = json.dumps(body).encode() if body else None
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("Authorization", f"Bearer {token}")
        req.add_header("Content-Type", "application/json")
        with urllib.request.urlopen(req, timeout=60) as resp:
            return json.loads(resp.read())

    try:
        msg = _api("POST",
                    f"/api/2.0/genie/spaces/{GENIE_SPACE_ID}/conversations/{conversation_id}/messages",
                    {"content": question})

        message_id = msg.get("message_id", msg.get("id", ""))
        msg_obj = msg.get("message", {})
        if not message_id:
            message_id = msg_obj.get("id", msg_obj.get("message_id", ""))

        if not message_id:
            return {"error": "Failed to send follow-up message"}

        # Poll
        t_start = time.time()
        while time.time() - t_start < timeout_seconds:
            time.sleep(3)
            result = _api("GET",
                          f"/api/2.0/genie/spaces/{GENIE_SPACE_ID}/conversations/{conversation_id}/messages/{message_id}")
            status = result.get("status", "")
            if status == "COMPLETED":
                return _parse_genie_result(result, question, conversation_id)
            elif status in ("FAILED", "CANCELLED"):
                return {"error": "Follow-up query failed", "source": "genie"}

        return {"error": "Follow-up timed out", "source": "genie"}

    except Exception as e:
        return {"error": f"Genie follow-up error: {str(e)}", "source": "genie"}


def _parse_genie_result(result: dict, question: str, conversation_id: str) -> dict:
    """Parse Genie response into structured result."""
    sql = ""
    description = ""
    row_count = 0
    statement_id = ""

    for att in result.get("attachments", []):
        query = att.get("query", {})
        if query.get("query"):
            sql = query["query"]
            description = query.get("description", "")
            row_count = query.get("query_result_metadata", {}).get("row_count", 0)
            statement_id = query.get("statement_id", "")

    # Fetch actual query results if we have a statement_id
    rows = []
    if statement_id:
        try:
            host = get_workspace_host()
            token = get_oauth_token()
            import urllib.request
            req = urllib.request.Request(
                f"{host}/api/2.0/sql/statements/{statement_id}",
                method="GET")
            req.add_header("Authorization", f"Bearer {token}")
            with urllib.request.urlopen(req, timeout=30) as resp:
                stmt_result = json.loads(resp.read())

            if stmt_result.get("result") and stmt_result.get("manifest"):
                columns = [c.get("name", "") for c in stmt_result["manifest"]["schema"]["columns"]]
                for row_data in stmt_result["result"].get("data_array", [])[:25]:
                    rows.append(dict(zip(columns, row_data)))
        except Exception:
            pass  # Results may not be available via statement API

    return {
        "question": question,
        "sql": sql,
        "description": description,
        "results": rows,
        "row_count": row_count,
        "conversation_id": conversation_id,
        "source": "genie",
    }
