"""Foundation Model API client for inventory anomaly reasoning."""

import json
import os
import time
from server.config import get_workspace_host, get_oauth_token

_last_metrics: dict = {}


def get_last_llm_metrics() -> dict:
    return _last_metrics


def chat_completion(messages: list[dict], max_tokens: int = 768, temperature: float = 0.2) -> str:
    global _last_metrics
    endpoint = os.environ.get("SERVING_ENDPOINT", "databricks-claude-sonnet-4-5")
    host = get_workspace_host()
    token = get_oauth_token()

    import urllib.request

    url = f"{host}/serving-endpoints/{endpoint}/invocations"
    payload = json.dumps({
        "messages": messages,
        "max_tokens": max_tokens,
        "temperature": temperature,
    }).encode()

    req = urllib.request.Request(url, data=payload, method="POST")
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Content-Type", "application/json")

    with urllib.request.urlopen(req) as resp:
        result = json.loads(resp.read())

    content = result.get("choices", [{}])[0].get("message", {}).get("content", "")
    usage = result.get("usage", {})
    _last_metrics = {
        "input_tokens": usage.get("prompt_tokens", 0),
        "output_tokens": usage.get("completion_tokens", 0),
        "total_tokens": usage.get("total_tokens", 0),
    }
    return content
