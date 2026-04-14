"""Vector Search client for historical anomaly pattern matching."""

import json
import os
from server.config import get_workspace_host, get_oauth_token, get_catalog


def find_similar_anomalies(query_text: str, num_results: int = 5) -> list[dict]:
    cat = get_catalog()
    index_name = os.environ.get(
        "VS_INDEX_NAME", f"{cat}.gold.anomaly_summary_vs_index"
    )
    host = get_workspace_host()
    token = get_oauth_token()

    import urllib.request

    url = f"{host}/api/2.0/vector-search/indexes/{index_name}/query"
    payload = json.dumps({
        "query_text": query_text,
        "columns": [
            "anomaly_id", "store_name", "sku_name", "category",
            "risk_tier", "primary_anomaly_type", "composite_risk_score",
            "financial_impact", "explanation_text"
        ],
        "num_results": num_results,
    }).encode()

    req = urllib.request.Request(url, data=payload, method="POST")
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Content-Type", "application/json")

    try:
        with urllib.request.urlopen(req) as resp:
            result = json.loads(resp.read())

        manifest = result.get("manifest", {}).get("columns", [])
        col_names = [c.get("name", "") for c in manifest]
        data = result.get("result", {}).get("data_array", [])

        return [dict(zip(col_names, row)) for row in data]
    except Exception as e:
        print(f"Vector Search error: {e}")
        return []
