# Perpetual Inventory (PI) Accuracy & Autonomous Correction Engine

An AI-driven Perpetual Inventory Accuracy Engine built on Databricks that continuously monitors inventory health, detects anomalies, provides explainable AI reasoning, and routes flagged items to store teams for validation.

## The Problem

Retailers operate with approximately **59% PI accuracy**. "Ghost inventory" -- where the system shows stock that doesn't physically exist -- causes failed fulfillment and lost revenue. Traditional cycle counts are expensive, infrequent, and reactive.

This engine takes a proactive approach: continuously scoring every SKU-store pair across five anomaly signals, surfacing the highest-risk items with AI-generated explanations, and routing them to store teams through a human-in-the-loop validation workflow.

## Architecture Overview

```
Bronze (Raw)          Silver (Enriched)           Gold (Scores)            Serving (Operational)
─────────────         ──────────────────          ─────────────            ────────────────────
sku_master       ──►  inventory_velocity     ──►  sku_risk_scores     ──►  at_risk_skus
store_master          adjustment_patterns         store_health             store_validations
inventory_ledger      stock_movements             anomaly_summary          correction_history
shipment_events                                        │
sales_transactions                                     ▼
store_adjustments                               Vector Search Index
                                                       │
                                                       ▼
                                                ┌──────────────┐
                                                │ Databricks   │
                                                │ App (FastAPI │
                                                │ + React)     │
                                                └──────┬───────┘
                                                       │
                                          ┌────────────┼────────────┐
                                          ▼            ▼            ▼
                                     Claude        Genie       MLflow
                                     Sonnet 4.5   Space       Tracing
```

### Databricks Components Used

| Component | Purpose |
|-----------|---------|
| **Unity Catalog** | 4-schema medallion architecture (bronze, silver, gold, serving) |
| **Lakeflow (DLT)** | Serverless pipeline: Bronze ingestion, Silver enrichment, Gold scoring |
| **Delta Serving Tables** | Liquid clustered operational tables with TTL caching |
| **Foundation Model API** | Claude Sonnet 4.5 for explainable anomaly reasoning |
| **Vector Search** | Historical anomaly pattern matching for context |
| **Genie Space** | Natural language queries over inventory data |
| **Databricks Apps** | Full-stack FastAPI + React application |
| **MLflow Tracing** | End-to-end observability of the AI agent pipeline |

## Project Structure

```
perpetual-inventory-engine/
├── config.env.template                  # Config template for local shell deployment
├── generate_data.py                     # Local data generation (Polars + NumPy + Mimesis)
├── pipeline_notebook.py                 # DLT pipeline (Bronze -> Silver -> Gold)
├── deploy_from_workspace.py             # Single-notebook deployment (all 9 phases)
├── scripts/
│   ├── deploy_all.sh                    # Master orchestrator (local CLI)
│   ├── 01_setup_catalog.sh              # Phase 1: Catalog & schemas
│   ├── 02_generate_data.sh              # Phase 2: Data generation
│   ├── 03_deploy_pipeline.sh            # Phase 3: DLT pipeline
│   ├── 04_setup_security.sh             # Phase 4: Tags & column masks
│   ├── 05_setup_serving.py              # Phase 5: Serving tables
│   ├── 06_setup_vector_search.sh        # Phase 6: Vector Search
│   ├── 07_setup_genie.py                # Phase 7: Genie Space
│   └── 08_deploy_app.sh                 # Phase 8: Databricks App
└── perpetual-inventory-app/             # Databricks App source
    ├── app.yaml                         # App config (env vars injected at deploy time)
    ├── app.py                           # FastAPI entry point
    ├── requirements.txt
    ├── server/
    │   ├── config.py                    # Dual-mode auth (CLI profile / service principal)
    │   ├── warehouse.py                 # SQL queries + 30s TTL cache
    │   ├── llm.py                       # Foundation Model API client
    │   ├── vector_search.py             # VS index query client
    │   ├── agent.py                     # 4-step AI anomaly agent with MLflow tracing
    │   ├── genie.py                     # Genie Space API client
    │   └── routes/
    │       ├── dashboard.py             # GET /api/dashboard/summary
    │       ├── anomalies.py             # GET /api/anomalies, GET /api/anomalies/{sku}/{store}
    │       ├── agent_route.py           # POST /api/anomalies/{sku}/{store}/analyze
    │       ├── validations.py           # POST /api/validations, GET /api/validations/queue/{store}
    │       ├── stores.py                # GET /api/stores/health
    │       └── analytics.py             # GET /api/analytics/trends
    └── frontend/
        ├── package.json
        ├── vite.config.ts
        ├── index.html
        └── src/
            ├── App.tsx                  # Router + sidebar with FreshMart branding
            ├── index.css                # Teal/emerald retail color palette
            ├── pages/                   # Dashboard, AtRiskInventory, StoreValidation, AIAnalysis, Analytics
            ├── components/              # StatCard, RiskBadge, SignalPanel
            └── hooks/                   # useApi data fetching hook
```

## Deployment

The codebase supports two deployment modes. Both produce identical results.

### Prerequisites

- A Databricks workspace (any cloud: AWS, Azure, or GCP)
- A Serverless SQL Warehouse
- A Foundation Model endpoint (e.g., `databricks-claude-sonnet-4-5`)

### Mode 1: Single Notebook (Recommended for Demos)

**Requires:** Nothing beyond a Databricks workspace. No local tools needed.

1. Clone this repo into your workspace via **Git Folders**:
   - Workspace > Git Folders > Add Git Folder > paste the repo URL

2. Open `deploy_from_workspace.py` as a notebook

3. Configure the widgets at the top:
   - `catalog`: Catalog name (default: `perpetual_inventory_engine`)
   - `warehouse_id`: Your SQL Warehouse ID (required -- find it on the SQL Warehouses page)
   - `serving_endpoint`: Foundation Model endpoint name (default: `databricks-claude-sonnet-4-5`)
   - `app_name`: App name (default: `perpetual-inventory-app`)

4. Run All cells. The notebook orchestrates all 9 phases automatically:

   | Phase | Description | Method |
   |-------|-------------|--------|
   | 1 | Catalog & Schema Setup | SQL API |
   | 2 | Synthetic Data Generation (885K rows) | PySpark |
   | 3 | DLT Pipeline (Bronze -> Silver -> Gold) | Pipelines API |
   | 4 | Security (Tags & Column Masks) | SQL API |
   | 5 | Serving Tables (Liquid Clustered) | SparkSQL |
   | 6 | Vector Search Endpoint & Index | VS API |
   | 7 | Genie Space | REST API |
   | 8 | App Creation & Deployment | Apps API |
   | 9 | Post-Deploy (SP Permissions, MLflow) | REST API + SparkSQL |

5. After completion, the notebook prints a summary with all resource URLs including the app URL.

**Key patterns:** Auth is automatic via notebook execution context. Host and user are auto-detected. All operations are idempotent (safe to re-run).

### Mode 2: Local Shell Scripts

**Requires:** Databricks CLI, Node.js (18+), Python 3.10+, [uv](https://github.com/astral-sh/uv)

1. Copy and configure environment:
   ```bash
   cp config.env.template config.env
   # Edit config.env with your workspace details:
   #   DATABRICKS_HOST, DATABRICKS_PROFILE, DATABRICKS_USER,
   #   DATABRICKS_WAREHOUSE_ID
   ```

2. Authenticate:
   ```bash
   source config.env
   databricks auth login --host $DATABRICKS_HOST --profile $DATABRICKS_PROFILE
   ```

3. Run the full deployment:
   ```bash
   bash scripts/deploy_all.sh
   ```

   Or run individual phases:
   ```bash
   bash scripts/01_setup_catalog.sh
   bash scripts/02_generate_data.sh
   bash scripts/03_deploy_pipeline.sh
   # ... etc
   ```

4. Post-deployment: add resources to the app (SQL Warehouse, Serving Endpoint) and grant the app's service principal access to the catalog.

## Resource Naming

| Resource | Name |
|----------|------|
| Catalog | `perpetual_inventory_engine` |
| Schemas | `bronze`, `silver`, `gold`, `serving` |
| Volume | `perpetual_inventory_engine.bronze.source_files` |
| DLT Pipeline | `perpetual-inventory-pipeline` |
| Databricks App | `perpetual-inventory-app` |
| VS Endpoint | `perpetual-inventory-vs-endpoint` |
| VS Index | `perpetual_inventory_engine.gold.anomaly_summary_vs_index` |
| Genie Space | `Inventory Accuracy Intelligence` |

## Data Model

### Synthetic Datasets (Phase 2)

| Table | Rows | Description |
|-------|------|-------------|
| `sku_master` | 5,000 | SKUs across 7 categories (Grocery, Electronics, Apparel, Home, Health & Beauty, Toys, Sports) |
| `store_master` | 50 | FreshMart stores (superstore / neighborhood / express) across 5 regions |
| `inventory_ledger` | 250,000 | System quantity, last counted quantity, PI variance per SKU-store |
| `shipment_events` | 50,000 | Inbound shipments with shipped vs. received quantities |
| `sales_transactions` | 500,000 | POS and ecommerce transactions with scanned SKU tracking |
| `store_adjustments` | 30,000 | Manual corrections, cycle counts, damage/theft write-offs |

### Injected Anomalies (~10% of SKU-store pairs)

| Anomaly Type | Share | Pattern |
|--------------|-------|---------|
| Ghost Inventory | 35% | System shows 50-200 units, zero sales for 30+ days |
| Systematic Inflation | 20% | 3-8 upward adjustments without corresponding shipments, same employee |
| POS Scanning Errors | 15% | Scanned SKU differs from actual SKU (high-value as low-value) |
| Shrinkage Spikes | 15% | Sudden large negative adjustments (20-100 units) clustered at one store |
| Velocity Mismatch | 15% | Sales exceed what's physically possible given shipments + starting inventory |

### DLT Pipeline Layers

**Silver (Enriched):**
- `inventory_velocity` -- Sales velocity (7d/30d/90d), days of supply, zero-velocity flags
- `adjustment_patterns` -- Adjustment frequency, upward-without-shipment flags, supervisor approval rates
- `stock_movements` -- Reconciled stock flow: calculated on-hand vs. system quantity, unexplained gains/losses

**Gold (Scores & Summaries):**
- `sku_risk_scores` -- Composite anomaly score per SKU-store with 5 component signals
- `store_health` -- Store-level PI accuracy, ghost inventory value, shrinkage estimates
- `anomaly_summary` -- High-risk items (CRITICAL + HIGH) with explanations and recommended actions

### Composite Risk Scoring

Five signals, each scored 0.0-1.0:

| Signal | Weight | What It Measures |
|--------|--------|------------------|
| Velocity Score | 25% | Zero/low sales velocity with positive system inventory |
| Stock Consistency Score | 25% | Mismatch between calculated on-hand and reported system quantity |
| Adjustment Score | 20% | Upward corrections without corresponding shipments |
| Shrinkage Score | 20% | Unexplained inventory loss rate |
| Shipment Gap Score | 10% | Overdue replenishment with positive system inventory |

**Risk Tiers:** CRITICAL (>= 0.75), HIGH (>= 0.50), MEDIUM (>= 0.30), LOW (< 0.30)

## Application Pages

### 1. Dashboard (`/`)
The landing page showing overall inventory health. Hero card with fleet-wide PI Accuracy %, KPI stat cards (Total Monitored, Critical/High counts, Financial Exposure), risk distribution pie chart, and anomaly count by category bar chart.

### 2. At-Risk Inventory (`/at-risk`)
Filterable investigation list of all CRITICAL and HIGH tier SKU-store pairs. Each anomaly card shows the risk badge, SKU/store name, anomaly type, financial impact, and composite score. Click to expand full details including explanation text and recommended action.

### 3. Store Validation (`/validation`)
Human-in-the-loop workflow for store teams. Select a store to load its queue of open anomalies. For each item, workers can **Confirm** (ghost inventory verified), **Dismiss** (system was wrong, inventory exists), or **Investigate** (needs further review). Optional physical count and notes fields write back to the `serving.store_validations` table.

### 4. AI Analysis (`/analysis`)
Deep-dive on any single SKU-store pair using the 4-step AI agent pipeline:
1. **Data Validation** -- checks data freshness and completeness
2. **Signal Extraction** -- computes 5 component scores
3. **Composite Risk Scoring** -- weighted combination with confidence
4. **LLM Reasoning** -- Claude Sonnet 4.5 generates explanation, root cause hypothesis, and suggested action

Results include signal breakdown bars, AI explanation, similar historical patterns from Vector Search, and latency breakdown. All steps are traced with MLflow.

### 5. Analytics (`/analytics`)
Trend analysis and store-level operational intelligence. Anomaly type distribution, department PI accuracy comparison, store health heatmap (50-tile grid, color-coded by accuracy), and store rankings table sorted by PI accuracy.

### Operational Workflow

```
Dashboard (spot problems)
  -> At-Risk Inventory (investigate which SKUs)
    -> AI Analysis (deep-dive on specific SKU-store)
      -> Store Validation (send to store team for physical count)
        -> Analytics (track improvement over time)
```

## API Reference

| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/api/health` | Health check |
| `GET` | `/api/dashboard/summary` | KPIs, distribution charts, category breakdown |
| `GET` | `/api/anomalies` | Paginated list with filters (tier, category, store, type) |
| `GET` | `/api/anomalies/{sku_id}/{store_id}` | Full anomaly detail with all signals |
| `POST` | `/api/anomalies/{sku_id}/{store_id}/analyze` | Trigger AI agent pipeline |
| `POST` | `/api/validations` | Submit store validation (confirm / dismiss / investigate) |
| `GET` | `/api/validations/queue/{store_id}` | Pending validation items for a store |
| `GET` | `/api/stores/health` | Store health overview with PI accuracy |
| `GET` | `/api/analytics/trends` | Anomaly type trends, department accuracy, store rankings |

## Technical Details

### App Authentication
The app uses dual-mode authentication (`server/config.py`):
- **Deployed (Databricks Apps):** Automatic service principal OAuth via the Databricks SDK
- **Local development:** Databricks CLI profile authentication

### Caching
All SQL warehouse queries use a 30-second TTL in-memory cache (`server/warehouse.py`) to avoid redundant queries while keeping data reasonably fresh.

### AI Agent with MLflow Tracing
The 4-step agent pipeline (`server/agent.py`) is fully traced with MLflow:
```
pi_anomaly_analysis_pipeline (root span)
├── data_lookup
├── step1_data_validation
├── step2_signal_extraction
├── step3_composite_risk_scoring
├── vector_search_similar
└── step4_llm_reasoning (SpanType.LLM)
```
Includes a rule-based fallback if the LLM endpoint is unavailable.

### Frontend Stack
React 18 + TypeScript + Tailwind CSS + Recharts, built with Vite. Branded as "FreshMart Inventory Intelligence" with a teal/emerald retail color palette. The `frontend/dist/` directory is not committed to git -- it is built at deploy time by `scripts/08_deploy_app.sh`.

## Cleanup

To remove all resources created by this engine:

```sql
-- Drop the catalog (removes all schemas, tables, and volumes)
DROP CATALOG IF EXISTS perpetual_inventory_engine CASCADE;
```

Then manually delete:
- DLT Pipeline: `perpetual-inventory-pipeline`
- Databricks App: `perpetual-inventory-app`
- Vector Search Endpoint: `perpetual-inventory-vs-endpoint`
- Genie Space: `Inventory Accuracy Intelligence`
- MLflow Experiment: `/Users/<your-email>/perpetual-inventory-engine/pi-agent`
