# Perpetual Inventory (PI) Accuracy & Autonomous Correction Engine

An AI-driven Perpetual Inventory Accuracy Engine built on Databricks that continuously monitors inventory health, detects anomalies, provides explainable AI reasoning, and routes flagged items to store teams for validation.

## The Problem

Retailers operate with approximately **59% PI accuracy**. Every item in a store has a "perpetual inventory" record -- the system's belief of how many units are on the shelf. When that number is wrong, the consequences cascade:

- **Ghost inventory**: The system says 50 units are in stock, but the shelf is empty. Customers see "available" online, place an order, and the retailer can't fulfill it. BOPIS (buy-online-pickup-in-store) fails. Delivery promises break. Revenue is lost, and the customer doesn't come back.
- **Overstocking from phantom demand**: Replenishment systems trust the PI number. If the system thinks stock is low (but it's actually fine), it triggers unnecessary reorders -- tying up capital and shelf space.
- **Shrinkage blindness**: When theft, damage, or process errors steadily drain inventory, inaccurate PI masks the problem until a physical count reveals a massive write-off.
- **Failed promotions**: A retailer plans a weekend promotion for 500 stores. The system says all stores are stocked. In reality, 80 stores have ghost inventory on the promoted SKU -- the promotion fails silently.

Traditional approaches rely on **periodic physical counts** (cycle counts): teams walk the store with scanners, counting every item. This is expensive ($0.03-0.10 per unit counted), infrequent (most SKUs are counted once or twice a year), and purely reactive -- by the time you count, the damage is done.

https://github.com/user-attachments/assets/5a6471cd-6e4e-4336-a770-e88e0232e0c5


## What This Engine Does

This engine replaces reactive counting with **continuous, AI-driven monitoring**. Rather than waiting for a human to discover the problem, the system watches every SKU in every store across five independent anomaly signals, computes a composite risk score, and surfaces the highest-risk items with AI-generated explanations.

The approach:

1. **Ingest all available signals** -- sales transactions, shipment receipts, manual adjustments, and the inventory ledger itself. No new hardware or data collection is needed; retailers already have this data.

2. **Reconcile what should be there vs. what the system says** -- For every SKU-store pair, calculate what the on-hand quantity *should* be based on the last physical count plus received shipments minus sales minus adjustments. Compare that to what the system currently reports. Discrepancies surface immediately.

3. **Score anomalies across five dimensions** -- A single discrepancy number isn't enough. The engine looks at velocity patterns (is the item selling?), stock consistency (does the math add up?), adjustment patterns (are corrections suspicious?), shrinkage indicators (is inventory disappearing?), and shipment gaps (should a reorder have happened?). Each dimension produces a 0-1 score, and a weighted composite determines the risk tier.

4. **Explain why, not just what** -- Every flagged item comes with a human-readable explanation built from the signals: *"Zero/low velocity: no sales for 87 days with 51 units on hand; Suspicious adjustments: 8 upward adjustments without matching shipments."* When a store team sees this, they know exactly what to investigate.

5. **AI-powered deep analysis** -- For any individual SKU-store pair, a 4-step AI agent pipeline (powered by Claude Sonnet 4.5) validates the data, extracts signals, computes confidence, and generates a root cause hypothesis with a recommended action. Vector Search retrieves similar historical anomalies for context.

6. **Human-in-the-loop validation** -- Flagged items are routed to store teams who can confirm (ghost inventory is real), dismiss (system was wrong, inventory exists), or mark for investigation. This feedback loop improves the system over time and ensures corrections are grounded in physical reality.

7. **Track and measure** -- Analytics show improvement over time: which stores are getting more accurate, which anomaly types are being resolved, and where the biggest financial exposure remains.

### The Result

Instead of counting 250,000 SKU-store pairs annually, the engine directs store teams to the ~2,000 CRITICAL items and ~48,000 HIGH-risk items that actually need attention -- a 20x reduction in wasted effort with a direct line to the items most likely to cause fulfillment failures and financial loss.

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

## Data Flow: From Raw Signals to Actionable Intelligence

The engine processes six operational datasets through a medallion architecture (Bronze -> Silver -> Gold -> Serving) to transform raw transactional data into scored, explained anomalies ready for human action.

### Input Datasets (Bronze Layer)

These six tables represent the data a retailer already generates through daily operations. No new instrumentation is required.

**`sku_master`** (5,000 rows) -- The product catalog. Every item the retailer sells: SKU ID, product name, category (Grocery, Electronics, Apparel, Home, Health & Beauty, Toys, Sports), department, unit cost, retail price, supplier, and UPC barcode. This is the reference dimension that tells the engine *what* the item is and *what it's worth*.

**`store_master`** (50 rows) -- The store fleet. Each store has a type (superstore, neighborhood, express), a region (Northeast, Southeast, Midwest, West, Southwest), and a shrinkage profile (high/medium/low). Store type matters because a superstore carries 3x the inventory depth of an express location, so "normal" velocity differs dramatically.

**`inventory_ledger`** (250,000 rows) -- The perpetual inventory itself -- one row per SKU-store pair. This is the core table the engine is trying to validate. It contains the `system_quantity` (what the system *believes* is on the shelf), `last_counted_quantity` (what was physically counted), `last_count_date`, `reorder_point`, and `pi_variance`. The gap between `system_quantity` and reality is exactly what this engine measures.

**`sales_transactions`** (500,000 rows) -- POS and e-commerce sales over 90 days. Each transaction records the `sku_id`, `scanned_sku_id` (what the cashier actually scanned -- these differ during POS scanning errors), `quantity_sold`, `sale_date`, `channel` (pos/ecomm), and `cashier_id`. Sales are the primary "outflow" signal: if an item is selling, inventory should be going down. If it's *not* selling but the system says stock is high, that's a ghost inventory signal.

**`shipment_events`** (50,000 rows) -- Inbound shipments from vendors, inter-store transfers, and returns. Each event records `quantity_shipped` vs. `quantity_received` (to detect receiving discrepancies), the shipment date, and the DC/warehouse of origin. Shipments are the primary "inflow" signal: if an item hasn't been restocked in months but the system says there's plenty of inventory, something doesn't add up.

**`store_adjustments`** (30,000 rows) -- Manual inventory corrections made by store employees. These include cycle count results, damage write-offs, theft acknowledgments, and ad-hoc corrections. Each record includes who made the adjustment (`adjusted_by`), the reason code, and whether a supervisor approved it. Adjustments are a double-edged signal: legitimate corrections improve accuracy, but suspicious patterns (repeated upward adjustments by the same employee without corresponding shipments) indicate systematic inflation.

### Enrichment Layer (Silver)

The Silver layer joins these raw inputs and computes derived metrics that the scoring engine needs. Three enriched tables are produced:

**`silver_inventory_velocity`** -- For every SKU-store pair, calculates sales velocity at three time horizons (7-day, 30-day, 90-day), the velocity trend (is it accelerating or decelerating?), days of supply remaining, days since the last sale, and a `zero_velocity_flag` (no sales for 30+ days while the system shows positive inventory). This table answers: *Is this item actually moving, or is it just sitting there?*

**`silver_adjustment_patterns`** -- Aggregates all adjustments per SKU-store: total count, positive vs. negative splits, net adjustment quantity, frequency per week, how many distinct employees made changes, what percentage were supervisor-approved, and critically, an `upward_without_shipment_flag` -- true when the store made 2+ positive corrections (adding inventory) without any shipment arriving within 3 days. This is the clearest signal of systematic inflation: someone is manually inflating the count without any physical goods arriving.

**`silver_stock_movements`** -- The reconciliation engine. Starting from the `last_counted_quantity`, it adds all received shipments, subtracts all sales, and adds net adjustments to compute a `calculated_on_hand`. It then compares this to the `system_quantity` to produce the `stock_discrepancy` -- the gap between what *should* be there and what the system *says* is there. Positive discrepancies are `unexplained_gain` (the system shows more than the math says it should); negative discrepancies are `unexplained_loss` (inventory has disappeared).

### Scoring Layer (Gold)

The Gold layer combines all Silver outputs into actionable scores and summaries.

**`gold_sku_risk_scores`** -- The core scoring table. For each of the 250,000 SKU-store pairs, it computes five component scores (each 0.0 to 1.0) and combines them into a single composite risk score:

| Score | Weight | Inputs | What It Catches |
|-------|--------|--------|-----------------|
| **Velocity** | 25% | `zero_velocity_flag`, `days_since_last_sale`, `system_quantity`, `velocity_trend` | Ghost inventory: system says we have it, but nobody's buying it. If zero sales for 30+ days with 50+ units showing, score = 1.0. |
| **Stock Consistency** | 25% | `discrepancy_pct` (calculated on-hand vs. system quantity) | Calculation mismatch: the math of "starting count + shipments - sales + adjustments" doesn't match what the system says. >50% discrepancy = 1.0. |
| **Adjustment** | 20% | `upward_without_shipment_flag`, `adjustment_frequency_per_week`, `pct_supervisor_approved`, `distinct_adjusters` | Systematic inflation: repeated upward corrections without goods arriving, especially by a single employee without supervisor sign-off. |
| **Shrinkage** | 20% | `unexplained_loss` / (`total_received` + `last_counted_quantity`) | Unexplained loss: inventory is disappearing at a rate that can't be explained by recorded sales or adjustments. Loss rate >30% = 1.0. |
| **Shipment Gap** | 10% | `days_since_last_shipment` vs. expected reorder interval, `system_quantity` vs. `reorder_point` | Replenishment anomaly: the item should have been reordered (it's been too long since last shipment) but the system shows adequate stock -- suggesting the stock number is wrong. |

The composite score = `velocity * 0.25 + consistency * 0.25 + adjustment * 0.20 + shrinkage * 0.20 + shipment_gap * 0.10`, and determines the risk tier:
- **CRITICAL** (>= 0.75): Multiple strong signals. Immediate physical count required.
- **HIGH** (>= 0.50): Clear anomaly indicators. Schedule for next cycle count.
- **MEDIUM** (>= 0.30): Some concerning signals. Monitor closely.
- **LOW** (< 0.30): Within normal variance.

Each scored row also includes an `explanation_text` built from the signals, e.g.: *"Zero/low velocity: no sales for 87 days with 51 units on hand; Suspicious adjustments: 8 upward adjustments without matching shipments."*

**`gold_store_health`** -- Rolls up risk scores to the store level: total SKUs monitored, counts by risk tier, average composite score, estimated ghost inventory dollar value, estimated shrinkage dollars, and an overall PI accuracy percentage. This powers the store-by-store heatmap in the Analytics page.

**`gold_anomaly_summary`** -- Filters to only CRITICAL and HIGH items (~50K rows) and enriches them with SKU/store names, a `primary_anomaly_type` (derived from whichever component score is highest: ghost_inventory, systematic_inflation, stock_mismatch, shrinkage_spike, or replenishment_anomaly), `financial_impact` (system_quantity * retail_price), a `recommended_action`, and a `search_text` field that gets embedded by Vector Search for natural language similarity queries.

### Operational Layer (Serving)

Three liquid-clustered Delta tables hold the live operational state:

**`serving.at_risk_skus`** -- All CRITICAL and HIGH anomalies with a `status` column (open / sent_to_store / confirmed / dismissed / investigating) and `assigned_to`. This is the working queue that store teams process.

**`serving.store_validations`** -- Feedback from store teams: physical count, variance from system, validation type (confirmed/dismissed/investigated), notes, and who validated. This table closes the loop -- it's how the system learns whether its flags were correct.

**`serving.correction_history`** -- Approved corrections: old vs. new system quantity, financial impact of the correction, who approved it, and the outcome. This is the audit trail and the basis for measuring improvement over time.

### AI Analysis Layer

When a user requests a deep-dive on a specific SKU-store pair, the 4-step AI agent pipeline runs:

1. **Data Validation** -- Verifies data freshness and completeness for the specific SKU/store
2. **Signal Extraction** -- Computes all five component scores from the Gold table data
3. **Composite Risk Scoring** -- Weighted combination with a confidence calculation
4. **LLM Reasoning** -- All signals plus similar historical patterns (from Vector Search) are sent to Claude Sonnet 4.5, which generates: a human-readable explanation, a root cause hypothesis, and a specific suggested action

The entire pipeline is traced with MLflow for observability and debugging.

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

## Synthetic Data: Injected Anomaly Patterns

The demo uses synthetic data modeled on a fictional grocery/general merchandise retailer (FreshMart, 50 stores, 5,000 SKUs). Approximately 10% of SKU-store pairs (~25,000 combinations) have anomalies injected into the raw data, distributed across five realistic patterns:

| Anomaly Type | Share | How It's Injected |
|--------------|-------|-------------------|
| **Ghost Inventory** | 35% | `system_quantity` set to 50-200, but all sales shifted to 35+ days ago (no recent sales). The velocity score catches this. |
| **Systematic Inflation** | 20% | 3-8 upward correction adjustments injected by the same employee (EMP-900 range), with no matching shipment within +/-3 days. The adjustment score catches this. |
| **POS Scanning Errors** | 15% | `scanned_sku_id` set to a different SKU (a cheap Grocery item) for 30% of recent transactions, by high-numbered cashier IDs (CSH-800 range). Detectable via scanned vs. actual SKU mismatch. |
| **Shrinkage Spikes** | 15% | Large negative adjustments (-20 to -100 units) clustered within a few days at the same store. The shrinkage score catches this. |
| **Velocity Mismatch** | 15% | `system_quantity` reduced well below what shipments + count would suggest. The stock consistency score catches this. |

These injected patterns are designed to exercise each of the five scoring dimensions independently, so the engine can demonstrate detection across all anomaly types.

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
