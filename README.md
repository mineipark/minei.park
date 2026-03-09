# Bike-Share Operations Platform

An end-to-end operations intelligence platform for a bike-sharing service, covering **demand forecasting**, **supply-demand gap analysis**, **route optimization**, **real-time operations dashboards**, and **workflow automation**.

Built during my work as a PMO (Project Management Office) team member, this platform helped optimize fleet utilization, reduce operational costs, and maximize revenue through data-driven decision making.

> **Note:** This is a sanitized portfolio version. All company-specific data, credentials, and identifying information have been removed or generalized.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                    Data Sources                               │
│  BigQuery  ·  Weather API  ·  Google Sheets  ·  App Events   │
└──────────┬───────────────────────────────────┬───────────────┘
           │                                   │
           ▼                                   ▼
┌─────────────────────┐         ┌──────────────────────────────┐
│  Demand Forecasting │         │  Operations Dashboard        │
│  ─────────────────  │         │  ──────────────────────────  │
│  · ML Models (V7/V8)│         │  · Streamlit Multi-page App  │
│  · Conversion Model │         │  · Center-level KPIs         │
│  · District-Hour    │         │  · Worker Route Tracking     │
│  · Weather Adjust   │         │  · Maintenance Performance   │
└────────┬────────────┘         └──────────────────────────────┘
         │
         ▼
┌─────────────────────┐         ┌──────────────────────────────┐
│  Supply-Demand Gap  │         │  Workflow Automation          │
│  ─────────────────  │         │  ──────────────────────────  │
│  · Gap Analysis     │         │  · Email → AI Parser         │
│  · Work Orders      │         │  · Slack Bot Approval        │
│  · Priority Scoring │         │  · Admin Web Automation      │
│  · Folium Maps      │         │  · Return Zone Processing    │
└────────┬────────────┘         └──────────────────────────────┘
         │
         ▼
┌─────────────────────┐         ┌──────────────────────────────┐
│  Route Optimization │         │  Automated Reports           │
│  ─────────────────  │         │  ──────────────────────────  │
│  · TSP Solver       │         │  · Monthly Fleet Stats       │
│  · Cluster-based    │         │  · Slack Integration         │
│  · Time-slot Split  │         │  · GitHub Actions CI/CD      │
│  · AntPath Visual   │         │  · Google Sheets Sync        │
└─────────────────────┘         └──────────────────────────────┘
```

<!-- Screenshots / diagrams can be added here -->
<!-- ![Dashboard Screenshot](docs/images/dashboard.png) -->
<!-- ![Gap Map Example](docs/images/gap_map.png) -->

---

## Key Modules

### 1. Demand Forecasting (`demand_forecast/`)

ML-powered ride demand prediction at district x hour granularity.

- **V7 Model** (`demand_model_v7.py`): GradientBoosting with region-specific weather/day-of-week corrections
- **V8 App-Open Model** (`app_open_model.py`): Breaks the circular dependency of supply-constrained ride data by predicting app opens first, then applying conversion rates
- **Conversion Model** (`conversion_model.py`): Learns the bike_count -> conversion_rate curve using exponential saturation: `CVR = base + gain * (1 - e^(-decay * bikes))`
- **District-Hour Model** (`district_hour_model.py`): Hierarchical disaggregation from region -> district -> hour
- **Auto-Tuner** (`district_hour_tuner.py`, `auto_improve.py`): Automated parameter optimization with backtesting

### 2. Supply-Demand Gap Analysis (`demand_forecast/supply_demand_gap.py`)

Compares predicted demand with current bike supply to generate actionable work orders.

- Time-slot based analysis (night prep / morning / afternoon / evening)
- Priority scoring with unconstrained demand weighting
- Center-specific Excel work orders
- Interactive Folium gap visualization maps

### 3. Route Optimization (`demand_forecast/route_optimization_v5.py`)

Optimizes field worker routes for bike rebalancing and battery swap tasks.

- Time-slot separation (afternoon demand response vs. evening preparation)
- Demand-cluster-based routing with TSP solver
- Task type bundling (rebalance, battery, repair)
- Animated route visualization with Folium AntPath

### 4. Operations Dashboard (`ops_dashboard/`)

Multi-page Streamlit dashboard for real-time operations monitoring.

- **All-Center Overview**: Fleet-wide KPIs (availability, repair rate, field action rate)
- **Per-Center Dashboard**: Detailed metrics per service center
- **Worker Route Tracking**: GPS-based worker movement visualization
- **Monthly Performance**: Per-worker monthly statistics
- **Maintenance Analytics**: Repair efficiency, cost analysis

### 5. Return Zone Approval (`return_zone_approval/`)

End-to-end automation for processing return zone requests.

- Gmail monitoring for incoming requests
- AI-powered document parsing (Claude API)
- Slack bot with interactive approval workflow
- Playwright-based admin web automation

### 6. Automated Reporting (`bike_stats_report.py`)

Monthly fleet statistics report with Slack integration.

- BigQuery data aggregation
- Slack Block Kit message formatting
- GitHub Actions scheduled execution

---

## Tech Stack

| Category | Technologies |
|----------|-------------|
| **Language** | Python 3.11 |
| **Data Warehouse** | Google BigQuery |
| **ML/Statistics** | LightGBM, scikit-learn, SciPy (curve fitting) |
| **Visualization** | Streamlit, Folium, Plotly |
| **Geospatial** | H3 hexagonal indexing, GeoJSON |
| **Automation** | Playwright, Gmail API, Slack Bolt |
| **AI** | Anthropic Claude API (document parsing) |
| **Infrastructure** | GitHub Actions, Firebase |
| **Integrations** | Google Sheets API, Slack API, Weather API |

---

## Setup Guide

### Prerequisites

- Python 3.11+
- Google Cloud service account with BigQuery access (or use sample data)

### Installation

```bash
# Clone the repository
git clone https://github.com/mineipark/minei.park.git
cd minei.park

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# .venv\Scripts\activate   # Windows

# Install dependencies (pick your module)
pip install pandas numpy scikit-learn scipy google-cloud-bigquery folium
# For dashboard:
pip install -r ops_dashboard/requirements.txt
# For return zone approval:
pip install -r return_zone_approval/requirements.txt

# Configure environment
cp .env.example .env
# Edit .env with your credentials
```

### Generate Sample Data

If you don't have BigQuery access, generate synthetic data:

```bash
python seed_data.py --days 90 --bikes 500
```

### Run the Dashboard

```bash
cd ops_dashboard
streamlit run ops_worker_dashboard.py
```

### Run Demand Forecast

```bash
# Single date prediction
python demand_forecast/app_open_model.py --date 2026-02-25

# Supply-demand gap analysis
python demand_forecast/supply_demand_gap.py --date 2026-02-25

# Route optimization
python demand_forecast/route_optimization_v5.py
```

---

## Project Structure

```
.
├── demand_forecast/           # ML demand prediction & optimization
│   ├── app_open_model.py      # V8: App-open based prediction
│   ├── demand_model_v7.py     # V7: Region-corrected model
│   ├── conversion_model.py    # Bike count → conversion rate
│   ├── district_hour_model.py # District×hour disaggregation
│   ├── supply_demand_gap.py   # Gap analysis + work orders
│   ├── route_optimization_v5.py # Field worker route optimizer
│   ├── relocation_task_system.py # Evening rebalancing support
│   ├── daily_pipeline.py      # Automated daily prediction pipeline
│   └── ...
├── ops_dashboard/             # Streamlit operations dashboard
│   ├── ops_worker_dashboard.py # Main app entry point
│   ├── pages/                 # Multi-page dashboard views
│   └── utils/                 # BigQuery, Sheets, calculation helpers
├── return_zone_approval/      # Automated approval workflow
│   ├── main.py                # Orchestrator
│   ├── email_monitor/         # Gmail integration
│   ├── parser/                # AI document parser
│   ├── slack_bot/             # Slack interactive bot
│   ├── automation/            # Admin web automation
│   └── workflow/              # Approval state machine
├── service_flow_visualizer/   # Service flow map visualization
├── reallocation/              # Bike reallocation algorithm
├── bike_stats_report.py       # Monthly fleet report → Slack
├── seed_data.py               # Sample data generator
├── .env.example               # Environment variable template
├── .github/workflows/         # CI/CD automation
└── CLAUDE.md                  # AI assistant context
```

---

## Key Metrics & Formulas

| Metric | Formula | Description |
|--------|---------|-------------|
| **Accessibility Rate** | `accessible_opens / total_opens` | % of app opens with a bike within 100m |
| **Conversion Rate** | `rides / accessible_opens` | % of accessible users who actually ride |
| **Availability Rate** | `usable_bikes / total_bikes` | % of fleet available for rides |
| **Field Action Rate** | `usable_bikes / on_field_bikes` | % of on-field bikes that are rideable |
| **Supply-Demand Gap** | `predicted_demand - available_bikes` | Bikes needed per district |

### Analysis Funnel

```
App Open → Accessible (bike within 100m) → Converted (actual ride)
  │              │                              │
  └─ Stage 1     └─ Stage 2                     └─ Revenue
     drop-off:      drop-off:
     Supply gap      Quality/UX issue
```

---

## License

This project is shared for portfolio and educational purposes. The code architecture and algorithms are original work. All business-specific data has been anonymized.
