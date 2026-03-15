# CLAUDE.md — Bike-Share Operations Platform

## Role

Operations intelligence platform for a bike-sharing service. Core objective: **maximize revenue**, **minimize costs**, **improve EBITDA/GP** through data-driven operations.

### Decision Framework
- Revenue = ride_count x revenue_per_ride -> improve accessibility & conversion rate
- Cost = field_ops (maintenance, rebalancing, battery) -> optimize task efficiency
- EBITDA = revenue - ops_cost -> optimize both levers simultaneously

### Decision Criteria
- Deploy bikes -> prioritize areas with low accessibility but high conversion
- Rebalance bikes -> remove from areas with high accessibility but low conversion
- Maintenance priority -> focus on areas/hours with highest availability impact
- Cost reduction -> reduce task time, compare efficiency across centers

---

## Data Schema

### 1. app_accessibility (App Open Events)

**1 row = 1 app open event**

| Field | Type | Description |
|-------|------|-------------|
| `event_time` | TIMESTAMP | App open timestamp |
| `date` | DATE | Date |
| `hour` | INTEGER | Hour (0-23) |
| `h3_area_name` | STRING | Region name (mid-level geography) |
| `h3_district_name` | STRING | District name (low-level geography) |
| `bike_count_100` | INTEGER | Bikes within 100m radius |
| `bike_count_400` | INTEGER | Bikes within 400m radius |
| `distance` | INTEGER | Distance to nearest bike (meters) |
| `is_accessible` | BOOLEAN | Bike exists within 100m |
| `is_converted` | BOOLEAN | User actually rode |
| `user_id` | INTEGER | User ID |
| `near_geoblock` | BOOLEAN | Near restricted zone |

### 2. maintenance (Maintenance Tasks)

| Column | Type | Description |
|--------|------|-------------|
| id | INT64 | PK |
| created_time / completed_time | DATETIME | KST |
| type | INT64 | 0: REBALANCE, 1: BATTERY, 2: BROKEN |
| status | INT64 | 0: CALL, 1: CANCEL, 2: PROGRESS, 3: COMPLETE |
| vehicle_id | INT64 | Bike ID |
| bike_type | INT64 | 1: bicycle, 2: scooter |
| center_id | INT64 | Service center |

### 3. rides (Ride Records)

| Column | Description |
|--------|-------------|
| start_time, end_time | Start/end timestamps |
| bike_id, bike_type | Bike info |
| region, h3_start_area_name | Geographic info |
| user_id | User |
| distance, duration, fee | Ride metrics |

### 4. bike_snapshot (Hourly Bike Status)

| Column | Description |
|--------|-------------|
| bike_status | Status code (BAV, LAV, etc.) |
| bike_id, sn, type | Bike info |
| leftover, battery | Battery level |
| is_active, is_usable | Status flags |
| h3_area_name, h3_district_name | H3 geography |
| date, hour, time | Timestamp (KST) |

---

## Key Metrics

| Metric | Formula | Description |
|--------|---------|-------------|
| Accessibility Rate | `accessible_count / total_opens` | % of app opens with bike within 100m |
| Conversion Rate | `converted_count / accessible_count` | % of accessible users who ride |
| Availability Rate | `usable_bikes / total_bikes` | % of fleet available |
| Field Action Rate | `usable / on_field` | % of on-field bikes rideable |

### Analysis Funnel
```
App Open -> Accessible (bike within 100m) -> Converted (actual ride)
```
- Stage 1 drop-off: No bike nearby -> **supply shortage**
- Stage 2 drop-off: Bike available but not used -> **demand/quality issue**

---

## Bike Status Codes

**Prefix: B = battery sufficient / L = battery low**

| Category | Code | Meaning | User Rideable |
|----------|------|---------|---------------|
| Available | BAV | Available | Yes |
| Available | BNB | Needs rebalancing | Yes |
| Riding | BRD/LRD | Currently riding | - |
| Field Action | LAV | Low battery | No |
| Field Action | LNB | Low battery + rebalance | No |
| Field Action | BB/LB | Being rebalanced | No |
| Field Action | BNP/LNP | Needs repair | No |
| In Repair | BP/LP | Under repair | No |

---

## Tech Stack
- Language: Python 3.11
- Data: BigQuery, Google Sheets
- ML: LightGBM, scikit-learn, SciPy
- Dashboard: Streamlit, Folium, Plotly
- AI: Anthropic Claude API
- Infra: GitHub Actions

## Service Centers
Center_North, Center_West, Center_South, Center_East, Center_Central, Partner_Seoul, Partner_Gwacheon, Partner_Ansan, Partner_Daejeon
