# 📊 Financial Data Quality Monitor (FDQM)

**A production-grade, Airflow-orchestrated data quality monitoring pipeline for financial market data — featuring statistical anomaly detection, cross-source reconciliation, and a real-time Streamlit health scorecard.**

Built to demonstrate end-to-end data engineering for quantitative asset management, covering multi-asset ingestion, automated quality controls, and operational alerting.

![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python&logoColor=white)
![Airflow](https://img.shields.io/badge/Apache_Airflow-2.8-017CEE?logo=apacheairflow&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791?logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-1.31-FF4B4B?logo=streamlit&logoColor=white)
![Great Expectations](https://img.shields.io/badge/Great_Expectations-0.18-FF6310)

---

## 📸 Dashboard Screenshots

<table>
  <tr>
    <td><img src="docs/screenshots/health_scorecard.png" alt="Health Scorecard" width="400"/><br/><em>Health Scorecard — RED/YELLOW/GREEN status per category</em></td>
    <td><img src="docs/screenshots/anomaly_explorer.png" alt="Anomaly Explorer" width="400"/><br/><em>Anomaly Explorer — Price chart with Bollinger bands & z-score</em></td>
  </tr>
  <tr>
    <td><img src="docs/screenshots/reconciliation.png" alt="Reconciliation" width="400"/><br/><em>Cross-Source Reconciliation — Yahoo vs Alpha Vantage heatmap</em></td>
    <td><img src="docs/screenshots/ingestion_log.png" alt="Ingestion Log" width="400"/><br/><em>Ingestion Log — Pipeline history & source registry</em></td>
  </tr>
</table>

> 💡 Save your dashboard screenshots to `docs/screenshots/` and they'll render here automatically.

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          ORCHESTRATION — Apache Airflow                 │
│                                                                         │
│   DAG 1: Ingestion (6AM ET)    DAG 2: Quality (7AM ET)    DAG 3: Report│
│   ┌──────────────────┐         ┌────────────────────┐     ┌───────────┐│
│   │ Check Market Open │───────▶│ Great Expectations  │────▶│ Scorecard ││
│   │ Yahoo Finance     │        │ Null Checks         │    │ Anomaly   ││
│   │ Alpha Vantage     │        │ Stale Detection     │    │  Summary  ││
│   │ FRED API          │        │ Z-Score Anomaly     │    │ Alerting  ││
│   │ Log Metadata      │        │ Rolling Window      │    └───────────┘│
│   └──────────────────┘         │ Cross-Source Recon  │                  │
│                                 │ Gap Detection       │                  │
│                                 │ Health Score Calc   │                  │
│                                 └────────────────────┘                  │
└────────────────────────────────────┬────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                          STORAGE — PostgreSQL 15                        │
│                                                                         │
│   Raw Data              Quality Results           Reference             │
│   ├── equities          ├── qc_results            ├── market_holidays   │
│   ├── equities_recon    ├── anomaly_log           ├── data_source_      │
│   └── macro_indicators  ├── recon_results         │   registry          │
│                          ├── ge_validation_results └── ingestion_       │
│                          │                             metadata         │
└────────────────────────────────────┬────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                       DASHBOARD — Streamlit                             │
│                                                                         │
│   🏥 Health Scorecard  │  🔍 Anomaly Explorer  │  ⚖️ Reconciliation    │
│   📥 Ingestion Log     │                                                │
└─────────────────────────────────────────────────────────────────────────┘
```

### DAG Chain Flow

```
DAG 1: Ingestion ──triggers──▶ DAG 2: Quality Control ──triggers──▶ DAG 3: Reporting
     (6 AM ET)                       (7 AM ET)                         (8 AM ET)

  • Market holiday check          • Great Expectations             • Daily scorecard
  • Yahoo Finance (8 ETFs)        • Null checks (11 rules)        • Anomaly summary
  • Alpha Vantage (recon)         • Stale detection (9 feeds)     • Alert dispatch
  • FRED (7 macro series)         • Z-score anomalies               (log + email)
  • Monthly FRED (1st biz day)    • Rolling window bands          • Pipeline audit log
  • Metadata logging              • Volume spike detection
                                  • Cross-source recon
                                  • Holiday-aware gaps
                                  • Health score computation
```

---

## 📦 Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Orchestration** | Apache Airflow 2.8 | 3 chained DAGs, market-open branching, auto-retry |
| **Database** | PostgreSQL 15 | 10 tables — raw data, QC results, reference, audit |
| **Ingestion** | yfinance, fredapi, Alpha Vantage REST | 3 API sources with retry + rate limiting |
| **Quality** | Custom Python + scipy + pandas | Z-score, rolling window, recon, gap detection |
| **Validation** | Great Expectations 0.18 | 3 programmatic suites (33 expectations total) |
| **Dashboard** | Streamlit + Plotly | 4-page interactive dashboard with dark theme |
| **Infrastructure** | Docker Compose | 5 services, single-command deployment |
| **Config** | Pydantic Settings | Type-safe, env-driven configuration |
| **Logging** | structlog | Structured JSON logging throughout |
| **Resilience** | tenacity | Exponential backoff retry on all API calls |

---

## 📁 Project Structure

```
financial-data-quality-monitor/
│
├── docker-compose.yml              # Full stack: Postgres + Airflow + Streamlit
├── Dockerfile.airflow              # Airflow image with project deps
├── Dockerfile.streamlit            # Lightweight dashboard image
├── .env.example                    # Environment config template
├── requirements.txt                # Python dependencies
│
├── dags/
│   ├── dag_data_ingestion.py       # DAG 1: Market-aware data ingestion
│   ├── dag_quality_control.py      # DAG 2: 6 parallel QC checks + GE
│   └── dag_reporting.py            # DAG 3: Scorecard + alerting
│
├── src/
│   ├── ingestion/
│   │   ├── base_ingester.py        # ABC with retry, rate limit, upsert
│   │   ├── yahoo_finance.py        # Equity OHLCV (8 ETFs)
│   │   ├── fred_api.py             # Macro indicators (7 FRED series)
│   │   └── alpha_vantage.py        # Reconciliation source
│   │
│   ├── quality/
│   │   ├── null_checks.py          # 11 null/missing value rules
│   │   ├── stale_detection.py      # 9 freshness rules (daily + monthly)
│   │   ├── anomaly_detection.py    # Z-score + rolling window + volume spike
│   │   ├── cross_source_recon.py   # Yahoo vs Alpha Vantage comparison
│   │   ├── gap_detection.py        # Holiday-aware missing day detection
│   │   ├── ge_validation.py        # 3 Great Expectations suites
│   │   └── quality_engine.py       # Orchestrator + health score computation
│   │
│   ├── models/
│   │   └── schema.py               # 10 SQLAlchemy ORM models
│   │
│   ├── database/
│   │   └── connection.py           # Engine singleton, session manager
│   │
│   └── utils/
│       ├── config.py               # Pydantic-powered centralized config
│       ├── market_calendar.py      # NYSE holiday logic, trading day utils
│       └── alerting.py             # Alert dispatch (log + email)
│
├── great_expectations/
│   └── great_expectations.yml      # GE project config
│
├── streamlit_app/
│   ├── app.py                      # Main app + landing page
│   ├── pages/
│   │   ├── 01_health_scorecard.py  # Health cards + trend + GE results
│   │   ├── 02_anomaly_explorer.py  # Price charts + z-score + volume
│   │   ├── 03_reconciliation.py    # Heatmap + source comparison
│   │   └── 04_ingestion_log.py     # Pipeline history + error logs
│   └── components/
│       ├── charts.py               # 6 Plotly chart types (dark themed)
│       └── scorecard_card.py       # Reusable HTML status cards
│
├── scripts/
│   ├── init_db.py                  # Table creation + seed 77 NYSE holidays
│   ├── init_postgres.sql           # App database + user creation
│   ├── seed_historical.py          # 2-year backfill from all sources
│   └── setup_directories.sh        # Project scaffolding script
│
└── docs/
    └── screenshots/                # Dashboard screenshots for README
```

---

## 🚀 Quick Start

### Prerequisites

- **Docker Desktop** (4GB+ memory allocated)
- **Free API keys:**
  - [FRED API Key](https://fred.stlouisfed.org/docs/api/api_key.html) (instant)
  - [Alpha Vantage Key](https://www.alphavantage.co/support/#api-key) (instant)

### 1. Clone & Configure

```bash
git clone https://github.com/YOUR_USERNAME/financial-data-quality-monitor.git
cd financial-data-quality-monitor

# Create environment file
cp .env.example .env

# Add your API keys to .env
# FRED_API_KEY=your_key_here
# ALPHA_VANTAGE_API_KEY=your_key_here
```

### 2. Create Directory Structure

```bash
bash scripts/setup_directories.sh
```

### 3. Start the Stack

```bash
# Build and launch all services
docker compose up -d

# Wait ~60 seconds for Airflow to initialize, then verify
docker compose ps
```

### 4. Initialize Database & Seed Data

```bash
# Create tables + seed 77 NYSE holidays
docker compose run --rm --entrypoint bash app-db-init -c \
  "cd /opt/airflow && python scripts/init_db.py"

# Backfill 2 years of historical data
docker compose run --rm --entrypoint bash app-db-init -c \
  "cd /opt/airflow && python scripts/seed_historical.py"
```

### 5. Run Quality Checks

```bash
docker compose run --rm --entrypoint bash app-db-init -c \
  "cd /opt/airflow && python -c \"
from src.quality.quality_engine import run_full_quality_suite
result = run_full_quality_suite()
print(f'Health Score: {result[\"health_score\"][\"score\"]}% ({result[\"health_score\"][\"color\"]})')
\""
```

### 6. Access Dashboards

| Service | URL | Credentials |
|---------|-----|-------------|
| **Streamlit Dashboard** | http://localhost:8501 | — |
| **Airflow UI** | http://localhost:8080 | admin / admin |

---

## 📊 Data Coverage

### Equities (8 ETFs across 5 asset classes)

| Ticker | Asset Class | Description |
|--------|------------|-------------|
| SPY | US Large Cap | S&P 500 ETF |
| QQQ | US Growth | Nasdaq-100 ETF |
| IWM | US Small Cap | Russell 2000 ETF |
| EFA | Intl Developed | MSCI EAFE ETF |
| AGG | US Fixed Income | Bloomberg Agg Bond ETF |
| TLT | Long Treasury | 20+ Year Treasury ETF |
| GLD | Commodities | Gold ETF |
| VNQ | REITs | Vanguard Real Estate ETF |

### Macro Indicators (7 FRED series)

| Series | Name | Frequency |
|--------|------|-----------|
| DGS10 | 10-Year Treasury Yield | Daily |
| DGS2 | 2-Year Treasury Yield | Daily |
| DTWEXBGS | Trade-Weighted USD Index | Daily |
| CPIAUCSL | Consumer Price Index | Monthly |
| UNRATE | Unemployment Rate | Monthly |
| PCE | Personal Consumption Expenditures | Monthly |
| UMCSENT | Consumer Sentiment Index | Monthly |

---

## 🔍 Quality Control Layers

### Layer 1: Great Expectations Validation
Three programmatic suites validate structural contracts — schema existence, column types, value ranges, uniqueness constraints, and referential integrity. **33 expectations** across equity, macro, and reconciliation data.

### Layer 2: Null & Missing Value Detection
**11 rules** scan critical columns. Close prices and macro values have zero-tolerance (CRITICAL severity). OHLC columns allow <1% nulls (MEDIUM severity).

### Layer 3: Stale Data Detection
**9 freshness rules** with holiday-aware expected dates. Daily equity data expected by T-1; monthly macro data within 45-60 days. Uses NYSE holiday calendar (77 holidays seeded) to avoid false positives on market closures.

### Layer 4: Statistical Anomaly Detection
**Z-Score Method:** Flags values deviating >2σ (WARN) or >3σ (FAIL) from a trailing 60-day baseline. Applied to close prices and macro values.

**Rolling Window Method:** 20-day rolling mean ± 2σ bands (Bollinger-style). Catches gradual drift missed by wider z-score windows.

**Volume Spike Detection:** Flags days where volume exceeds 3× the 20-day rolling average.

### Layer 5: Cross-Source Reconciliation
Joins Yahoo Finance and Alpha Vantage on `(ticker, trade_date)`. Flags price breaks exceeding **0.5%** and volume breaks exceeding **5%**. Every comparison (MATCH and BREAK) is stored in `recon_results` for full audit trail.

### Layer 6: Holiday-Aware Gap Detection
Compares actual data dates against expected NYSE trading days (weekdays minus holidays). Groups consecutive gaps into stretches. Severity: 1 day = MEDIUM, 2-3 days = HIGH, 4+ days = CRITICAL.

### Health Score Computation
```
Weighted Score = Σ(status_score × severity_weight) / Σ(severity_weight)

Status:   PASS = 1.0    WARN = 0.5    FAIL = 0.0
Weight:   CRITICAL = 3x  HIGH = 2x    MEDIUM = 1x   LOW = 0.5x

GREEN  ≥ 90%  |  YELLOW  ≥ 70%  |  RED  < 70%
```

---

## 🔑 Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| **3 separate DAGs** | Ingestion, Quality, Reporting run independently — each is retriggerable without re-running the full chain |
| **Idempotent upserts** | `INSERT ON CONFLICT DO NOTHING` makes every pipeline run safe to retry without duplicates |
| **Trailing window stats** | Z-scores adapt to market regime changes — no hardcoded thresholds to maintain |
| **Severity-weighted scoring** | A null close price (CRITICAL, 3× weight) impacts the score far more than stale sentiment data (LOW, 0.5× weight) |
| **Holiday calendar in DB** | 77 NYSE holidays seeded for 2020-2027; prevents false gap alerts on MLK Day, Good Friday, Thanksgiving, etc. |
| **Programmatic GE suites** | Code-defined expectations are version-controlled, testable, and don't break across GE upgrades |
| **Base Ingester pattern** | Abstract base class with retry, rate limiting, upsert, and audit logging — adding a new source requires only `fetch_data()` and `transform_data()` |

---

## 📈 Sample Pipeline Results

From a live run on **2026-03-12**:

| Metric | Value |
|--------|-------|
| Overall Health Score | **86.8% (YELLOW)** |
| Equity Records | 4,016 rows (8 ETFs × ~500 trading days) |
| Macro Records | 1,582 observations (7 series) |
| QC Checks Executed | 29 |
| Null Check Pass Rate | 100% (11/11) |
| Gap Detection Pass Rate | 100% (8/8) |
| Recon Match Rate | 100% (0 breaks) |
| Anomalies Detected | 232 (mostly WARN — legitimate market moves) |
| Pipeline Avg Duration | 30s per source |

The YELLOW score reflects reality: monthly FRED series have legitimate publication delays, and the anomaly detector correctly flagged SPY's recent price dips (z-score ≈ -2.8) as statistical outliers.

---

## ⚙️ Configuration

All settings are environment-driven via `.env`:

```bash
# Quality thresholds
ZSCORE_WARN_THRESHOLD=2.0        # Flag if |z| > 2
ZSCORE_FAIL_THRESHOLD=3.0        # Critical if |z| > 3
ROLLING_WINDOW_DAYS=20           # Rolling band window
ANOMALY_LOOKBACK_DAYS=60         # Z-score baseline period
RECON_PRICE_TOLERANCE_PCT=0.005  # 0.5% recon tolerance
RECON_VOLUME_TOLERANCE_PCT=0.05  # 5% volume tolerance

# Tickers and series
EQUITY_TICKERS=SPY,QQQ,IWM,EFA,AGG,TLT,GLD,VNQ
FRED_DAILY_SERIES=DGS10,DGS2,DTWEXBGS
FRED_MONTHLY_SERIES=CPIAUCSL,UNRATE,PCE,UMCSENT

# Alerting
ENABLE_EMAIL_ALERTS=false
ALERT_EMAIL_TO=your_email@example.com
```

---

## 🛠️ Database Schema

**10 tables** organized into three groups:

**Raw Data** — `equities` (OHLCV from Yahoo), `equities_recon` (close/volume from Alpha Vantage), `macro_indicators` (FRED series with metadata)

**Quality Results** — `qc_results` (master check results with JSONB details), `anomaly_log` (per-observation anomaly records), `recon_results` (every MATCH/BREAK pair), `ge_validation_results` (Great Expectations run outcomes)

**Reference & Audit** — `market_holidays` (77 NYSE holidays), `data_source_registry` (API metadata), `ingestion_metadata` (full pipeline audit trail)

---

## 🚧 Future Enhancements

- [ ] **ML-based anomaly detection** — Isolation forest for multivariate outlier detection
- [ ] **International calendars** — LSE, TSE, HKEX holiday support for global coverage
- [ ] **Streaming ingestion** — Kafka integration for intraday monitoring
- [ ] **Automated remediation** — Analyst override workflow with approval chain
- [ ] **Slack/PagerDuty** — Real-time alert integration beyond email
- [ ] **CI/CD** — GitHub Actions for lint + test + Docker build on PR
- [ ] **dbt integration** — Transform layer between raw and quality tables

---

## 📄 License

This project is for portfolio and educational purposes.

---

<div align="center">
  <p>Built with ❤️ using Apache Airflow · PostgreSQL · Great Expectations · Streamlit</p>
</div>
