# Crypto Market ETL Pipeline

> End-to-end data engineering project: public API → ETL → PostgreSQL → Airflow automation

## Architecture

```
CoinGecko API (free tier)
        │
        ▼
  ┌─────────────┐
  │   Extract   │  fetch_market_data() + fetch_global_stats()
  └──────┬──────┘
         │  raw JSON
         ▼
  ┌─────────────┐
  │  Transform  │  clean · validate · derive · normalize timestamps
  └──────┬──────┘
         │  typed dicts
         ▼
  ┌─────────────┐
  │    Load     │  UPSERT → PostgreSQL (or SQLite for local dev)
  └──────┬──────┘
         │
         ▼
  Apache Airflow DAG  ──  runs every 6 hours
         │
         ├── bootstrap_schema
         ├── extract_and_load_coin_prices   ─┐
         ├── extract_and_load_global_stats  ─┤
         └── log_run_complete  ◄─────────────┘
```

## Tables

| Table | Rows per run | Key columns |
|---|---|---|
| `coin_prices` | 8 | `coin_id`, `current_price_usd`, `market_cap_usd`, `volatility_score` |
| `global_market_stats` | 1 | `total_market_cap_usd`, `btc_dominance_pct` |
| `pipeline_run_log` | 1 | `run_id`, `status`, `records_loaded` |

## Quick start — GitHub Codespaces

The fastest way to run this project is directly in your browser via GitHub Codespaces.
The `.devcontainer` config installs all dependencies automatically on launch.

1. Push this repo to GitHub
2. Click **Code → Codespaces → Create codespace on main**
3. Wait ~60s for the environment to build
4. Open `notebooks/04_pipeline_end_to_end.ipynb` and run all cells

No local Python install, no Docker, no configuration needed.

## Notebooks

Each stage of the pipeline has a dedicated notebook. Run them in order to step through
the full ETL, or jump straight to `04` for a single end-to-end execution.

| Notebook | Description |
|---|---|
| `01_extract.ipynb` | Call the CoinGecko API, inspect raw JSON, print a live price table |
| `02_transform.ipynb` | Clean nulls, normalize timestamps, compute volatility scores, verify null safety |
| `03_load.ipynb` | Bootstrap SQLite schema, upsert records, run sample queries against the DB |
| `04_pipeline_end_to_end.ipynb` | Full pipeline in one shot — the recommended starting point |

To run locally:

```bash
pip install -r requirements.txt
jupyter lab notebooks/
```

## Full stack (Airflow + PostgreSQL via Docker)

The `dags/` folder contains the production Airflow DAG that mirrors the notebook logic.
Use this when you're ready to schedule automated runs against a real PostgreSQL instance.

```bash
# Spin everything up
docker compose up -d

# Wait ~60s for Airflow to initialize, then open:
# Airflow UI  →  http://localhost:8080  (admin / admin)
# Trigger the DAG manually or let it run on schedule (every 6h)

# Tear down
docker compose down -v
```

## Project structure

```
crypto-etl-pipeline/
├── notebooks/
│   ├── 01_extract.ipynb               # CoinGecko API client walkthrough
│   ├── 02_transform.ipynb             # Cleaning, validation, derived metrics
│   ├── 03_load.ipynb                  # SQLite schema, upserts, queries
│   └── 04_pipeline_end_to_end.ipynb  # Full E2E run — start here
├── dags/
│   └── crypto_market_etl_dag.py      # Airflow DAG (production scheduling)
├── etl/
│   ├── extract.py                     # CoinGecko API client (module)
│   ├── transform.py                   # Cleaning + enrichment (module)
│   └── load.py                        # DB upsert logic (module)
├── tests/
│   └── test_transform.py              # 12 pytest unit tests
├── config/
│   └── init_crypto_db.sql             # PostgreSQL bootstrap
├── .devcontainer/
│   └── devcontainer.json              # GitHub Codespaces config
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## Key design decisions

- **Idempotent upserts** — safe to re-run without creating duplicate rows (`UNIQUE` on `coin_id + ingested_at`)
- **Pure transform functions** — no DB calls inside transform; fully unit-testable
- **Dual backend** — `DB_BACKEND=sqlite` for local dev, `DB_BACKEND=postgres` for production
- **Audit trail** — every DAG run writes a row to `pipeline_run_log` with status and record counts
- **Derived metrics** — `volatility_score` (|1h_change − 24h_change|) computed at transform time, not query time
- **Exponential backoff** — Airflow retries 3× with 5 → 10 → 20 min delays on transient API failures

## Sample queries

```sql
-- Latest price snapshot per coin
SELECT coin_id, symbol, current_price_usd, price_change_pct_24h, volatility_score
FROM coin_prices
WHERE ingested_at = (SELECT MAX(ingested_at) FROM coin_prices)
ORDER BY market_cap_rank;

-- Price trend for BTC over last 7 days
SELECT DATE(ingested_at) AS day, AVG(current_price_usd) AS avg_price
FROM coin_prices
WHERE coin_id = 'bitcoin'
  AND ingested_at >= datetime('now', '-7 days')
GROUP BY day ORDER BY day;

-- Most volatile coins in last 24h
SELECT coin_id, symbol, volatility_score, price_change_pct_1h, price_change_pct_24h
FROM coin_prices
WHERE ingested_at = (SELECT MAX(ingested_at) FROM coin_prices)
ORDER BY volatility_score DESC
LIMIT 5;

-- Pipeline health
SELECT run_id, status, records_loaded, started_at
FROM pipeline_run_log
ORDER BY started_at DESC
LIMIT 10;
```

## Tech stack

| Layer | Tool |
|---|---|
| Source | CoinGecko Public API |
| Language | Python 3.11 |
| Orchestration | Apache Airflow 2.9 |
| Warehouse | PostgreSQL 16 |
| Local dev DB | SQLite |
| Testing | pytest |
| Infra | Docker Compose |
