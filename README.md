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

## Quick start (SQLite — no Docker needed)

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Run the full pipeline once
python -c "
from etl.extract import fetch_market_data, fetch_global_stats
from etl.transform import transform_market_data, transform_global_stats
from etl.load import bootstrap_schema, load_coin_prices, load_global_stats

bootstrap_schema()
load_coin_prices(transform_market_data(fetch_market_data()))
load_global_stats(transform_global_stats(fetch_global_stats()))
print('Done! DB at data/crypto_market.db')
"

# 3. Run tests
pytest tests/ -v
```

## Full stack (Airflow + PostgreSQL via Docker)

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
├── dags/
│   └── crypto_market_etl_dag.py  # Airflow DAG definition
├── etl/
│   ├── extract.py                 # CoinGecko API client
│   ├── transform.py               # Cleaning + enrichment
│   └── load.py                    # DB upsert logic
├── tests/
│   └── test_transform.py          # 12 pytest unit tests
├── config/
│   └── init_crypto_db.sql         # PostgreSQL bootstrap
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
