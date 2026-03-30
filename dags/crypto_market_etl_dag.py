"""
crypto_market_etl_dag.py
------------------------
Apache Airflow DAG that orchestrates the full CoinGecko ETL pipeline.

Schedule: Every 6 hours  (@every 6 hours / cron: '0 */6 * * *')
Owner:    data-engineering
Retries:  3 × 5min backoff per task

DAG topology:
    bootstrap_schema
         │
         ├─── extract_and_load_coin_prices
         │
         └─── extract_and_load_global_stats
                        │
                    log_run_complete
"""

import uuid
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Import our ETL modules (these live alongside this DAG file or on PYTHONPATH)
from etl.extract import fetch_market_data, fetch_global_stats
from etl.transform import transform_market_data, transform_global_stats
from etl.load import bootstrap_schema, load_coin_prices, load_global_stats, log_run

logger = logging.getLogger(__name__)

# ─── Default args ──────────────────────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}

# ─── DAG definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id="crypto_market_etl",
    description="Extract CoinGecko market data, transform, and load to PostgreSQL every 6h",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 */6 * * *",   # 00:00, 06:00, 12:00, 18:00 UTC daily
    start_date=days_ago(1),
    catchup=False,                      # Don't backfill historical runs on deploy
    max_active_runs=1,                  # Prevent overlapping executions
    tags=["crypto", "etl", "coingecko", "market-data"],
) as dag:

    dag.doc_md = """
    ## Crypto Market ETL Pipeline

    **Source:** CoinGecko Public API (free tier, no key required)
    **Destination:** PostgreSQL `crypto_db`
    **Schedule:** Every 6 hours

    ### Tables populated
    | Table | Description |
    |---|---|
    | `coin_prices` | Per-coin price, volume, market cap, % changes |
    | `global_market_stats` | Aggregated market totals, BTC/ETH dominance |
    | `pipeline_run_log` | Audit trail of every DAG run |

    ### Retry strategy
    Each task retries 3× with exponential backoff (5min → 10min → 20min).
    """

    # ── Task 1: Schema bootstrap ────────────────────────────────────────────────

    def task_bootstrap():
        """Ensure all DB tables + indexes exist before any data is written."""
        bootstrap_schema()
        logger.info("Schema bootstrap complete")

    t_bootstrap = PythonOperator(
        task_id="bootstrap_schema",
        python_callable=task_bootstrap,
    )

    # ── Task 2: ETL for coin prices ─────────────────────────────────────────────

    def task_coin_prices(**context):
        """
        Full extract → transform → load for per-coin market data.
        Pushes record count to XCom for the run-log task.
        """
        run_id = context["run_id"]
        try:
            raw = fetch_market_data()
            clean = transform_market_data(raw)
            loaded = load_coin_prices(clean)
            context["ti"].xcom_push(key="coin_records_loaded", value=loaded)
            logger.info(f"[{run_id}] Coin prices ETL complete: {loaded} records")
        except Exception as e:
            log_run(run_id, status="FAILED", error=str(e))
            raise

    t_coin_prices = PythonOperator(
        task_id="extract_and_load_coin_prices",
        python_callable=task_coin_prices,
        provide_context=True,
    )

    # ── Task 3: ETL for global stats ────────────────────────────────────────────

    def task_global_stats(**context):
        """Full extract → transform → load for global market stats."""
        run_id = context["run_id"]
        try:
            raw = fetch_global_stats()
            clean = transform_global_stats(raw)
            load_global_stats(clean)
            logger.info(f"[{run_id}] Global stats ETL complete")
        except Exception as e:
            log_run(run_id, status="FAILED", error=str(e))
            raise

    t_global_stats = PythonOperator(
        task_id="extract_and_load_global_stats",
        python_callable=task_global_stats,
        provide_context=True,
    )

    # ── Task 4: Audit log ───────────────────────────────────────────────────────

    def task_log_completion(**context):
        """Write a SUCCESS entry to pipeline_run_log with total records loaded."""
        run_id = context["run_id"]
        coin_loaded = context["ti"].xcom_pull(
            task_ids="extract_and_load_coin_prices",
            key="coin_records_loaded"
        ) or 0
        log_run(run_id, status="SUCCESS", records_loaded=coin_loaded)
        logger.info(f"[{run_id}] Pipeline complete. Total coin records: {coin_loaded}")

    t_log_complete = PythonOperator(
        task_id="log_run_complete",
        python_callable=task_log_completion,
        provide_context=True,
        trigger_rule="all_success",
    )

    # ── DAG wiring ──────────────────────────────────────────────────────────────
    #
    #   bootstrap_schema
    #        │
    #        ├─── extract_and_load_coin_prices ──┐
    #        │                                   ├─── log_run_complete
    #        └─── extract_and_load_global_stats ─┘
    #

    t_bootstrap >> [t_coin_prices, t_global_stats] >> t_log_complete
