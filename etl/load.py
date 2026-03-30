"""
load.py
-------
Loads transformed records into PostgreSQL.
Falls back to SQLite for local development (set DB_BACKEND=sqlite in .env).

Uses an UPSERT strategy on (coin_id, ingested_at) — safe to re-run
idempotently without creating duplicate rows.
"""

import os
import logging
import sqlite3
from datetime import datetime, timezone
from contextlib import contextmanager
from typing import Generator

logger = logging.getLogger(__name__)

# ─── Connection helpers ────────────────────────────────────────────────────────

DB_BACKEND = os.getenv("DB_BACKEND", "sqlite")  # "postgres" | "sqlite"
SQLITE_PATH = os.getenv("SQLITE_PATH", "data/crypto_market.db")
PG_CONN_STR = os.getenv(
    "POSTGRES_CONN",
    "postgresql://postgres:password@localhost:5432/crypto_db"
)


@contextmanager
def get_connection() -> Generator:
    """
    Context manager returning an active DB connection.
    Automatically commits or rolls back on exit.
    """
    if DB_BACKEND == "postgres":
        try:
            import psycopg2
            conn = psycopg2.connect(PG_CONN_STR)
            try:
                yield conn
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()
        except ImportError:
            raise RuntimeError("psycopg2 not installed. Run: pip install psycopg2-binary")
    else:
        os.makedirs(os.path.dirname(SQLITE_PATH) or ".", exist_ok=True)
        conn = sqlite3.connect(SQLITE_PATH)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


# ─── Schema bootstrap ──────────────────────────────────────────────────────────

CREATE_COIN_PRICES_SQL = """
CREATE TABLE IF NOT EXISTS coin_prices (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    coin_id                     TEXT NOT NULL,
    symbol                      TEXT NOT NULL,
    name                        TEXT NOT NULL,
    current_price_usd           REAL,
    market_cap_usd              INTEGER,
    market_cap_rank             INTEGER,
    fully_diluted_valuation_usd INTEGER,
    total_volume_24h_usd        INTEGER,
    high_24h_usd                REAL,
    low_24h_usd                 REAL,
    price_change_24h_usd        REAL,
    price_change_pct_1h         REAL,
    price_change_pct_24h        REAL,
    price_change_pct_7d         REAL,
    volatility_score            REAL,
    circulating_supply          REAL,
    total_supply                REAL,
    max_supply                  REAL,
    ath_usd                     REAL,
    ath_change_pct              REAL,
    atl_usd                     REAL,
    last_updated                TEXT,
    ingested_at                 TEXT NOT NULL,
    UNIQUE(coin_id, ingested_at)
)
"""

CREATE_GLOBAL_STATS_SQL = """
CREATE TABLE IF NOT EXISTS global_market_stats (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    total_market_cap_usd        INTEGER,
    total_volume_24h_usd        INTEGER,
    btc_dominance_pct           REAL,
    eth_dominance_pct           REAL,
    active_cryptocurrencies     INTEGER,
    total_exchanges             INTEGER,
    market_cap_change_pct_24h   REAL,
    ingested_at                 TEXT NOT NULL,
    UNIQUE(ingested_at)
)
"""

CREATE_RUN_LOG_SQL = """
CREATE TABLE IF NOT EXISTS pipeline_run_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id          TEXT NOT NULL,
    started_at      TEXT NOT NULL,
    completed_at    TEXT,
    records_loaded  INTEGER DEFAULT 0,
    status          TEXT DEFAULT 'RUNNING',
    error_message   TEXT
)
"""

INDEXES_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_coin_prices_coin_id ON coin_prices(coin_id)",
    "CREATE INDEX IF NOT EXISTS idx_coin_prices_ingested_at ON coin_prices(ingested_at)",
    "CREATE INDEX IF NOT EXISTS idx_global_stats_ingested_at ON global_market_stats(ingested_at)",
]


def bootstrap_schema():
    """Create all tables and indexes if they don't already exist."""
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(CREATE_COIN_PRICES_SQL)
        cursor.execute(CREATE_GLOBAL_STATS_SQL)
        cursor.execute(CREATE_RUN_LOG_SQL)
        for idx_sql in INDEXES_SQL:
            cursor.execute(idx_sql)
    logger.info("Schema bootstrap complete")


# ─── Load functions ────────────────────────────────────────────────────────────

def load_coin_prices(records: list[dict]) -> int:
    """
    Upsert a batch of transformed coin price records.

    Args:
        records: Output of transform.transform_market_data()

    Returns:
        Number of rows successfully inserted/updated
    """
    if not records:
        logger.warning("No coin price records to load")
        return 0

    cols = list(records[0].keys())
    placeholders = ", ".join(["?" if DB_BACKEND == "sqlite" else f"${i+1}" for i, _ in enumerate(cols)])
    col_names = ", ".join(cols)

    if DB_BACKEND == "sqlite":
        sql = f"INSERT OR IGNORE INTO coin_prices ({col_names}) VALUES ({placeholders})"
    else:
        update_clause = ", ".join([f"{c}=EXCLUDED.{c}" for c in cols if c not in ("coin_id", "ingested_at")])
        sql = f"""
            INSERT INTO coin_prices ({col_names}) VALUES ({placeholders})
            ON CONFLICT (coin_id, ingested_at) DO UPDATE SET {update_clause}
        """

    with get_connection() as conn:
        cursor = conn.cursor()
        rows = [tuple(r[c] for c in cols) for r in records]
        cursor.executemany(sql, rows)
        loaded = cursor.rowcount if cursor.rowcount >= 0 else len(records)

    logger.info(f"Loaded {loaded} coin price records")
    return loaded


def load_global_stats(stats: dict) -> int:
    """
    Upsert one global market stats record.

    Args:
        stats: Output of transform.transform_global_stats()

    Returns:
        1 on success, 0 on skip
    """
    if not stats:
        return 0

    cols = list(stats.keys())
    placeholders = ", ".join(["?" if DB_BACKEND == "sqlite" else f"${i+1}" for i, _ in enumerate(cols)])
    col_names = ", ".join(cols)

    if DB_BACKEND == "sqlite":
        sql = f"INSERT OR IGNORE INTO global_market_stats ({col_names}) VALUES ({placeholders})"
    else:
        update_clause = ", ".join([f"{c}=EXCLUDED.{c}" for c in cols if c != "ingested_at"])
        sql = f"""
            INSERT INTO global_market_stats ({col_names}) VALUES ({placeholders})
            ON CONFLICT (ingested_at) DO UPDATE SET {update_clause}
        """

    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(sql, tuple(stats[c] for c in cols))
        loaded = cursor.rowcount if cursor.rowcount >= 0 else 1

    logger.info("Loaded global market stats")
    return loaded


def log_run(run_id: str, status: str, records_loaded: int = 0, error: str = None):
    """Write a pipeline run entry to the audit log table."""
    with get_connection() as conn:
        cursor = conn.cursor()
        now = datetime.now(timezone.utc).isoformat()
        cursor.execute(
            """INSERT INTO pipeline_run_log
               (run_id, started_at, completed_at, records_loaded, status, error_message)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (run_id, now, now, records_loaded, status, error)
        )
    logger.info(f"Run log written: {run_id} → {status}")
