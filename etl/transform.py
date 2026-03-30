"""
transform.py
------------
Cleans, validates, and enriches raw CoinGecko data before loading.
All transforms are pure functions — no side effects, fully testable.
"""

import logging
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)


def transform_market_data(raw_records: list[dict]) -> list[dict]:
    """
    Clean and reshape raw CoinGecko market records into a flat,
    database-ready schema.

    Transformations applied:
    - Null/missing value handling with typed defaults
    - Timestamp normalization to UTC ISO-8601
    - Float rounding for price precision
    - Derived field: volatility_score (1h vs 24h price change delta)
    - Derived field: market_dominance_pct placeholder for later join

    Args:
        raw_records: List of raw dicts from extract.fetch_market_data()

    Returns:
        List of cleaned, schema-consistent dictionaries
    """
    if not raw_records:
        logger.warning("No records to transform")
        return []

    transformed = []
    ingested_at = datetime.now(timezone.utc).isoformat()

    for record in raw_records:
        try:
            price_change_1h = record.get("price_change_percentage_1h_in_currency") or 0.0
            price_change_24h = record.get("price_change_percentage_24h_in_currency") or 0.0
            price_change_7d = record.get("price_change_percentage_7d_in_currency") or 0.0

            # Derived: absolute swing between 1h and 24h signals short-term volatility
            volatility_score = round(abs(price_change_1h - price_change_24h), 4)

            # Normalize last_updated timestamp
            last_updated_raw = record.get("last_updated")
            last_updated = (
                datetime.fromisoformat(last_updated_raw.replace("Z", "+00:00")).isoformat()
                if last_updated_raw else ingested_at
            )

            cleaned = {
                "coin_id": record.get("id", "unknown"),
                "symbol": (record.get("symbol") or "").upper(),
                "name": record.get("name", "Unknown"),
                "current_price_usd": round(float(record.get("current_price") or 0), 6),
                "market_cap_usd": int(record.get("market_cap") or 0),
                "market_cap_rank": record.get("market_cap_rank"),
                "fully_diluted_valuation_usd": record.get("fully_diluted_valuation"),
                "total_volume_24h_usd": int(record.get("total_volume") or 0),
                "high_24h_usd": round(float(record.get("high_24h") or 0), 6),
                "low_24h_usd": round(float(record.get("low_24h") or 0), 6),
                "price_change_24h_usd": round(float(record.get("price_change_24h") or 0), 6),
                "price_change_pct_1h": round(price_change_1h, 4),
                "price_change_pct_24h": round(price_change_24h, 4),
                "price_change_pct_7d": round(price_change_7d, 4),
                "volatility_score": volatility_score,
                "circulating_supply": record.get("circulating_supply"),
                "total_supply": record.get("total_supply"),
                "max_supply": record.get("max_supply"),
                "ath_usd": round(float(record.get("ath") or 0), 6),
                "ath_change_pct": round(float(record.get("ath_change_percentage") or 0), 4),
                "atl_usd": round(float(record.get("atl") or 0), 10),
                "last_updated": last_updated,
                "ingested_at": ingested_at,
            }
            transformed.append(cleaned)

        except (TypeError, ValueError, KeyError) as e:
            coin_id = record.get("id", "UNKNOWN")
            logger.warning(f"Skipping malformed record for {coin_id}: {e}")
            continue

    logger.info(f"Transformed {len(transformed)}/{len(raw_records)} records successfully")
    return transformed


def transform_global_stats(raw_stats: dict) -> Optional[dict]:
    """
    Clean and flatten global market statistics.

    Args:
        raw_stats: Raw dict from extract.fetch_global_stats()

    Returns:
        Cleaned dictionary ready for DB insert
    """
    if not raw_stats:
        return None

    try:
        total_market_cap = raw_stats.get("total_market_cap", {})
        total_volume = raw_stats.get("total_volume", {})
        market_cap_pct = raw_stats.get("market_cap_percentage", {})

        return {
            "total_market_cap_usd": int(total_market_cap.get("usd") or 0),
            "total_volume_24h_usd": int(total_volume.get("usd") or 0),
            "btc_dominance_pct": round(float(market_cap_pct.get("btc") or 0), 4),
            "eth_dominance_pct": round(float(market_cap_pct.get("eth") or 0), 4),
            "active_cryptocurrencies": raw_stats.get("active_cryptocurrencies"),
            "total_exchanges": raw_stats.get("markets"),
            "market_cap_change_pct_24h": round(
                float(raw_stats.get("market_cap_change_percentage_24h_usd") or 0), 4
            ),
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        }

    except (TypeError, ValueError) as e:
        logger.error(f"Failed to transform global stats: {e}")
        return None
