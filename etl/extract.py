"""
extract.py
----------
Pulls raw cryptocurrency market data from the CoinGecko public API.
No API key required for the free tier (up to 30 calls/min).
"""

import requests
import logging
from datetime import datetime
from typing import Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_URL = "https://api.coingecko.com/api/v3"

COINS = [
    "bitcoin", "ethereum", "solana", "cardano",
    "polkadot", "chainlink", "avalanche-2", "uniswap"
]


def fetch_market_data(
    vs_currency: str = "usd",
    coins: list[str] = COINS,
    sparkline: bool = False
) -> Optional[list[dict]]:
    """
    Fetch current market data for a list of coins from CoinGecko.

    Args:
        vs_currency: The target currency for pricing (default: 'usd')
        coins: List of coin IDs to fetch
        sparkline: Whether to include 7-day sparkline data

    Returns:
        List of raw coin dictionaries, or None on failure
    """
    endpoint = f"{BASE_URL}/coins/markets"
    params = {
        "vs_currency": vs_currency,
        "ids": ",".join(coins),
        "order": "market_cap_desc",
        "per_page": len(coins),
        "page": 1,
        "sparkline": str(sparkline).lower(),
        "price_change_percentage": "1h,24h,7d"
    }

    try:
        logger.info(f"Fetching market data for {len(coins)} coins...")
        response = requests.get(endpoint, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
        logger.info(f"Successfully fetched {len(data)} coin records")
        return data

    except requests.exceptions.Timeout:
        logger.error("CoinGecko API request timed out")
        raise
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error from CoinGecko API: {e.response.status_code} - {e.response.text}")
        raise
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
        raise


def fetch_global_stats() -> Optional[dict]:
    """
    Fetch global crypto market statistics (total market cap, BTC dominance, etc.)

    Returns:
        Dictionary of global market stats, or None on failure
    """
    endpoint = f"{BASE_URL}/global"

    try:
        logger.info("Fetching global market statistics...")
        response = requests.get(endpoint, timeout=15)
        response.raise_for_status()
        data = response.json().get("data", {})
        logger.info("Successfully fetched global stats")
        return data

    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch global stats: {e}")
        raise
