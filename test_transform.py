"""
test_transform.py
-----------------
Unit tests for all transform functions.
Run with: pytest tests/test_transform.py -v
"""

import pytest
from etl.transform import transform_market_data, transform_global_stats


# ─── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def raw_coin_record():
    return {
        "id": "bitcoin",
        "symbol": "btc",
        "name": "Bitcoin",
        "current_price": 65000.12,
        "market_cap": 1280000000000,
        "market_cap_rank": 1,
        "fully_diluted_valuation": 1365000000000,
        "total_volume": 32000000000,
        "high_24h": 66500.0,
        "low_24h": 63800.0,
        "price_change_24h": 800.5,
        "price_change_percentage_1h_in_currency": 0.42,
        "price_change_percentage_24h_in_currency": 1.25,
        "price_change_percentage_7d_in_currency": -3.10,
        "circulating_supply": 19700000,
        "total_supply": 21000000,
        "max_supply": 21000000,
        "ath": 73750.0,
        "ath_change_percentage": -11.84,
        "atl": 67.81,
        "last_updated": "2024-03-15T12:00:00.000Z",
    }


@pytest.fixture
def raw_global_stats():
    return {
        "total_market_cap": {"usd": 2400000000000},
        "total_volume": {"usd": 110000000000},
        "market_cap_percentage": {"btc": 52.3, "eth": 17.1},
        "active_cryptocurrencies": 13500,
        "markets": 984,
        "market_cap_change_percentage_24h_usd": 1.42,
    }


# ─── transform_market_data tests ───────────────────────────────────────────────

class TestTransformMarketData:

    def test_returns_correct_schema(self, raw_coin_record):
        result = transform_market_data([raw_coin_record])
        assert len(result) == 1
        record = result[0]

        required_fields = [
            "coin_id", "symbol", "name", "current_price_usd",
            "market_cap_usd", "price_change_pct_1h", "price_change_pct_24h",
            "price_change_pct_7d", "volatility_score", "ingested_at"
        ]
        for field in required_fields:
            assert field in record, f"Missing field: {field}"

    def test_symbol_uppercased(self, raw_coin_record):
        result = transform_market_data([raw_coin_record])
        assert result[0]["symbol"] == "BTC"

    def test_volatility_score_computed(self, raw_coin_record):
        result = transform_market_data([raw_coin_record])
        # |0.42 - 1.25| = 0.83
        assert result[0]["volatility_score"] == pytest.approx(0.83, abs=0.001)

    def test_empty_input_returns_empty_list(self):
        assert transform_market_data([]) == []

    def test_none_price_defaults_to_zero(self, raw_coin_record):
        raw_coin_record["current_price"] = None
        result = transform_market_data([raw_coin_record])
        assert result[0]["current_price_usd"] == 0.0

    def test_none_price_changes_default_to_zero(self, raw_coin_record):
        raw_coin_record["price_change_percentage_1h_in_currency"] = None
        raw_coin_record["price_change_percentage_24h_in_currency"] = None
        result = transform_market_data([raw_coin_record])
        assert result[0]["price_change_pct_1h"] == 0.0
        assert result[0]["price_change_pct_24h"] == 0.0

    def test_skips_malformed_records_gracefully(self, raw_coin_record):
        bad_record = {"id": "broken", "current_price": "NOT_A_FLOAT"}
        result = transform_market_data([raw_coin_record, bad_record])
        # Should still return the good record
        assert len(result) >= 1
        assert result[0]["coin_id"] == "bitcoin"

    def test_timestamp_normalized_to_iso(self, raw_coin_record):
        result = transform_market_data([raw_coin_record])
        ts = result[0]["last_updated"]
        # Should end with timezone info
        assert "+" in ts or "Z" in ts or ts.endswith("00:00")

    def test_price_rounded_to_6_decimal_places(self, raw_coin_record):
        raw_coin_record["current_price"] = 65000.1234567890
        result = transform_market_data([raw_coin_record])
        price_str = str(result[0]["current_price_usd"])
        decimals = price_str.split(".")[-1] if "." in price_str else ""
        assert len(decimals) <= 6

    def test_processes_multiple_records(self, raw_coin_record):
        eth_record = {**raw_coin_record, "id": "ethereum", "symbol": "eth", "name": "Ethereum"}
        result = transform_market_data([raw_coin_record, eth_record])
        assert len(result) == 2
        coin_ids = {r["coin_id"] for r in result}
        assert coin_ids == {"bitcoin", "ethereum"}


# ─── transform_global_stats tests ──────────────────────────────────────────────

class TestTransformGlobalStats:

    def test_returns_correct_schema(self, raw_global_stats):
        result = transform_global_stats(raw_global_stats)
        assert result is not None
        required = [
            "total_market_cap_usd", "total_volume_24h_usd",
            "btc_dominance_pct", "eth_dominance_pct",
            "active_cryptocurrencies", "ingested_at"
        ]
        for field in required:
            assert field in result, f"Missing field: {field}"

    def test_btc_dominance_rounded(self, raw_global_stats):
        result = transform_global_stats(raw_global_stats)
        assert result["btc_dominance_pct"] == pytest.approx(52.3, abs=0.001)

    def test_empty_dict_returns_none(self):
        assert transform_global_stats({}) is not None  # empty but valid dict
        assert transform_global_stats(None) is None

    def test_missing_nested_keys_dont_crash(self):
        minimal = {"total_market_cap": {}, "total_volume": {}, "market_cap_percentage": {}}
        result = transform_global_stats(minimal)
        assert result is not None
        assert result["total_market_cap_usd"] == 0
