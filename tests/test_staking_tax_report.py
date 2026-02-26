"""Tests for core staking_tax_report.py functions."""
import os
import sys
import pytest
from decimal import Decimal
from datetime import datetime, timezone

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from staking_tax_report import (
    slot_to_timestamp,
    parse_validator_input,
    find_nearest_price,
    _prepare_events,
    PriceCache,
    GENESIS_TIMESTAMP,
    SECONDS_PER_SLOT,
    WEI_PER_ETH,
    GWEI_PER_ETH,
    KNOWN_BUILDER_ADDRESSES,
)


# ---------------------------------------------------------------------------
# slot_to_timestamp
# ---------------------------------------------------------------------------

class TestSlotToTimestamp:
    def test_slot_zero(self):
        assert slot_to_timestamp(0) == GENESIS_TIMESTAMP

    def test_slot_one(self):
        assert slot_to_timestamp(1) == GENESIS_TIMESTAMP + SECONDS_PER_SLOT

    def test_slot_hundred(self):
        assert slot_to_timestamp(100) == GENESIS_TIMESTAMP + 100 * SECONDS_PER_SLOT

    def test_large_slot(self):
        slot = 10_000_000
        expected = GENESIS_TIMESTAMP + slot * SECONDS_PER_SLOT
        assert slot_to_timestamp(slot) == expected


# ---------------------------------------------------------------------------
# parse_validator_input
# ---------------------------------------------------------------------------

class TestParseValidatorInput:
    def test_single_index(self):
        result = parse_validator_input("652648")
        assert result == {"type": "validator_index", "value": "652648"}

    def test_single_index_with_whitespace(self):
        result = parse_validator_input("  652648  ")
        assert result == {"type": "validator_index", "value": "652648"}

    def test_multiple_indices(self):
        result = parse_validator_input("1, 2, 3")
        assert result == {"type": "validator_indices", "value": ["1", "2", "3"]}

    def test_multiple_indices_no_spaces(self):
        result = parse_validator_input("100,200,300")
        assert result == {"type": "validator_indices", "value": ["100", "200", "300"]}

    def test_address_lowercase(self):
        addr = "0x" + "a" * 40
        result = parse_validator_input(addr)
        assert result == {"type": "withdrawal_address", "value": addr}

    def test_address_uppercase(self):
        addr = "0x" + "A" * 40
        result = parse_validator_input(addr)
        assert result == {"type": "withdrawal_address", "value": addr}

    def test_address_mixed_case(self):
        addr = "0x4a748fd39539e615c863ae8d10b3ebefcc0a6c38"
        result = parse_validator_input(addr)
        assert result == {"type": "withdrawal_address", "value": addr}

    def test_pubkey_with_0x(self):
        pubkey = "0x" + "b" * 96
        result = parse_validator_input(pubkey)
        assert result == {"type": "validator_pubkey", "value": pubkey}

    def test_pubkey_without_0x(self):
        raw = "c" * 96
        result = parse_validator_input(raw)
        assert result == {"type": "validator_pubkey", "value": "0x" + raw}

    def test_empty_string_raises(self):
        with pytest.raises(ValueError, match="Empty input"):
            parse_validator_input("")

    def test_whitespace_only_raises(self):
        with pytest.raises(ValueError, match="Empty input"):
            parse_validator_input("   ")

    def test_non_digit_in_comma_list_raises(self):
        with pytest.raises(ValueError, match="Invalid validator index"):
            parse_validator_input("123, abc, 456")

    def test_wrong_length_hex_raises(self):
        with pytest.raises(ValueError, match="Unrecognized input"):
            parse_validator_input("0x" + "a" * 38)  # 38 instead of 40

    def test_wrong_length_pubkey_raises(self):
        with pytest.raises(ValueError, match="Unrecognized input"):
            parse_validator_input("0x" + "a" * 94)  # 94 instead of 96

    def test_random_text_raises(self):
        with pytest.raises(ValueError, match="Unrecognized input"):
            parse_validator_input("not_a_valid_input")

    def test_0X_prefix(self):
        addr = "0X" + "a" * 40
        result = parse_validator_input(addr)
        assert result == {"type": "withdrawal_address", "value": addr}


# ---------------------------------------------------------------------------
# find_nearest_price
# ---------------------------------------------------------------------------

class TestFindNearestPrice:
    def test_empty_list(self):
        assert find_nearest_price([], 1000) == Decimal(0)

    def test_single_price(self):
        prices = [(1000, Decimal("500"))]
        assert find_nearest_price(prices, 1500) == Decimal("500")

    def test_exact_match(self):
        prices = [(1000, Decimal("100")), (2000, Decimal("200"))]
        assert find_nearest_price(prices, 1000) == Decimal("100")

    def test_before_all(self):
        prices = [(1000, Decimal("100")), (2000, Decimal("200"))]
        assert find_nearest_price(prices, 500) == Decimal("100")

    def test_after_all(self):
        prices = [(1000, Decimal("100")), (2000, Decimal("200"))]
        assert find_nearest_price(prices, 3000) == Decimal("200")

    def test_closer_to_earlier(self):
        prices = [(1000, Decimal("100")), (2000, Decimal("200"))]
        assert find_nearest_price(prices, 1400) == Decimal("100")

    def test_closer_to_later(self):
        prices = [(1000, Decimal("100")), (2000, Decimal("200"))]
        assert find_nearest_price(prices, 1700) == Decimal("200")

    def test_exact_tie_favors_earlier(self):
        prices = [(1000, Decimal("100")), (2000, Decimal("200"))]
        assert find_nearest_price(prices, 1500) == Decimal("100")

    def test_three_prices_middle(self):
        prices = [
            (1000, Decimal("100")),
            (2000, Decimal("200")),
            (3000, Decimal("300")),
        ]
        assert find_nearest_price(prices, 2100) == Decimal("200")

    def test_many_prices_binary_search(self):
        prices = [(i * 3600, Decimal(str(i))) for i in range(100)]
        # Closest to timestamp 50*3600 + 100 is index 50
        assert find_nearest_price(prices, 50 * 3600 + 100) == Decimal("50")


# ---------------------------------------------------------------------------
# _prepare_events
# ---------------------------------------------------------------------------

class TestPrepareEvents:
    def test_basic_enrichment(self):
        events = [{"timestamp": 1606824023, "amount_eth": Decimal("1.5")}]
        prices = [(1606824023, Decimal("500"))]
        _prepare_events(events, prices)
        assert events[0]["price_fiat"] == Decimal("500")
        assert events[0]["value_fiat"] == Decimal("750")
        assert events[0]["datetime"] == datetime(2020, 12, 1, 12, 0, 23, tzinfo=timezone.utc)

    def test_zero_amount(self):
        events = [{"timestamp": 1606824023, "amount_eth": Decimal("0")}]
        prices = [(1606824023, Decimal("500"))]
        _prepare_events(events, prices)
        assert events[0]["value_fiat"] == Decimal("0")

    def test_multiple_events(self):
        events = [
            {"timestamp": 2000, "amount_eth": Decimal("1")},
            {"timestamp": 1000, "amount_eth": Decimal("2")},
        ]
        prices = [(1000, Decimal("100")), (2000, Decimal("200"))]
        _prepare_events(events, prices)
        # Both events should have price data
        for e in events:
            assert "price_fiat" in e
            assert "value_fiat" in e
            assert "datetime" in e

    def test_empty_prices(self):
        events = [{"timestamp": 1000, "amount_eth": Decimal("1")}]
        _prepare_events(events, [])
        assert events[0]["price_fiat"] == Decimal("0")
        assert events[0]["value_fiat"] == Decimal("0")


# ---------------------------------------------------------------------------
# PriceCache
# ---------------------------------------------------------------------------

class TestPriceCache:
    def test_store_and_get(self, tmp_path):
        db = str(tmp_path / "test_cache.db")
        cache = PriceCache(db)
        prices = [(1000, Decimal("100")), (2000, Decimal("200")), (3000, Decimal("300"))]
        cache.store("EUR", prices)
        cached = cache.get_cached("EUR", 1000, 3000)
        assert len(cached) == 3
        assert cached[0] == (1000, Decimal("100"))
        assert cached[2] == (3000, Decimal("300"))
        cache.close()

    def test_get_empty(self, tmp_path):
        db = str(tmp_path / "test_cache.db")
        cache = PriceCache(db)
        cached = cache.get_cached("EUR", 1000, 3000)
        assert cached == []
        cache.close()

    def test_find_gaps_no_cache(self, tmp_path):
        db = str(tmp_path / "test_cache.db")
        cache = PriceCache(db)
        gaps = cache.find_gaps("EUR", 1000, 50000)
        assert gaps == [(1000, 50000)]
        cache.close()

    def test_find_gaps_full_cache(self, tmp_path):
        db = str(tmp_path / "test_cache.db")
        cache = PriceCache(db)
        prices = [(t, Decimal("100")) for t in range(1000, 50001, 3600)]
        cache.store("EUR", prices)
        gaps = cache.find_gaps("EUR", 1000, 50000)
        assert gaps == []
        cache.close()

    def test_find_gaps_partial_cache(self, tmp_path):
        db = str(tmp_path / "test_cache.db")
        cache = PriceCache(db)
        # Cache from 20000 to 40000
        prices = [(t, Decimal("100")) for t in range(20000, 40001, 3600)]
        cache.store("EUR", prices)
        gaps = cache.find_gaps("EUR", 10000, 50000)
        # Should find gaps before and after
        assert len(gaps) >= 1
        cache.close()

    def test_different_currencies_isolated(self, tmp_path):
        db = str(tmp_path / "test_cache.db")
        cache = PriceCache(db)
        cache.store("EUR", [(1000, Decimal("100"))])
        cache.store("USD", [(1000, Decimal("120"))])
        eur = cache.get_cached("EUR", 1000, 1000)
        usd = cache.get_cached("USD", 1000, 1000)
        assert eur[0][1] == Decimal("100")
        assert usd[0][1] == Decimal("120")
        cache.close()

    def test_duplicate_insert_ignored(self, tmp_path):
        db = str(tmp_path / "test_cache.db")
        cache = PriceCache(db)
        cache.store("EUR", [(1000, Decimal("100"))])
        cache.store("EUR", [(1000, Decimal("999"))])  # duplicate
        cached = cache.get_cached("EUR", 1000, 1000)
        assert len(cached) == 1
        assert cached[0][1] == Decimal("100")  # original preserved
        cache.close()


# ---------------------------------------------------------------------------
# Constants sanity checks
# ---------------------------------------------------------------------------

class TestConstants:
    def test_genesis_timestamp(self):
        # Dec 1, 2020 12:00:23 UTC
        dt = datetime.fromtimestamp(GENESIS_TIMESTAMP, tz=timezone.utc)
        assert dt.year == 2020
        assert dt.month == 12
        assert dt.day == 1

    def test_wei_per_eth(self):
        assert WEI_PER_ETH == Decimal(10**18)

    def test_gwei_per_eth(self):
        assert GWEI_PER_ETH == Decimal(10**9)

    def test_known_builders_not_empty(self):
        assert len(KNOWN_BUILDER_ADDRESSES) > 40

    def test_known_builders_lowercase(self):
        for addr in KNOWN_BUILDER_ADDRESSES:
            assert addr == addr.lower()
            assert addr.startswith("0x")
