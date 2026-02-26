"""Tests for app.py helper functions."""
import os
import sys
import pytest
from decimal import Decimal
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import _sanitize_error, _build_validator_info, _build_summary


# ---------------------------------------------------------------------------
# _sanitize_error
# ---------------------------------------------------------------------------

class TestSanitizeError:
    def test_rate_limit(self):
        result = _sanitize_error(RuntimeError("rate limit exceeded"))
        assert "rate limit" in result.lower()

    def test_rate_limit_case_insensitive(self):
        result = _sanitize_error(RuntimeError("RATE LIMIT EXCEEDED"))
        assert "rate limit" in result.lower()

    def test_api_key(self):
        result = _sanitize_error(RuntimeError("Invalid API key provided"))
        assert "API key" in result

    def test_validator_not_found_passes_through(self):
        msg = "Validator '123' not found on beacon chain"
        result = _sanitize_error(RuntimeError(msg))
        assert result == msg

    def test_bls_withdrawal_passes_through(self):
        msg = "BLS withdrawal credentials detected"
        result = _sanitize_error(RuntimeError(msg))
        assert result == msg

    def test_no_events_passes_through(self):
        msg = "No events found for this address"
        result = _sanitize_error(RuntimeError(msg))
        assert result == msg

    def test_connection_error(self):
        result = _sanitize_error(RuntimeError("Connection refused"))
        assert "connect" in result.lower()

    def test_timeout_error(self):
        result = _sanitize_error(RuntimeError("Request timeout after 30s"))
        assert "connect" in result.lower()

    def test_price_data_error(self):
        result = _sanitize_error(RuntimeError("Failed to fetch price data"))
        assert "price data" in result.lower()

    def test_cryptocompare_error(self):
        result = _sanitize_error(RuntimeError("CryptoCompare returned error"))
        assert "price data" in result.lower()

    def test_unknown_error_generic(self):
        result = _sanitize_error(RuntimeError("Segfault in module xyz"))
        assert "unexpected error" in result.lower()
        assert "xyz" not in result  # internal details stripped

    def test_does_not_leak_traceback(self):
        result = _sanitize_error(RuntimeError("File /app/staking_tax_report.py line 42"))
        assert "/app/" not in result
        assert "line 42" not in result


# ---------------------------------------------------------------------------
# _build_validator_info
# ---------------------------------------------------------------------------

class TestBuildValidatorInfo:
    def _make_account(self, validators):
        return {
            "name": "test_account",
            "_validator_details": [
                {
                    "validator_index": v["index"],
                    "credential_type": v.get("cred", "0x01"),
                    "status": v.get("status", "active_ongoing"),
                    "balance_eth": v.get("balance", Decimal("32")),
                    "effective_balance_eth": v.get("eff_balance", Decimal("32")),
                }
                for v in validators
            ],
        }

    def test_basic_withdrawal(self):
        account = self._make_account([{"index": 100}])
        events = [
            {"validator": 100, "type": "withdrawal", "amount_eth": Decimal("0.5")},
        ]
        result = _build_validator_info([account], events)
        assert result is not None
        assert len(result) == 1
        assert result[0]["index"] == 100
        assert result[0]["withdrawn_eth"] == Decimal("0.5")
        assert result[0]["withdrawn_income_eth"] == Decimal("0.5")

    def test_principal_vs_income(self):
        account = self._make_account([{"index": 100}])
        events = [
            {"validator": 100, "type": "withdrawal", "amount_eth": Decimal("10")},
            {"validator": 100, "type": "withdrawal_principal", "amount_eth": Decimal("22")},
        ]
        result = _build_validator_info([account], events)
        assert result[0]["withdrawn_eth"] == Decimal("32")  # total
        assert result[0]["withdrawn_income_eth"] == Decimal("10")  # income only

    def test_block_rewards_not_counted(self):
        account = self._make_account([{"index": 100}])
        events = [
            {"validator": 100, "type": "block_reward", "amount_eth": Decimal("0.1")},
        ]
        result = _build_validator_info([account], events)
        assert result[0]["withdrawn_eth"] == Decimal("0")

    def test_no_validator_details_returns_none(self):
        account = {"name": "empty"}
        result = _build_validator_info([account], [])
        assert result is None

    def test_events_without_validator_skipped(self):
        account = self._make_account([{"index": 100}])
        events = [
            {"type": "withdrawal", "amount_eth": Decimal("1")},  # no validator key
        ]
        result = _build_validator_info([account], events)
        assert result[0]["withdrawn_eth"] == Decimal("0")

    def test_multiple_validators(self):
        account = self._make_account([{"index": 100}, {"index": 200}])
        events = [
            {"validator": 100, "type": "withdrawal", "amount_eth": Decimal("1")},
            {"validator": 200, "type": "withdrawal", "amount_eth": Decimal("2")},
        ]
        result = _build_validator_info([account], events)
        assert len(result) == 2
        v100 = next(v for v in result if v["index"] == 100)
        v200 = next(v for v in result if v["index"] == 200)
        assert v100["withdrawn_eth"] == Decimal("1")
        assert v200["withdrawn_eth"] == Decimal("2")

    def test_default_credential_type(self):
        account = self._make_account([{"index": 100}])
        result = _build_validator_info([account], [])
        assert result[0]["credential_type"] == "0x01"


# ---------------------------------------------------------------------------
# _build_summary
# ---------------------------------------------------------------------------

class TestBuildSummary:
    def _make_event(self, type_, amount, fiat_price, dt):
        return {
            "type": type_,
            "amount_eth": Decimal(str(amount)),
            "value_fiat": Decimal(str(amount)) * Decimal(str(fiat_price)),
            "price_fiat": Decimal(str(fiat_price)),
            "datetime": dt,
        }

    def test_basic(self):
        events = [
            self._make_event("withdrawal", 1.0, 500, datetime(2024, 1, 15, tzinfo=timezone.utc)),
            self._make_event("block_reward", 0.1, 500, datetime(2024, 1, 20, tzinfo=timezone.utc)),
        ]
        s = _build_summary(events, None, "EUR")
        assert s["currency"] == "EUR"
        assert abs(s["total_income_eth"] - 1.1) < 0.001
        assert abs(s["withdrawals_eth"] - 1.0) < 0.001
        assert abs(s["block_rewards_eth"] - 0.1) < 0.001
        assert s["event_count"] == 2
        assert "principal_eth" not in s

    def test_monthly_grouping(self):
        events = [
            self._make_event("withdrawal", 1.0, 500, datetime(2024, 1, 15, tzinfo=timezone.utc)),
            self._make_event("withdrawal", 2.0, 600, datetime(2024, 2, 15, tzinfo=timezone.utc)),
        ]
        s = _build_summary(events, None, "EUR")
        assert len(s["months"]) == 2
        assert s["months"][0]["month"] == "2024-01"
        assert s["months"][1]["month"] == "2024-02"

    def test_with_principal(self):
        events = [
            self._make_event("withdrawal", 1.0, 500, datetime(2024, 1, 15, tzinfo=timezone.utc)),
            self._make_event("withdrawal_principal", 22.0, 500, datetime(2024, 1, 16, tzinfo=timezone.utc)),
        ]
        s = _build_summary(events, None, "EUR")
        assert "principal_eth" in s
        assert abs(s["principal_eth"] - 22.0) < 0.001
        # Principal should not be in total_income
        assert abs(s["total_income_eth"] - 1.0) < 0.001

    def test_with_validators(self):
        events = [
            self._make_event("withdrawal", 1.0, 500, datetime(2024, 1, 15, tzinfo=timezone.utc)),
        ]
        validator_info = [{
            "index": 100,
            "name": "v1",
            "credential_type": "0x02",
            "status": "active_ongoing",
            "balance_eth": Decimal("40"),
            "effective_balance_eth": Decimal("32"),
            "withdrawn_eth": Decimal("1"),
            "withdrawn_income_eth": Decimal("1"),
        }]
        s = _build_summary(events, validator_info, "EUR")
        assert "validators" in s
        assert len(s["validators"]) == 1
        assert s["validators"][0]["type"] == "Compounding"
        assert s["validators"][0]["index"] == 100

    def test_distributing_type(self):
        events = [
            self._make_event("withdrawal", 1.0, 500, datetime(2024, 1, 15, tzinfo=timezone.utc)),
        ]
        validator_info = [{
            "index": 100,
            "name": "v1",
            "credential_type": "0x01",
            "status": "active_ongoing",
            "balance_eth": Decimal("32"),
            "effective_balance_eth": Decimal("32"),
            "withdrawn_eth": Decimal("1"),
            "withdrawn_income_eth": Decimal("1"),
        }]
        s = _build_summary(events, validator_info, "EUR")
        assert s["validators"][0]["type"] == "Distributing"

    def test_no_validators_no_key(self):
        events = [
            self._make_event("withdrawal", 1.0, 500, datetime(2024, 1, 15, tzinfo=timezone.utc)),
        ]
        s = _build_summary(events, None, "EUR")
        assert "validators" not in s

    def test_avg_price_calculated(self):
        events = [
            self._make_event("withdrawal", 1.0, 400, datetime(2024, 1, 10, tzinfo=timezone.utc)),
            self._make_event("withdrawal", 1.0, 600, datetime(2024, 1, 20, tzinfo=timezone.utc)),
        ]
        s = _build_summary(events, None, "EUR")
        assert abs(s["months"][0]["avg_price"] - 500.0) < 0.001

    def test_empty_events(self):
        s = _build_summary([], None, "USD")
        assert s["total_income_eth"] == 0
        assert s["total_income_fiat"] == 0
        assert s["event_count"] == 0
        assert s["months"] == []
