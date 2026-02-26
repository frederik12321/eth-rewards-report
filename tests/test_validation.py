"""Tests for /generate input validation via Flask test client."""
import os
import sys
import json
import pytest
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import app, limiter


@pytest.fixture
def client():
    app.config["TESTING"] = True
    limiter.enabled = False
    with app.test_client() as c:
        yield c
    limiter.enabled = True


def _post(client, **overrides):
    """Build a valid /generate payload and apply overrides."""
    data = {
        "validator_or_address": "0x" + "a" * 40,
        "date_from": "2024-01-01",
        "date_to": "2024-12-31",
        "etherscan_api_key": "A" * 34,
        "currency": "EUR",
        "output_format": "xlsx",
    }
    data.update(overrides)
    return client.post("/generate", json=data)


class TestMissingFields:
    def test_missing_validator(self, client):
        resp = _post(client, validator_or_address="")
        assert resp.status_code == 400

    def test_missing_etherscan_key(self, client):
        resp = _post(client, etherscan_api_key="")
        assert resp.status_code == 400

    def test_missing_date_from(self, client):
        resp = _post(client, date_from="")
        assert resp.status_code == 400

    def test_missing_date_to(self, client):
        resp = _post(client, date_to="")
        assert resp.status_code == 400


class TestDateValidation:
    def test_invalid_date_format(self, client):
        resp = _post(client, date_from="01-01-2024")
        assert resp.status_code == 400
        assert "date" in resp.get_json()["error"].lower()

    def test_date_slash_format(self, client):
        resp = _post(client, date_from="2024/01/01")
        assert resp.status_code == 400

    def test_before_beacon_genesis(self, client):
        resp = _post(client, date_from="2019-01-01")
        assert resp.status_code == 400
        assert "genesis" in resp.get_json()["error"].lower() or "beacon" in resp.get_json()["error"].lower()

    def test_start_after_end(self, client):
        resp = _post(client, date_from="2024-12-31", date_to="2024-01-01")
        assert resp.status_code == 400


class TestEtherscanKeyValidation:
    def test_too_short(self, client):
        resp = _post(client, etherscan_api_key="ABC")
        assert resp.status_code == 400

    def test_too_long(self, client):
        resp = _post(client, etherscan_api_key="A" * 40)
        assert resp.status_code == 400

    def test_special_chars(self, client):
        resp = _post(client, etherscan_api_key="A" * 33 + "!")
        assert resp.status_code == 400

    def test_valid_key_format(self, client):
        # Valid format but will fail on actual API call — just checking validation passes
        with patch("app.threading.Thread") as mock_thread:
            mock_thread.return_value.start = lambda: None
            resp = _post(client, etherscan_api_key="A" * 34)
            # Should pass validation (200) or fail later in processing
            assert resp.status_code in (200, 429)


class TestFeeRecipientValidation:
    def test_invalid_format(self, client):
        resp = _post(client, fee_recipient="not_an_address")
        assert resp.status_code == 400

    def test_too_short(self, client):
        resp = _post(client, fee_recipient="0x123")
        assert resp.status_code == 400

    def test_valid_empty(self, client):
        """Fee recipient is optional — empty is valid."""
        with patch("app.threading.Thread") as mock_thread:
            mock_thread.return_value.start = lambda: None
            resp = _post(client, fee_recipient="")
            assert resp.status_code in (200, 429)


class TestCurrencyValidation:
    def test_unsupported_currency(self, client):
        resp = _post(client, currency="XYZ")
        assert resp.status_code == 400
        assert "currency" in resp.get_json()["error"].lower()

    def test_valid_currencies(self, client):
        """All supported currencies should pass validation."""
        for cur in ["EUR", "USD", "GBP", "CHF", "CAD", "AUD", "JPY", "SEK", "NOK", "DKK"]:
            with patch("app.threading.Thread") as mock_thread:
                mock_thread.return_value.start = lambda: None
                resp = _post(client, currency=cur)
                assert resp.status_code in (200, 429), f"Currency {cur} rejected"


class TestOutputFormatValidation:
    def test_invalid_format(self, client):
        resp = _post(client, output_format="pdf")
        assert resp.status_code == 400

    def test_valid_formats(self, client):
        for fmt in ["xlsx", "csv", "both"]:
            with patch("app.threading.Thread") as mock_thread:
                mock_thread.return_value.start = lambda: None
                resp = _post(client, output_format=fmt)
                assert resp.status_code in (200, 429), f"Format {fmt} rejected"


class TestRoutes:
    def test_index_returns_200(self, client):
        resp = client.get("/")
        assert resp.status_code == 200

    def test_health_returns_json(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200
        data = resp.get_json()
        assert "status" in data
        assert data["status"] == "healthy"

    def test_info_returns_200(self, client):
        resp = client.get("/info")
        assert resp.status_code == 200

    def test_terms_returns_200(self, client):
        resp = client.get("/terms")
        assert resp.status_code == 200

    def test_privacy_returns_200(self, client):
        resp = client.get("/privacy")
        assert resp.status_code == 200

    def test_404_on_unknown_route(self, client):
        resp = client.get("/nonexistent")
        assert resp.status_code == 404
