#!/usr/bin/env python3
"""Web UI for the ETH Rewards Report generator.

Hardened for public deployment (Railway) using NIST CSF principles:
- Rate limiting and concurrency control (PROTECT)
- Thread-safe logging via callback pattern (PROTECT)
- Error sanitization — no tracebacks to clients (PROTECT)
- HTTPS enforcement and HSTS (PROTECT)
- Nonce-based CSP for scripts; unsafe-inline for styles (PROTECT)
- Health check endpoint (DETECT)
- SSE heartbeat for proxy keepalive (DETECT)
- Structured logging with request IDs (DETECT)
- Periodic job cleanup (RECOVER)
"""

import io
import json
import logging
import os
import re
import secrets
import tempfile
import threading
import time
import uuid
import zipfile
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal

from flask import Flask, render_template, request, jsonify, Response, redirect, has_request_context
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

from staking_tax_report import (
    EtherscanClient,
    CryptoCompareClient,
    BeaconClient,
    SUPPORTED_CURRENCIES,
    gather_events,
    parse_validator_input,
    write_excel_bytes,
    write_csv_bytes,
    _prepare_events,
)

# ---------------------------------------------------------------------------
# Logging setup (structured, with request ID support)
# ---------------------------------------------------------------------------

class _RequestIdFilter(logging.Filter):
    def filter(self, record):
        if has_request_context():
            record.request_id = getattr(request, 'request_id', '-')
        else:
            record.request_id = '-'
        return True

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(request_id)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("eth_rewards")
logger.addFilter(_RequestIdFilter())

# Suppress werkzeug request logging (avoids logging addresses/API keys in URLs)
logging.getLogger("werkzeug").setLevel(logging.ERROR)

# ---------------------------------------------------------------------------
# App configuration
# ---------------------------------------------------------------------------

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 1 * 1024 * 1024  # 1 MB max request size

# Environment-based config
_MAX_CONCURRENT_JOBS = int(os.environ.get("MAX_CONCURRENT_JOBS", "5"))
_PRICE_CACHE_PATH = os.environ.get("PRICE_CACHE_PATH", "price_cache.db")
_STATS_PATH = os.environ.get("STATS_PATH", os.path.join(os.path.dirname(__file__), "stats.json"))

# Rate limiter (in-memory for single-worker Railway deployment)
limiter = Limiter(
    get_remote_address,
    app=app,
    default_limits=["60 per minute"],
    storage_uri="memory://",
)

# Beacon chain genesis: Dec 1, 2020
BEACON_GENESIS = datetime(2020, 12, 1, tzinfo=timezone.utc)
ETH_ADDRESS_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")
ETHERSCAN_KEY_RE = re.compile(r"^[a-zA-Z0-9]{34}$")

# ---------------------------------------------------------------------------
# Job store (in-memory, single worker)
# ---------------------------------------------------------------------------

_jobs = {}
_jobs_lock = threading.Lock()
_job_semaphore = threading.Semaphore(_MAX_CONCURRENT_JOBS)


def _cleanup_old_jobs():
    """Remove jobs older than 10 minutes (safety net for undownloaded reports)."""
    now = time.time()
    with _jobs_lock:
        expired = [jid for jid, j in _jobs.items() if now - j["created"] > 600]
        for jid in expired:
            del _jobs[jid]


def _start_cleanup_scheduler():
    """Run job cleanup every 60 seconds in a background daemon thread."""
    def _loop():
        while True:
            time.sleep(60)
            _cleanup_old_jobs()
    t = threading.Thread(target=_loop, daemon=True)
    t.start()

_start_cleanup_scheduler()


def _log_to_job(job, message):
    """Thread-safe log callback: appends message to job's log list."""
    with job["lock"]:
        job["logs"].append(str(message).rstrip())


# ---------------------------------------------------------------------------
# Error sanitization — never expose tracebacks to clients
# ---------------------------------------------------------------------------

def _sanitize_error(exc):
    """Convert exception to a user-safe error message without internal details."""
    msg = str(exc).lower()
    if "rate limit" in msg:
        return "Etherscan rate limit exceeded. Please wait a moment and try again."
    if "api key" in msg:
        return "Etherscan API key error. Please verify your API key is correct."
    if "not found" in msg and "validator" in msg:
        return str(exc)  # Validator-not-found messages are user-facing by design
    if "bls withdrawal" in msg:
        return str(exc)  # BLS credential messages are user-facing
    if "no events found" in msg:
        return str(exc)
    if "connection" in msg or "timeout" in msg:
        return "Could not connect to an external API. Please try again later."
    if "price data" in msg or "cryptocompare" in msg:
        return "Could not fetch price data. Please try again later."
    return "An unexpected error occurred while generating your report. Please try again."


# ---------------------------------------------------------------------------
# Analytics: simple privacy-respecting counters (no PII stored)
# ---------------------------------------------------------------------------

_stats_lock = threading.Lock()
_stats = {"page_views": 0, "reports_generated": 0, "reports_completed": 0}


def _load_stats():
    """Load stats from disk if available."""
    if os.path.exists(_STATS_PATH):
        try:
            with open(_STATS_PATH) as f:
                saved = json.load(f)
                _stats.update(saved)
        except Exception:
            pass


def _save_stats():
    """Persist stats to disk atomically (safe for concurrent writes)."""
    try:
        fd, tmp_path = tempfile.mkstemp(dir=os.path.dirname(_STATS_PATH) or ".", suffix=".tmp")
        with os.fdopen(fd, "w") as f:
            json.dump(_stats, f)
        os.replace(tmp_path, _STATS_PATH)
    except Exception:
        try:
            os.unlink(tmp_path)
        except Exception:
            pass


_load_stats()


def _inc_stat(key):
    with _stats_lock:
        _stats[key] = _stats.get(key, 0) + 1
        _save_stats()


# ---------------------------------------------------------------------------
# Request hooks: HTTPS enforcement, CSP nonces, request IDs
# ---------------------------------------------------------------------------

@app.before_request
def _before_request():
    """Assign request ID, CSP nonce, and enforce HTTPS."""
    request.request_id = uuid.uuid4().hex[:8]
    request.csp_nonce = secrets.token_urlsafe(16)

    # HTTPS enforcement when behind a TLS-terminating proxy (Railway)
    if not app.debug and request.headers.get("X-Forwarded-Proto", "https") != "https":
        url = request.url.replace("http://", "https://", 1)
        return redirect(url, code=301)


@app.after_request
def set_security_headers(response):
    nonce = getattr(request, 'csp_nonce', '')
    csp = (
        f"default-src 'self'; "
        f"script-src 'self' 'nonce-{nonce}'; "
        f"style-src 'self' 'unsafe-inline'; "
        f"connect-src 'self'"
    )
    response.headers["Content-Security-Policy"] = csp
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "no-referrer"
    response.headers["Permissions-Policy"] = "interest-cohort=()"
    if not app.debug:
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    return response


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    _inc_stat("page_views")
    return render_template("index.html")


@app.route("/info")
def info():
    return render_template("info.html")


@app.route("/terms")
def terms():
    return render_template("terms.html")


@app.route("/privacy")
def privacy():
    return render_template("privacy.html")


@app.route("/dashboard")
def dashboard():
    return render_template("dashboard.html")


@app.route("/api/stats")
def api_stats():
    """Return aggregate stats (no PII)."""
    with _stats_lock:
        return jsonify({
            "page_views": _stats.get("page_views", 0),
            "reports_generated": _stats.get("reports_generated", 0),
            "reports_completed": _stats.get("reports_completed", 0),
        })


@app.route("/health")
@limiter.exempt
def health():
    """Health check for Railway / load balancers."""
    with _jobs_lock:
        active_jobs = sum(1 for j in _jobs.values() if j["status"] == "running")
    return jsonify({
        "status": "healthy",
        "active_jobs": active_jobs,
        "max_jobs": _MAX_CONCURRENT_JOBS,
    }), 200


@app.route("/generate", methods=["POST"])
@limiter.limit("3 per minute")
def generate():
    """Validate input, start background generation, return job_id."""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "Invalid request body"}), 400

        # --- Extract and strictly validate every field ---
        validator_or_address = str(data.get("validator_or_address", "")).strip()[:200]
        fee_recipient = str(data.get("fee_recipient", "")).strip()[:42]
        date_from = str(data.get("date_from", "")).strip()[:10]
        date_to = str(data.get("date_to", "")).strip()[:10]
        etherscan_api_key = str(data.get("etherscan_api_key", "")).strip()[:40]
        currency = str(data.get("currency", "EUR")).strip().upper()[:3]
        output_format = str(data.get("output_format", "xlsx")).strip()[:4]

        if not validator_or_address:
            return jsonify({"error": "Validator index, public key, or withdrawal address is required"}), 400

        if not date_from or not date_to:
            return jsonify({"error": "Date range is required"}), 400
        if not re.match(r"^\d{4}-\d{2}-\d{2}$", date_from) or not re.match(r"^\d{4}-\d{2}-\d{2}$", date_to):
            return jsonify({"error": "Invalid date format. Use YYYY-MM-DD."}), 400

        if not etherscan_api_key:
            return jsonify({"error": "Etherscan API key is required (free at etherscan.io/apis)"}), 400
        if not ETHERSCAN_KEY_RE.match(etherscan_api_key):
            return jsonify({"error": "Invalid Etherscan API key format"}), 400

        if currency not in SUPPORTED_CURRENCIES:
            return jsonify({"error": f"Unsupported currency. Choose from: {', '.join(SUPPORTED_CURRENCIES)}"}), 400
        if output_format not in ("xlsx", "csv", "both"):
            return jsonify({"error": "Invalid output format. Choose xlsx, csv, or both."}), 400

        if fee_recipient:
            if not ETH_ADDRESS_RE.match(fee_recipient):
                return jsonify({"error": "Invalid fee recipient address format"}), 400

        # Validate and resolve validator/address input
        try:
            parsed = parse_validator_input(validator_or_address)
        except ValueError as e:
            return jsonify({"error": str(e)}), 400

        acc = {"name": "report"}
        beacon = None

        if parsed["type"] == "withdrawal_address":
            acc["withdrawal_address"] = parsed["value"]
        elif parsed["type"] == "validator_indices":
            try:
                beacon = BeaconClient()
                indices = []
                pubkeys = {}
                address = None
                cred_type = "0x01"
                for vid in parsed["value"]:
                    info = beacon.get_validator_info(vid)
                    indices.append(info["validator_index"])
                    pubkeys[info["validator_index"]] = info.get("pubkey", "")
                    cred_type = info.get("credential_type", "0x01")
                    acc.setdefault("_validator_details", []).append(info)
                    if address is None:
                        address = info["withdrawal_address"]
                    elif info["withdrawal_address"].lower() != address.lower():
                        return jsonify({"error": "Validators have different withdrawal addresses. Use a withdrawal address instead."}), 400
                acc["withdrawal_address"] = address
                acc["validator_indices"] = indices
                acc["credential_type"] = cred_type
                acc["validator_pubkeys"] = pubkeys
            except RuntimeError as e:
                return jsonify({"error": _sanitize_error(e)}), 400
        else:
            try:
                if beacon is None:
                    beacon = BeaconClient()
                info = beacon.get_validator_info(parsed["value"])
                acc["withdrawal_address"] = info["withdrawal_address"]
                acc["validator_indices"] = [info["validator_index"]]
                acc["credential_type"] = info.get("credential_type", "0x01")
                acc["validator_pubkeys"] = {info["validator_index"]: info.get("pubkey", "")}
                acc["_validator_details"] = [info]
            except RuntimeError as e:
                return jsonify({"error": _sanitize_error(e)}), 400

        if fee_recipient:
            acc["fee_recipient"] = fee_recipient

        accounts = [acc]

        # Parse and validate dates
        try:
            start_dt = datetime.strptime(date_from, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            end_dt = datetime.strptime(date_to, "%Y-%m-%d").replace(
                hour=23, minute=59, second=59, tzinfo=timezone.utc
            )
        except ValueError:
            return jsonify({"error": "Invalid date format. Use YYYY-MM-DD."}), 400

        if start_dt >= end_dt:
            return jsonify({"error": "Start date must be before end date"}), 400
        if start_dt < BEACON_GENESIS:
            return jsonify({"error": "Start date cannot be before beacon chain genesis (Dec 1, 2020)"}), 400
        if (end_dt - start_dt).days > 1095:
            return jsonify({"error": "Date range cannot exceed 3 years"}), 400

        # Concurrency control: reject if at capacity
        if not _job_semaphore.acquire(blocking=False):
            return jsonify({"error": "Server is busy. Please try again in a few minutes."}), 429

        # Create job
        job_id = uuid.uuid4().hex[:12]
        job = {
            "status": "running",
            "logs": [],
            "file_bytes": None,
            "filename": None,
            "mimetype": None,
            "error": None,
            "lock": threading.Lock(),
            "created": time.time(),
        }
        with _jobs_lock:
            _jobs[job_id] = job

        _inc_stat("reports_generated")

        thread = threading.Thread(
            target=_run_generation,
            args=(job, accounts, date_from, date_to, etherscan_api_key, currency, output_format),
            daemon=True,
        )
        thread.start()

        return jsonify({"job_id": job_id})

    except KeyError:
        return jsonify({"error": "Missing required fields"}), 400
    except ValueError:
        logger.warning("Invalid input in /generate", exc_info=True)
        return jsonify({"error": "Invalid input provided"}), 400
    except Exception:
        logger.exception("Unexpected error in /generate")
        return jsonify({"error": "An unexpected error occurred"}), 500


def _run_generation(job, accounts, date_from, date_to, etherscan_api_key, currency, output_format):
    """Background thread: fetch data, generate report, store result in job."""
    # Thread-safe log callback (no sys.stdout hijacking)
    log_fn = lambda msg: _log_to_job(job, msg)
    price_client = None
    try:
        start_ts = int(datetime.strptime(date_from, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
        end_ts = int(datetime.strptime(date_to, "%Y-%m-%d").replace(
            hour=23, minute=59, second=59, tzinfo=timezone.utc
        ).timestamp())

        # Init clients with log callback
        etherscan = EtherscanClient(etherscan_api_key, log_fn=log_fn)
        price_client = CryptoCompareClient(currency=currency, cache_path=_PRICE_CACHE_PATH, log_fn=log_fn)

        log_fn(f"Fetching ETH/{currency} prices...")
        prices = price_client.get_prices(start_ts, end_ts)

        if not prices:
            with job["lock"]:
                job["status"] = "error"
                job["error"] = "Could not fetch price data. Try again later."
            return

        # Gather all events
        all_events = []
        for account in accounts:
            events = gather_events(account, etherscan, start_ts, end_ts, log_fn=log_fn)
            all_events.extend(events)

        if not all_events:
            with job["lock"]:
                job["status"] = "error"
                job["error"] = "No events found for any account in the given date range"
            return

        _prepare_events(all_events, prices)
        validator_info = _build_validator_info(accounts, all_events)
        summary = _build_summary(all_events, validator_info, currency)

        # Build safe filename
        if len(accounts) == 1:
            safe_name = re.sub(r"[^a-zA-Z0-9_-]", "", accounts[0]["name"]) or "unnamed"
        else:
            safe_name = f"{len(accounts)}_accounts"
        filename_base = f"staking_report_{safe_name}_{date_from}_{date_to}"

        log_fn("Generating report file...")
        if output_format == "xlsx":
            file_bytes = write_excel_bytes(all_events, prices, currency, validator_info=validator_info)
            filename = f"{filename_base}.xlsx"
            mimetype = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        elif output_format == "csv":
            file_bytes = write_csv_bytes(all_events, prices, currency)
            filename = f"{filename_base}.csv"
            mimetype = "text/csv"
        else:
            xlsx_bytes = write_excel_bytes(all_events, prices, currency, validator_info=validator_info)
            csv_bytes = write_csv_bytes(all_events, prices, currency)
            zip_buf = io.BytesIO()
            with zipfile.ZipFile(zip_buf, "w", zipfile.ZIP_DEFLATED) as zf:
                zf.writestr(f"{filename_base}.xlsx", xlsx_bytes)
                zf.writestr(f"{filename_base}.csv", csv_bytes)
            file_bytes = zip_buf.getvalue()
            filename = f"{filename_base}.zip"
            mimetype = "application/zip"

        log_fn("Report ready for download.")
        _inc_stat("reports_completed")
        with job["lock"]:
            job["status"] = "done"
            job["file_bytes"] = file_bytes
            job["filename"] = filename
            job["mimetype"] = mimetype
            job["summary"] = summary

    except Exception as e:
        logger.exception("Report generation failed")
        safe_msg = _sanitize_error(e)
        with job["lock"]:
            job["status"] = "error"
            job["error"] = safe_msg
    finally:
        _job_semaphore.release()
        if price_client:
            try:
                price_client.close()
            except Exception:
                pass


def _build_validator_info(accounts, all_events):
    """Build validator_info list for the Validators sheet from account data and events."""
    withdrawn_by_validator = defaultdict(Decimal)
    withdrawn_income_by_validator = defaultdict(Decimal)
    for e in all_events:
        vid = e.get("validator")
        if vid is None:
            continue
        if e["type"] == "withdrawal":
            withdrawn_by_validator[vid] += e["amount_eth"]
            withdrawn_income_by_validator[vid] += e["amount_eth"]
        elif e["type"] == "withdrawal_principal":
            withdrawn_by_validator[vid] += e["amount_eth"]

    validator_info = []
    for acc in accounts:
        details = acc.get("_validator_details", [])
        for info in details:
            vid = info["validator_index"]
            validator_info.append({
                "index": vid,
                "name": acc.get("name", ""),
                "credential_type": info.get("credential_type", "0x01"),
                "status": info.get("status", ""),
                "balance_eth": info.get("balance_eth", Decimal(0)),
                "effective_balance_eth": info.get("effective_balance_eth", Decimal(0)),
                "withdrawn_eth": withdrawn_by_validator.get(vid, Decimal(0)),
                "withdrawn_income_eth": withdrawn_income_by_validator.get(vid, Decimal(0)),
            })

    return validator_info if validator_info else None


def _build_summary(all_events, validator_info, currency):
    """Build a JSON-serializable summary dict for the frontend."""
    w_eth = sum(float(e["amount_eth"]) for e in all_events if e["type"] == "withdrawal")
    w_fiat = sum(float(e["value_fiat"]) for e in all_events if e["type"] == "withdrawal")
    br_eth = sum(float(e["amount_eth"]) for e in all_events if e["type"] == "block_reward")
    br_fiat = sum(float(e["value_fiat"]) for e in all_events if e["type"] == "block_reward")
    p_eth = sum(float(e["amount_eth"]) for e in all_events if e["type"] == "withdrawal_principal")
    p_fiat = sum(float(e["value_fiat"]) for e in all_events if e["type"] == "withdrawal_principal")

    summary = {
        "currency": currency,
        "total_income_eth": w_eth + br_eth,
        "total_income_fiat": w_fiat + br_fiat,
        "withdrawals_eth": w_eth,
        "withdrawals_fiat": w_fiat,
        "block_rewards_eth": br_eth,
        "block_rewards_fiat": br_fiat,
        "event_count": len(all_events),
    }

    if p_eth > 0:
        summary["principal_eth"] = p_eth
        summary["principal_fiat"] = p_fiat

    months = defaultdict(lambda: {"w_eth": 0.0, "w_fiat": 0.0, "br_eth": 0.0, "br_fiat": 0.0, "p_eth": 0.0, "p_fiat": 0.0, "prices": []})
    for e in all_events:
        mk = e["datetime"].strftime("%Y-%m")
        amt = float(e["amount_eth"])
        val = float(e["value_fiat"])
        months[mk]["prices"].append(float(e["price_fiat"]))
        if e["type"] == "withdrawal":
            months[mk]["w_eth"] += amt
            months[mk]["w_fiat"] += val
        elif e["type"] == "block_reward":
            months[mk]["br_eth"] += amt
            months[mk]["br_fiat"] += val
        elif e["type"] == "withdrawal_principal":
            months[mk]["p_eth"] += amt
            months[mk]["p_fiat"] += val

    summary["months"] = []
    for mk in sorted(months.keys()):
        m = months[mk]
        prices_list = m["prices"]
        avg_price = sum(prices_list) / len(prices_list) if prices_list else 0.0
        entry = {
            "month": mk,
            "withdrawals_eth": m["w_eth"],
            "withdrawals_fiat": m["w_fiat"],
            "block_rewards_eth": m["br_eth"],
            "block_rewards_fiat": m["br_fiat"],
            "total_eth": m["w_eth"] + m["br_eth"],
            "total_fiat": m["w_fiat"] + m["br_fiat"],
            "avg_price": avg_price,
        }
        if p_eth > 0:
            entry["principal_eth"] = m["p_eth"]
            entry["principal_fiat"] = m["p_fiat"]
        summary["months"].append(entry)

    if validator_info:
        summary["validators"] = []
        for vi in sorted(validator_info, key=lambda x: x.get("index", 0)):
            cred = vi.get("credential_type", "0x01")
            summary["validators"].append({
                "index": vi.get("index", ""),
                "name": vi.get("name", ""),
                "type": "Compounding" if cred == "0x02" else "Distributing",
                "status": vi.get("status", ""),
                "balance": float(vi.get("balance_eth", 0)),
                "effective_balance": float(vi.get("effective_balance_eth", 0)),
                "withdrawn": float(vi.get("withdrawn_eth", 0)),
                "withdrawn_income": float(vi.get("withdrawn_income_eth", 0)),
            })

    return summary


@app.route("/stream/<job_id>")
@limiter.limit("10 per minute")
def stream(job_id):
    """SSE endpoint: streams log lines from a running job."""
    with _jobs_lock:
        job = _jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404

    def event_stream():
        sent = 0
        last_heartbeat = time.time()
        while True:
            with job["lock"]:
                status = job["status"]
                logs = job["logs"][sent:]
                error = job["error"]

            for line in logs:
                yield f"data: {json.dumps({'type': 'log', 'message': line})}\n\n"
                sent += 1

            if status == "done":
                yield f"data: {json.dumps({'type': 'done', 'filename': job['filename'], 'summary': job.get('summary')})}\n\n"
                break
            elif status == "error":
                yield f"data: {json.dumps({'type': 'error', 'message': error})}\n\n"
                break

            # SSE heartbeat to keep proxies (Railway, Cloudflare) from closing idle connections
            now = time.time()
            if now - last_heartbeat >= 15:
                yield ": heartbeat\n\n"
                last_heartbeat = now

            time.sleep(0.3)

    return Response(
        event_stream(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@app.route("/download/<job_id>")
def download(job_id):
    """Download the generated report file and delete the job from memory."""
    with _jobs_lock:
        job = _jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404

    with job["lock"]:
        if job["status"] != "done" or not job["file_bytes"]:
            return jsonify({"error": "Report not ready yet"}), 202

        file_bytes = job["file_bytes"]
        mimetype = job["mimetype"]
        filename = job["filename"]

    # Delete job from memory immediately — summary is already client-side
    with _jobs_lock:
        _jobs.pop(job_id, None)

    return Response(
        file_bytes,
        mimetype=mimetype,
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Cache-Control": "no-store",
        },
    )


if __name__ == "__main__":
    app.run(
        host="127.0.0.1",
        debug=os.environ.get("FLASK_DEBUG", "").lower() == "true",
        port=int(os.environ.get("PORT", "8080")),
    )
