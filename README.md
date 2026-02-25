# ETH Rewards Report

Generate tax reports for Ethereum staking rewards. Fetches consensus layer withdrawals and execution layer rewards (including MEV), prices them in your local fiat currency, and exports to Excel or CSV.

[![Deploy on Railway](https://railway.com/button.svg)](https://railway.com/template/new?repository=https://github.com/frederik12321/eth-rewards-report)

## Features

- **Consensus withdrawals** — fetches all validator withdrawals from Etherscan
- **Execution rewards** — detects local block proposals and MEV payments (40+ known builders)
- **Fiat pricing** — hourly ETH prices from CryptoCompare in 10 currencies (EUR, USD, GBP, CHF, CAD, AUD, JPY, SEK, NOK, DKK)
- **0x02 compounding validators** — correctly separates reward skimming from principal returns
- **Multiple validators** — enter a withdrawal address, comma-separated indices, or a public key
- **Export formats** — Excel (.xlsx), CSV, or both
- **In-browser summary** — monthly breakdown chart, validator overview, and totals before downloading

## Quick start

```bash
git clone https://github.com/frederik12321/eth-rewards-report.git
cd eth-rewards-report
pip install -r requirements.txt
python app.py
```

Open `http://localhost:8080` and enter your withdrawal address or validator index, date range, and a free [Etherscan API key](https://etherscan.io/apis).

## Deploy to Railway

The repo includes `railway.toml`, `Procfile`, and `gunicorn.conf.py` — ready to deploy.

1. Click the **Deploy on Railway** button above, or connect the repo manually in the Railway dashboard
2. Railway will auto-detect the Python buildpack and install dependencies
3. No environment variables are required — defaults work out of the box
4. The health check at `/health` confirms the service is running

Optional environment variables (all have sensible defaults):

| Variable | Default | Description |
|---|---|---|
| `PORT` | `8080` | Set automatically by Railway |
| `MAX_CONCURRENT_JOBS` | `5` | Max parallel report generations |
| `PRICE_CACHE_PATH` | `price_cache.db` | SQLite cache for ETH prices |
| `STATS_PATH` | `stats.json` | Aggregate usage counters |

## How it works

1. You provide a withdrawal address (or validator index) and an Etherscan API key
2. The app resolves validator indices via the Beacon Chain API
3. Consensus withdrawals are fetched from Etherscan and classified as reward or principal
4. Execution layer rewards are detected by scanning block proposals and matching against known MEV builder addresses
5. Hourly ETH prices are fetched from CryptoCompare (cached in SQLite)
6. All events are priced in your chosen currency and exported to Excel/CSV

## Security

- **Rate limiting** — 60 req/min global, 3 req/min on report generation
- **Concurrency cap** — configurable max parallel jobs
- **CSP** — nonce-based Content Security Policy for scripts
- **HTTPS** — enforced with HSTS
- **Error sanitization** — no tracebacks or internal details leak to clients
- **Structured logging** — request IDs for tracing, no PII logged
- **API resilience** — retry with exponential backoff for external APIs
- **Health check** — `GET /health` for uptime monitoring

## License

[MIT](LICENSE)
