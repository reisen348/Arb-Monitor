# Arb Monitor

Realtime multi-venue perpetual futures arbitrage monitor.

Arb Monitor polls public market data from centralized and decentralized perp venues, normalizes prices, funding rates, open interest, volume, fees, and order book depth, then renders a lightweight dashboard for spread and funding-rate monitoring. It is a monitoring and research tool only; it does not place live orders.

## Features

- REST-based scanner for live perp markets.
- Web dashboard focused on cross-venue spreads, funding direction, open interest, volume, fee estimates, and rolling 4-hour spread statistics.
- Telegram alerts for persistent spread dislocations:
  - spread minus 4-hour mean greater than 0.5%
  - 4-hour spread Z-score at least 1.25
  - both legs have at least $1M open interest
  - condition persists for at least 60 seconds
- Rolling SQLite state for spread history and dashboard recovery.
- Focused unit test coverage for adapters, scanner, scoring, dashboard payloads, and persistence.

## Supported Venues

Current adapters include:

- Binance
- Bybit
- OKX
- Bitget
- Gate
- Kraken
- Aster
- Hyperliquid
- GRVT
- Paradex
- Lighter
- Nado
- Ondo Perps
- Variational Omni

Binance and Bybit use their official public exchange APIs. The packaged dashboard service keeps the broad `all_live` source set, but explicitly disables third-party Binance/Bybit fallback sources.

Variational uses official `metadata/stats` for universe discovery and `/prices` WebSocket data for live BTC/ETH/SOL-style price streams. If you run a Chrome CDP forwarder that writes a `monitor_state.json` with fresh `/api/quotes/indicative` responses, pass `--variational-forwarder-snapshot /path/to/monitor_state.json` to let those fresh quotes override the conservative RWA stats-mark fallback while the file is still within `--variational-forwarder-quote-ttl`.

## Requirements

- Python 3.9+
- Network access to exchange public APIs
- Optional: Telegram bot token and chat ID for alerts

Install locally:

```bash
python3 -m pip install -e .
```

Run tests:

```bash
python3 -m unittest discover -s tests -v
```

## Configuration

Copy the example environment file if Telegram alerts are needed:

```bash
cp .env.example .env
```

Then edit `.env`:

```bash
TG_BOT_TOKEN=your_bot_token_here
TG_CHAT_ID=your_chat_id_here
```

`.env` is intentionally ignored by git. Do not commit real tokens, chat IDs, databases, logs, screenshots, or local service files.

## Run The Dashboard

Default live dashboard:

```bash
python3 -m perp_arb dashboard \
  --source all_live \
  --transport rest \
  --top 10000 \
  --min-label blocked \
  --refresh 30 \
  --interval 10 \
  --host 0.0.0.0 \
  --port 8765 \
  --top-book-markets 2 \
  --binance-oi-markets 100 \
  --lighter-book-request-workers 1 \
  --db-path arb_state.db
```

Open:

```text
http://127.0.0.1:8765
```

Run a one-shot JSON scan:

```bash
python3 -m perp_arb scan --source all_live --top 20 --min-label blocked --format json
```

Run a single source:

```bash
python3 -m perp_arb scan --source ondo --top 20 --min-label blocked --format json
```

## Systemd User Service

Install the user service template:

```bash
bash scripts/install_systemd_service.sh
systemctl --user daemon-reload
systemctl --user enable --now perp-arb-dashboard.service
systemctl --user status perp-arb-dashboard.service
```

Logs:

```bash
journalctl --user -u perp-arb-dashboard.service -f
```

## Data And Privacy

The repository is designed to contain source code only. Runtime data is intentionally excluded:

- `.env`
- SQLite databases and WAL/SHM files
- logs and pid files
- screenshots
- Python cache directories
- local handoff or agent files

Before publishing changes, run:

```bash
git status --short
```

and verify that only source, tests, scripts, and documentation are staged.

## Notes

This project estimates relative opportunities from public market data. Exchange APIs can be delayed, rate-limited, stale, or semantically inconsistent. Always validate prices, funding intervals, open interest, order book depth, and fees directly against the venues before using any output for trading decisions.
