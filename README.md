# trading-bot-15-min

Kalshi 15-minute crypto binary contract trading bot. Python port of an AHK Kalshi bot.

## Stack
- Python 3.12
- WebSocket: Kalshi orderbook + Kraken/Coinbase BTC ticks
- Dashboard: lightweight HTML served from `dashboard.py`
- Pricing: Black-Scholes d2 with realized vol from 1-min close-to-close returns (15m window, matches TradingView Polymarket Quant)

## Files
- `bot.py` — main loop, WS feed, order placement
- `signals.py` — BTC tick aggregation, candle/momentum, d2 fair value
- `filter_logic.py` — entry filter checklist (price, window, conviction, edge)
- `dashboard.py` — live status UI
- `requirements.txt` — Python deps

## Setup
1. `python -m venv venv && source venv/bin/activate`
2. `pip install -r requirements.txt`
3. Place Kalshi credentials in `reactions/` (not committed):
   - `reactions/apikey.json` — `{"code": "<key-id>"}`
   - `reactions/privatekey.pem` — RSA private key
4. `python bot.py BTC`
