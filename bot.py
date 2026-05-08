"""
Kalshi BTC 15-Minute Momentum Bot  (Strategy 1)
-------------------------------------------------
Enters when YES or NO price hits ≥ 70¢ in the last 8 minutes of a
15-min window. Take-profit at 91¢, stop-loss at 65¢.
"""

import asyncio
import base64
import json
import logging
import os
import sys
import threading
import time
import uuid
from collections import deque as _deque
from datetime import datetime, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

try:
    NY_TZ = ZoneInfo("America/New_York")  # auto-handles EDT (-04) ↔ EST (-05) DST switch
except ZoneInfoNotFoundError:
    # Windows lacks IANA tzdata — fall back to bundled `tzdata` pip package
    import subprocess, sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "--quiet", "tzdata"])
    NY_TZ = ZoneInfo("America/New_York")
from typing import Optional

import requests
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

try:
    from signals import start_feed, get_signal, d2_edge, get_live_price, get_ticks
    SIGNALS_AVAILABLE = True
except ImportError:
    SIGNALS_AVAILABLE = False

from filter_logic import evaluate_filters, compute_pnl, is_win

try:
    import websockets
    import ssl as _ssl
    import certifi as _certifi
    _ssl_ctx = _ssl.create_default_context(cafile=_certifi.where())
    WS_AVAILABLE = True
except ImportError:
    WS_AVAILABLE = False
    _ssl_ctx = True

# =========================
# Strategy Configuration
# =========================
ENTRY                  = 0.65   # relaxed from 0.82 → trade lower-price entries with better R:R, relying on Signal/Conviction/Settlement/Edge stack
ENTRY_LAST2            = 0.85   # last-2-min fallback: enter if contract >= this (no signal required)
LAST2_BYPASS_SIGNAL    = False         # global fallback (overridden per-asset below)
LAST2_BYPASS_SIGNAL_ASSETS = set()     # disabled — last-2min is too volatile, require signal always
EXIT                   = 0.50   # stop-loss: exit when contract drops below this (loosened from 0.55 to survive vol spikes)
TIME_WINDOW            = 10.0   # widened from 6 → catch earlier entries; relies on Edge filter (d2) to reject when σ√T makes fair_prob too uncertain
MIN_SETTLEMENT_SCORE   = 0.40   # settlement_score must confirm direction (tightened from 0.25 — higher conviction only)
MIN_CONVICTION         = 0.55   # signal conviction must be at least this strong to enter
USE_EDGE_FILTER        = True   # enforce d2 fair-value edge filter before entry
MIN_EDGE               = 0.03   # required edge (fair_prob - market_price) — e.g. 0.03 = 3¢ underpriced
MAX_CONSECUTIVE_LOSSES = 2      # pause trading after this many consecutive losses (tightened from 3)
ASSETS       = ["BTC"]
ORDER_SIZE   = 5   # default fallback
ASSET_ORDER_SIZE = {"BTC": 22, "ETH": 18}  # sized for ~$600 bankroll: ~$18/trade = 3% per trade (Phase 1)

# =========================
# API / Order Config
# =========================
ENABLE_LIVE_ORDERS = True
POLL_INTERVAL_MS   = 300
REST_DELAY         = 2.0
ENTRY_DIFF         = 0.02

KALSHI_API_BASE = "https://api.elections.kalshi.com"

ASSET_SERIES_MAP = {
    "BTC":  "KXBTC15M",
    "ETH":  "KXETH15M",
    "SOL":  "KXSOL15M",
    "XRP":  "KXXRP15M",
    "DOGE": "KXDOGE15M",
    "HYPE": "KXHYPE15M",
    "BNB":  "KXBNB15M",
}

KALSHI_API_KEY_FILE      = "reactions/apikey.json"
KALSHI_PRIVATE_KEY_FILE  = "reactions/privatekey.pem"
KALSHI_KEY_PASSPHRASE    = ""

# =========================
# Logging
# =========================
_LOG_FORMAT   = "%(asctime)s | %(message)s"
_LOG_DATEFMT  = "%Y-%m-%d %H:%M:%S%z"
_LOG_DIR      = os.path.dirname(os.path.abspath(__file__))

# Root logger: stdout only
logging.basicConfig(
    level=logging.INFO,
    format=_LOG_FORMAT,
    datefmt=_LOG_DATEFMT,
    handlers=[logging.StreamHandler(sys.stdout)],
)
_log = logging.getLogger(__name__)

# Per-asset file handlers (created on first use)
_asset_file_handlers: dict[str, logging.FileHandler] = {}
_asset_loggers: dict[str, logging.Logger] = {}

def _get_asset_logger(asset: str) -> logging.Logger:
    if asset not in _asset_loggers:
        log_path = os.path.join(_LOG_DIR, f"bot_{asset.lower()}.log")
        fh = logging.FileHandler(log_path, encoding="utf-8")
        fh.setFormatter(logging.Formatter(_LOG_FORMAT, datefmt=_LOG_DATEFMT))
        logger = logging.getLogger(f"bot.{asset}")
        logger.addHandler(fh)
        logger.propagate = True   # also goes to stdout via root
        _asset_file_handlers[asset] = fh
        _asset_loggers[asset] = logger
    return _asset_loggers[asset]

def Log(msg: str, asset: str = ""):
    if asset:
        _get_asset_logger(asset).info(msg)
    else:
        _log.info(msg)

# =========================
# Global State
# =========================
kalshi_api_key_id: str = ""
private_key = None

positions: dict             = {}
asset_phase: dict           = {}
asset_session_quarter: dict = {}
asset_last_price_log_tick: dict = {}
log_once_keys: dict         = {}
consecutive_losses: dict    = {}   # asset -> int, resets on win
asset_reentry_count: dict   = {}   # asset -> int, resets each session

# =========================
# Trade Journal (append-only JSONL)
# =========================
JOURNAL_FILE = os.path.join(_LOG_DIR, "trades.jsonl")
STATE_FILE   = os.path.join(_LOG_DIR, "bot_state.json")
PERSIST_FILE = os.path.join(_LOG_DIR, "bot_persistent.json")

def write_state(state: dict):
    """Atomically dump bot state for dashboard to read."""
    try:
        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, default=str)
        os.replace(tmp, STATE_FILE)
    except Exception:
        pass

def save_persistent():
    """Save state that must survive restart: positions, loss streak, reentry count."""
    try:
        data = {
            "positions": positions,
            "consecutive_losses": consecutive_losses,
            "asset_reentry_count": asset_reentry_count,
            "asset_phase": asset_phase,
            "asset_session_quarter": asset_session_quarter,
        }
        tmp = PERSIST_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f)
        os.replace(tmp, PERSIST_FILE)
    except Exception as e:
        Log(f"Persist save error: {e}")

def load_persistent():
    """Restore state from disk on startup (best-effort)."""
    if not os.path.exists(PERSIST_FILE):
        return
    try:
        with open(PERSIST_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        positions.update(data.get("positions", {}))
        consecutive_losses.update(data.get("consecutive_losses", {}))
        asset_reentry_count.update(data.get("asset_reentry_count", {}))
        asset_phase.update(data.get("asset_phase", {}))
        asset_session_quarter.update(data.get("asset_session_quarter", {}))
        if positions or consecutive_losses:
            Log(f"Restored state: positions={list(positions.keys())} losses={dict(consecutive_losses)}")
    except Exception as e:
        Log(f"Persist load error: {e}")

def journal(event: str, asset: str, **fields):
    """Append a trade event as one JSON line. Never raises."""
    try:
        rec = {"ts": time.time(), "iso": _now_ny().isoformat(), "event": event, "asset": asset, **fields}
        with open(JOURNAL_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec) + "\n")
    except Exception as e:
        Log(f"journal write error: {e}")

# =========================
# Contract Price Tracker
# =========================
class ContractPriceTracker:
    def __init__(self, maxlen: int = 300):
        self._up:   _deque = _deque(maxlen=maxlen)
        self._down: _deque = _deque(maxlen=maxlen)

    def push(self, up: float, down: float):
        ts = time.time()
        self._up.append((ts, up))
        self._down.append((ts, down))

    def velocity(self, side: str, window_s: float = 30.0) -> float:
        buf = self._up if side == "UP" else self._down
        now = time.time()
        recent = [(ts, p) for ts, p in buf if ts >= now - window_s]
        if len(recent) < 2:
            return 0.0
        dt = recent[-1][0] - recent[0][0]
        return (recent[-1][1] - recent[0][1]) / dt if dt > 0.5 else 0.0

    def reset(self):
        self._up.clear()
        self._down.clear()

_contract_trackers: dict[str, ContractPriceTracker] = {}

# =========================
# WebSocket Price Feed
# =========================
_ws_prices:  dict[str, dict] = {}  # asset -> {"up": float, "down": float, "ts": float}
_ws_ticker:  dict[str, str]  = {}  # asset -> currently subscribed ticker
_ws_books:   dict[str, dict] = {}  # asset -> {"yes_bids": {price:size}, "no_bids": {price:size}}
_ws_last_seq: dict[str, int] = {}  # asset -> last seq seen on current subscription

def _update_prices_from_book(asset: str):
    """Compute up/down as ask prices on each side (matches what Kalshi UI shows).
       up   = price to buy YES  = 1 - best_no_bid
       down = price to buy NO   = 1 - best_yes_bid
       Note: up + down ≈ 1 + spread (not exactly 1)."""
    book = _ws_books.get(asset)
    if not book:
        return
    yes_bids = book.get("yes_bids") or {}
    no_bids  = book.get("no_bids")  or {}
    if not yes_bids and not no_bids:
        return
    best_yes_bid = max(yes_bids) if yes_bids else 0.0
    best_no_bid  = max(no_bids)  if no_bids  else 0.0
    # Ask price on each side (price to buy)
    up_p   = (1.0 - best_no_bid) if best_no_bid else 0.99
    down_p = (1.0 - best_yes_bid) if best_yes_bid else 0.99
    up_p   = max(0.01, min(0.99, up_p))
    down_p = max(0.01, min(0.99, down_p))
    _ws_prices[asset] = {"up": up_p, "down": down_p, "ts": time.time()}
    _contract_trackers.setdefault(asset, ContractPriceTracker()).push(up_p, down_p)

async def _ws_feed(asset: str):
    uri     = "wss://api.elections.kalshi.com/trade-api/ws/v2"  # elections endpoint works; docs also list external-api-ws.kalshi.com
    backoff = 1.0
    msg_id  = 1
    current_sid: Optional[int] = None  # track subscription id for proper unsubscribe

    while True:
        try:
            ts  = str(current_time_millis())
            sig = sign_kalshi_message(ts + "GET" + "/trade-api/ws/v2")
            hdrs = {
                "KALSHI-ACCESS-KEY":       kalshi_api_key_id,
                "KALSHI-ACCESS-TIMESTAMP": ts,
                "KALSHI-ACCESS-SIGNATURE": sig,
            }
            if not sig or not kalshi_api_key_id:
                Log(f"{asset} WS skipped — credentials not ready", asset=asset)
                await asyncio.sleep(5)
                continue
            async with websockets.connect(uri, ssl=_ssl_ctx, additional_headers=hdrs, ping_interval=20, ping_timeout=10) as ws:
                backoff = 1.0
                Log(f"{asset} WS connected", asset=asset)
                current_sub: Optional[str] = None

                CHANNELS = ["ticker", "orderbook_delta"]
                t = _ws_ticker.get(asset, "")
                if t:
                    # plural array form — empirically works on api.elections.kalshi.com
                    await ws.send(json.dumps({
                        "id": msg_id, "cmd": "subscribe",
                        "params": {"channels": CHANNELS, "market_tickers": [t]},
                    }))
                    msg_id += 1
                    current_sub = t
                    current_sid = None
                    _ws_books[asset] = {"yes_bids": {}, "no_bids": {}}
                    _ws_last_seq.pop(asset, None)
                    Log(f"{asset} WS subscribed to {t} (ticker + orderbook_delta)", asset=asset)

                while True:
                    # Check ticker change BEFORE blocking on recv
                    new_t = _ws_ticker.get(asset, "")
                    if new_t and new_t != current_sub:
                        if current_sid is not None:
                            await ws.send(json.dumps({
                                "id": msg_id, "cmd": "unsubscribe",
                                "params": {"sids": [current_sid]},
                            }))
                            msg_id += 1
                        elif current_sub:
                            # fallback if we don't have sid (older subscriptions)
                            await ws.send(json.dumps({
                                "id": msg_id, "cmd": "unsubscribe",
                                "params": {"market_tickers": [current_sub]},
                            }))
                            msg_id += 1
                        await ws.send(json.dumps({
                            "id": msg_id, "cmd": "subscribe",
                            "params": {"channels": CHANNELS, "market_tickers": [new_t]},
                        }))
                        msg_id += 1
                        current_sub = new_t
                        current_sid = None
                        _ws_books[asset] = {"yes_bids": {}, "no_bids": {}}
                        _ws_last_seq.pop(asset, None)
                        Log(f"{asset} WS re-subscribed to {new_t}", asset=asset)

                    # Recv with 1s timeout so loop can re-check ticker even when market is silent
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                    except asyncio.TimeoutError:
                        continue

                    try:
                        data = json.loads(raw)
                        msg_type = data.get("type")
                        msg = data.get("msg", {})
                        if not isinstance(msg, dict):
                            continue

                        if msg_type == "subscribed":
                            sid = msg.get("sid")
                            channel = msg.get("channel", "?")
                            if sid is not None:
                                current_sid = int(sid)
                            Log(f"{asset} WS subscribed: channel={channel} sid={sid}", asset=asset)
                            continue

                        if msg_type == "error":
                            Log(f"{asset} WS ERROR: {json.dumps(msg)[:300]}", asset=asset)
                            continue

                        if msg_type == "ticker":
                            # Kalshi sends prices as strings in dollars: yes_ask_dollars, yes_bid_dollars, price_dollars
                            yes_ask = msg.get("yes_ask_dollars")
                            yes_bid = msg.get("yes_bid_dollars")
                            last_p  = msg.get("price_dollars")
                            try:
                                if yes_ask and yes_bid:
                                    yes_p = (float(yes_ask) + float(yes_bid)) / 2.0  # mid
                                elif yes_ask:
                                    yes_p = float(yes_ask)
                                elif yes_bid:
                                    yes_p = float(yes_bid)
                                elif last_p:
                                    yes_p = float(last_p)
                                else:
                                    continue
                            except (TypeError, ValueError):
                                continue
                            yes_p = max(0.01, min(0.99, yes_p))
                            no_p  = 1.0 - yes_p
                            _ws_prices[asset] = {"up": yes_p, "down": no_p, "ts": time.time()}
                            _contract_trackers.setdefault(asset, ContractPriceTracker()).push(yes_p, no_p)

                        elif msg_type == "orderbook_snapshot":
                            seq = data.get("seq")
                            book = _ws_books.setdefault(asset, {"yes_bids": {}, "no_bids": {}})
                            # round to 2 decimals — Kalshi prices are in penny increments,
                            # avoids float key mismatch (0.23 vs 0.230) when deltas come in.
                            book["yes_bids"] = {round(float(p), 2): float(s)
                                                for p, s in msg.get("yes_dollars_fp", [])}
                            book["no_bids"]  = {round(float(p), 2): float(s)
                                                for p, s in msg.get("no_dollars_fp",  [])}
                            if isinstance(seq, int):
                                _ws_last_seq[asset] = seq
                            log_once(asset, f"OB_SNAP_{current_sub}",
                                     f"{asset} WS orderbook_snapshot for {current_sub}: "
                                     f"yes_bids={len(book['yes_bids'])} no_bids={len(book['no_bids'])}")
                            _update_prices_from_book(asset)

                        elif msg_type == "orderbook_delta":
                            seq = data.get("seq")
                            last = _ws_last_seq.get(asset)
                            # Gap detection — per Kalshi spec seq must increment monotonically.
                            # On gap, local book is suspect: clear it and resubscribe to force a fresh snapshot.
                            if isinstance(seq, int) and isinstance(last, int) and seq != last + 1:
                                Log(f"{asset} WS seq gap on {current_sub}: expected {last+1} got {seq} — resubscribing", asset=asset)
                                try:
                                    if current_sid is not None:
                                        await ws.send(json.dumps({
                                            "id": msg_id, "cmd": "unsubscribe",
                                            "params": {"sids": [current_sid]},
                                        }))
                                        msg_id += 1
                                    if current_sub:
                                        await ws.send(json.dumps({
                                            "id": msg_id, "cmd": "subscribe",
                                            "params": {"channels": CHANNELS, "market_tickers": [current_sub]},
                                        }))
                                        msg_id += 1
                                except Exception as e:
                                    Log(f"{asset} WS resubscribe-on-gap failed: {e}", asset=asset)
                                _ws_books[asset] = {"yes_bids": {}, "no_bids": {}}
                                _ws_last_seq.pop(asset, None)
                                current_sid = None
                                continue
                            book = _ws_books.setdefault(asset, {"yes_bids": {}, "no_bids": {}})
                            side  = msg.get("side")
                            try:
                                price = round(float(msg.get("price_dollars", 0)), 2)
                                delta = float(msg.get("delta_fp", 0))
                            except (TypeError, ValueError):
                                continue
                            key = "yes_bids" if side == "yes" else "no_bids"
                            book[key][price] = book[key].get(price, 0.0) + delta
                            # use small epsilon to handle float drift from accumulated deltas
                            if book[key][price] <= 0.001:
                                book[key].pop(price, None)
                            if isinstance(seq, int):
                                _ws_last_seq[asset] = seq
                            log_once(asset, f"OB_DELTA_{current_sub}",
                                     f"{asset} WS orderbook_delta flowing for {current_sub}")
                            _update_prices_from_book(asset)

                        elif msg_type and msg_type not in ("subscribed", "unsubscribed", "ok", "error"):
                            log_once(asset, f"WS_UNK_{msg_type}",
                                     f"{asset} WS unhandled msg_type={msg_type} keys={list(msg.keys())[:8]}")

                    except Exception as e:
                        log_once(asset, "WS_PARSE_ERR", f"{asset} WS parse error: {e}")
                        continue

        except Exception as e:
            Log(f"{asset} WS disconnected ({e}) — retry in {backoff:.0f}s", asset=asset)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)

def start_ws_feed(asset: str):
    if not WS_AVAILABLE:
        Log(f"websockets not installed — WS feed disabled for {asset}")
        return
    def _run():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(_ws_feed(asset))
        except Exception as e:
            Log(f"{asset} WS thread crashed: {e}", asset=asset)
    threading.Thread(target=_run, daemon=True, name=f"ws-{asset}").start()

# =========================
# HTTP Session
# =========================
_http_session = requests.Session()
_http_session.headers.update({
    "User-Agent":    "Mozilla/5.0",
    "Cache-Control": "no-cache, no-store",
    "Pragma":        "no-cache",
})

# =========================
# Time Utilities
# =========================
def _now_utc() -> datetime:
    return datetime.now(timezone.utc)

def _now_ny() -> datetime:
    # Machine clock is already NY wall time; attach NY tzinfo to naive local now
    # (avoids double-conversion if OS tz config is non-standard, e.g. Windows TZ=UTC w/ NY clock)
    return datetime.now().replace(tzinfo=NY_TZ)

def current_time_millis() -> int:
    return int(time.time() * 1000)

def minutes_remaining_in_quarter() -> float:
    now = _now_utc()
    return 15 - (now.minute % 15) - (now.second / 60.0)

def current_quarter_index() -> int:
    now = _now_utc()
    return (now.hour * 60 + now.minute) // 15

def minutes_until_iso_utc(iso: str) -> Optional[float]:
    if not iso:
        return None
    try:
        iso = iso.replace("Z", "+00:00")
        dt = datetime.fromisoformat(iso)
        return (dt - _now_utc()).total_seconds() / 60.0
    except Exception:
        return None

# =========================
# Log-once Helpers
# =========================
def log_once(asset: str, reason: str, msg: str):
    key = f"{asset}|{reason}"
    if key not in log_once_keys:
        log_once_keys[key] = True
        Log(msg)

def log_once_reset(asset: str):
    for k in list(log_once_keys):
        if k.startswith(f"{asset}|"):
            del log_once_keys[k]

def fmt(price) -> str:
    return f"{round(float(price), 2):.2f}"

# =========================
# Credentials
# =========================
def _resolve_path(path_spec: str) -> str:
    if not path_spec:
        return ""
    if os.path.exists(path_spec):
        return path_spec
    candidate = os.path.join(os.path.dirname(os.path.abspath(__file__)), path_spec)
    return candidate if os.path.exists(candidate) else ""

def load_kalshi_credentials():
    global kalshi_api_key_id, private_key

    p = _resolve_path(KALSHI_API_KEY_FILE)
    if p:
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
        kalshi_api_key_id = data.get("code", "").strip()

    p = _resolve_path(KALSHI_PRIVATE_KEY_FILE)
    if p:
        with open(p, "rb") as f:
            passphrase = KALSHI_KEY_PASSPHRASE.encode() if KALSHI_KEY_PASSPHRASE else None
            private_key = serialization.load_pem_private_key(
                f.read(),
                password=passphrase,
                backend=default_backend(),
            )

def startup_credential_check():
    api_ready = bool(kalshi_api_key_id)
    key_ready = private_key is not None

    if api_ready and key_ready:
        res = kalshi_signed_request("GET", "/trade-api/v2/portfolio/balance")
        if res and res["status"] == 200:
            Log("Credentials processed successfully")
            return
        if res and res["status"] in (401, 403):
            Log(f"ERROR: API key or private key is incorrect (HTTP {res['status']})")
            Log("Please fix your API key / private key and restart.")
            sys.exit(1)
        status_str = str(res["status"]) if res else "no response"
        Log(f"WARNING: Could not verify credentials (HTTP {status_str}) — continuing anyway")
        return

    if not api_ready:
        Log("ERROR: API key not loaded")
    if not key_ready:
        Log("ERROR: Private key not loaded")
    Log("Please fix API credentials. Stopping script.")
    sys.exit(1)

# =========================
# RSA-PSS Signing
# =========================
def sign_kalshi_message(message: str) -> str:
    if private_key is None:
        return ""
    try:
        sig = private_key.sign(
            message.encode("utf-8"),
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=hashes.SHA256().digest_size,
            ),
            hashes.SHA256(),
        )
        return base64.b64encode(sig).decode("utf-8")
    except Exception as e:
        Log(f"Signing error: {e}")
        return ""

# =========================
# Kalshi Signed HTTP Client
# =========================
def kalshi_signed_request(
    method: str,
    endpoint_path: str,
    body_json: str = "",
) -> Optional[dict]:
    timestamp = str(current_time_millis())
    sign_path = endpoint_path.split("?")[0]
    signature = sign_kalshi_message(timestamp + method + sign_path)
    if not signature:
        return None

    url = KALSHI_API_BASE + endpoint_path
    headers = {
        "KALSHI-ACCESS-KEY":       kalshi_api_key_id,
        "KALSHI-ACCESS-TIMESTAMP": timestamp,
        "KALSHI-ACCESS-SIGNATURE": signature,
        "Content-Type":            "application/json",
    }
    try:
        resp = _http_session.request(
            method, url, headers=headers,
            data=body_json if body_json else None,
            timeout=(1, 5),
        )
        return {"status": resp.status_code, "body": resp.text}
    except Exception as e:
        Log(f"HTTP error ({method} {endpoint_path}): {e}")
        return None

# =========================
# Market Data
# =========================
def _http_get(url: str) -> str:
    try:
        resp = _http_session.get(url, timeout=(1, 1.5))
        return resp.text if resp.status_code == 200 else ""
    except Exception:
        return ""

def _first_nonzero(market: dict, fields: list) -> Optional[float]:
    for f in fields:
        v = market.get(f)
        if v is not None and v != "" and v != 0:
            return float(v)
    return None

def _to_dollars(val: float) -> float:
    return val / 100.0 if val > 1.0 else val

import re as _re

def _parse_strike(ticker: str, market: dict) -> Optional[float]:
    """Extract strike price from market data or ticker string.
    Ticker format: KXBTC15M-25MAY0445-B94500  (B=above, T=below)
    """
    for field in ("floor_strike", "cap_strike", "strike_price", "strike"):
        v = market.get(field)
        if v is not None:
            try:
                return float(v)
            except (TypeError, ValueError):
                pass
    m = _re.search(r'-[BbTt](\d+(?:\.\d+)?)$', ticker)
    if m:
        return float(m.group(1))
    return None


def get_kalshi_market_snapshot(asset: str, max_retries: int = 4) -> Optional[dict]:
    series = ASSET_SERIES_MAP.get(asset)
    if not series:
        return None

    for attempt in range(1, max_retries + 1):
        url = (
            f"{KALSHI_API_BASE}/trade-api/v2/markets"
            f"?series_ticker={series}&status=open&limit=1"
            f"&_={current_time_millis()}-{attempt}"
        )
        body = _http_get(url)
        if body:
            try:
                data    = json.loads(body)
                markets = data.get("markets", [])
                if not markets:
                    if attempt < max_retries:
                        time.sleep(0.2)
                        continue
                    return None

                m = markets[0]

                yes_raw = _first_nonzero(m, [
                    "yes_ask_dollars", "yes_bid_dollars", "last_price_dollars",
                    "yes_ask", "yes_bid", "last_price",
                ])
                no_raw  = _first_nonzero(m, [
                    "no_ask_dollars", "no_bid_dollars",
                    "no_ask", "no_bid",
                ])

                yes_price = _to_dollars(yes_raw) if yes_raw is not None else None
                no_price  = _to_dollars(no_raw)  if no_raw  is not None else None

                if no_price  is None and yes_price is not None:
                    no_price  = 1.0 - yes_price
                if yes_price is None and no_price  is not None:
                    yes_price = 1.0 - no_price

                close_iso = next(
                    (m.get(f) for f in [
                        "close_time", "expected_expiration_time",
                        "expiration_time", "settlement_time",
                    ] if m.get(f)),
                    None,
                )
                mins_left = minutes_until_iso_utc(close_iso)

                if mins_left is not None and mins_left < 0.5:
                    if attempt < max_retries:
                        time.sleep(0.2)
                        continue

                if yes_price is not None and no_price is not None:
                    t = m.get("ticker", "")
                    return {
                        "up":            yes_price,
                        "down":          no_price,
                        "minutes_left":  mins_left,
                        "close_time":    close_iso,
                        "market_ticker": t,
                        "strike":        _parse_strike(t, m),
                    }
            except Exception as e:
                Log(f"Error parsing snapshot for {asset}: {e}")

        if attempt < max_retries:
            time.sleep(0.2)

    return None

# =========================
# Position Checks
# =========================
def has_existing_position(market_ticker: str, asset: str = "") -> bool:
    if not market_ticker:
        return False
    res = kalshi_signed_request(
        "GET", f"/trade-api/v2/portfolio/positions?ticker={market_ticker}"
    )
    if not res or res["status"] != 200:
        Log(f"POSITION check: HTTP {res['status'] if res else 'no response'} for {market_ticker}", asset=asset)
        return False
    try:
        data = json.loads(res["body"])
        for pos in data.get("market_positions", []):
            qty = pos.get("position", 0) or pos.get("position_fp", 0) or 0
            if abs(float(qty)) > 0:
                Log(f"POSITION response: {res['body'][:300]}", asset=asset)
                return True
        return False
    except Exception as e:
        Log(f"POSITION parse error: {e} | body: {res['body'][:200]}", asset=asset)
        return False

# =========================
# Order Placement
# =========================
def place_kalshi_order(
    action: str,
    market_ticker: str,
    side: str,
    price: float,
    size: int,
    client_order_id: str = "",
    asset: str = "",
) -> bool:
    if not ENABLE_LIVE_ORDERS:
        return True
    if not kalshi_api_key_id or private_key is None:
        return False
    if not market_ticker:
        return False

    is_buy   = action.lower() == "buy"
    api_side = "yes" if side == "UP" else "no"

    if is_buy:
        price = min(price + ENTRY_DIFF, 0.98)
        if price >= 0.99:
            Log(f"{market_ticker} Order aborted: buy price {round(price,2)} >= 0.99, market nearly resolved", asset=asset)
            return False
    else:
        price = 0.01  # aggressive sell — fill immediately at any bid

    cents       = max(1, min(98, round(price * 100)))
    price_field = "yes_price" if api_side == "yes" else "no_price"

    if not client_order_id:
        client_order_id = str(uuid.uuid4())

    payload: dict = {
        "ticker":          market_ticker,
        "action":          action.lower(),
        "side":            api_side,
        "count":           size,
        "type":            "limit",
        price_field:       cents,
        "client_order_id": client_order_id,
    }
    if not is_buy:
        payload["reduce_only"]   = True
        payload["time_in_force"] = "immediate_or_cancel"

    Log(f"ORDER payload: {json.dumps(payload)}", asset=asset)
    res = kalshi_signed_request("POST", "/trade-api/v2/portfolio/orders", json.dumps(payload))
    if not res:
        Log("ORDER response: no response", asset=asset)
        return False

    Log(f"ORDER response: HTTP {res['status']} | {res['body'][:300]}", asset=asset)

    order_id = ""
    try:
        body_data = json.loads(res["body"])
        order_id = (
            body_data.get("order", {}).get("order_id", "")
            or body_data.get("order_id", "")
        )
    except Exception:
        pass

    if res["status"] in (401, 403):
        Log(f"WARN: HTTP {res['status']} on order submit, will retry", asset=asset)
        return False
    if res["status"] not in (200, 201):
        return False

    if is_buy:
        time.sleep(REST_DELAY)

    has_pos = has_existing_position(market_ticker, asset=asset)

    if is_buy and not has_pos and order_id:
        kalshi_signed_request("DELETE", f"/trade-api/v2/portfolio/orders/{order_id}")
        time.sleep(0.5)
        has_pos = has_existing_position(market_ticker, asset=asset)
        if has_pos:
            Log(f"{market_ticker} Order filled during cancel race, treating as confirmed", asset=asset)

    return has_pos if is_buy else not has_pos

def buy_position(asset: str, market_ticker: str, side: str, price: float, retry: int = 1) -> bool:
    size = ASSET_ORDER_SIZE.get(asset, ORDER_SIZE)
    # last 2 retries: add extra 0.02 to break through thin order books
    extra = 0.02 if retry >= 3 else 0.0
    adj_price = min(price + extra, 0.98)
    return place_kalshi_order("BUY", market_ticker, side, adj_price, size, asset=asset)

def sell_position(asset: str, side: str, price: float) -> bool:
    pos = positions.get(asset)
    if not pos or not pos.get("ticker"):
        return True
    return place_kalshi_order("SELL", pos["ticker"], side, price, ORDER_SIZE, asset=asset)


def can_place_order_now(asset: str) -> bool:
    if not ENABLE_LIVE_ORDERS:
        return True
    return bool(kalshi_api_key_id and private_key is not None)

# =========================
# Per-Asset Processing
# =========================
_latest_snapshot: dict = {}   # asset -> last successful snapshot (for dashboard)
_latest_signal:   dict = {}   # asset -> last signal dict
_latest_filters:  dict = {}   # asset -> last filter evaluation (for dashboard)

def process_asset(asset: str):
    snapshot = get_kalshi_market_snapshot(asset)
    if not snapshot:
        log_once(asset, f"NO_DATA_{current_quarter_index()}", f"{asset} Waiting for market data")
        return

    ticker    = snapshot["market_ticker"]
    mins_left = snapshot["minutes_left"] if snapshot["minutes_left"] is not None else minutes_remaining_in_quarter()
    quarter   = snapshot["close_time"] or str(current_quarter_index())

    # Always publish the active ticker to the WS feed — covers mid-quarter restarts
    # where the new-session branch below wouldn't fire until next rollover.
    if ticker:
        _ws_ticker[asset] = ticker

    # Use WS prices when fresh (< 2s old), else REST snapshot
    ws = _ws_prices.get(asset)
    if ws and (time.time() - ws["ts"]) < 30.0:
        up, down = ws["up"], ws["down"]
    else:
        up, down = snapshot["up"], snapshot["down"]
        _contract_trackers.setdefault(asset, ContractPriceTracker()).push(up, down)

    ws_tag = " [WS]" if (ws and (time.time() - ws["ts"]) < 30.0) else " [REST]"

    # Cache latest live snapshot + signal for dashboard
    sig_live = get_signal(f"KX{asset}15M", strike=snapshot.get("strike")) if SIGNALS_AVAILABLE else None
    _latest_snapshot[asset] = {
        "ticker":      snapshot.get("market_ticker"),
        "strike":      snapshot.get("strike"),
        "close_time":  snapshot.get("close_time"),
        "up":          round(up, 4),
        "down":        round(down, 4),
        "minutes_left": mins_left,
        "source":      "WS" if (ws and (time.time() - ws["ts"]) < 30.0) else "REST",
    }
    if sig_live:
        _latest_signal[asset] = {
            "signal":           sig_live.signal,
            "conviction":       sig_live.conviction,
            "settlement_score": sig_live.settlement_score,
            "momentum":         sig_live.momentum,
            "velocity":         sig_live.velocity,
            "noise":            sig_live.noise_score,
            "minutes_left":     sig_live.minutes_left,
            "synthetic_price":  sig_live.synthetic_price,
            "reason":           sig_live.reason,
            "candle": {
                "open":  sig_live.candle.open,
                "high":  sig_live.candle.high,
                "low":   sig_live.candle.low,
                "close": sig_live.candle.close,
                "ticks": sig_live.candle.ticks,
            },
        }

    # Evaluate filters (shared logic — dashboard reads this from state.json)
    fair_p = None
    if SIGNALS_AVAILABLE and snapshot.get("strike"):
        btc_p = get_live_price(asset)
        if btc_p:
            e = d2_edge(btc_p, snapshot["strike"], up, mins_left,
                        ticks=get_ticks(asset), asset=asset, min_edge=0.03)
            fair_p = e["fair_prob"]
            # Periodic log of pricing inputs — once per minute via log_once key
            # to avoid flooding while still capturing values for post-mortem.
            side_lbl = "UP" if fair_p >= 0.5 else "DOWN"
            disp_p   = fair_p if fair_p >= 0.5 else 1 - fair_p
            log_once(asset, f"PRICING_{int(time.time() // 60)}",
                f"{asset} pricing{ws_tag}: BTC=${btc_p:,.2f} strike=${snapshot['strike']:,.2f} "
                f"mins={mins_left:.2f} vol={e['annualized_vol']*100:.1f}% "
                f"d2={e['d2']:+.2f} fair={disp_p*100:.1f}%{side_lbl} "
                f"market={up:.2f}/{down:.2f} edge_up={e['edge_up']:+.3f}")
    _latest_filters[asset] = evaluate_filters(
        up=up, down=down, mins_left=mins_left,
        signal=(sig_live.signal if sig_live else None),
        conviction=(sig_live.conviction if sig_live else None),
        settlement_score=(sig_live.settlement_score if sig_live else None),
        fair_prob=fair_p,
    )

    # ---- New session ----
    if asset not in asset_session_quarter or asset_session_quarter[asset] != quarter:
        asset_session_quarter[asset] = quarter
        asset_phase[asset]           = "WAIT_WINDOW"
        asset_reentry_count[asset]   = 0
        log_once_reset(asset)
        losses = consecutive_losses.get(asset, 0)
        prev_phase = asset_phase.get(asset)
        if prev_phase == "PAUSED":
            # one session of rest completed — reset and resume
            consecutive_losses[asset] = 0
            Log(f"{asset} ▶ Resuming after pause — loss streak reset", asset=asset)
        elif losses >= MAX_CONSECUTIVE_LOSSES:
            Log(f"{asset} ⏸ Paused — {losses} consecutive losses. Sitting out this session.", asset=asset)
            asset_phase[asset] = "PAUSED"
            _ws_ticker[asset] = ticker
            return
        Log(f"{asset} New session ({ticker})", asset=asset)
        _ws_ticker[asset] = ticker
        if ticker and has_existing_position(ticker, asset=asset):
            asset_phase[asset] = "HAS_POSITION"
        return

    # ---- Monitor open position ----
    pos = positions.get(asset)
    if pos:
        asset_phase[asset] = "IN_POSITION"

        cur = up if pos["side"] == "UP" else down

        # Quarter rolled → held to resolution
        if current_quarter_index() != pos["quarter_index"]:
            won = is_win(cur)  # contract near $1 = win
            sz = pos.get("size", ASSET_ORDER_SIZE.get(asset, ORDER_SIZE))
            pnl = compute_pnl(pos["entry"], 1.0 if won else 0.0, sz)
            if won:
                consecutive_losses[asset] = 0
                Log(f"{asset} {pos['side']} held to resolution ✓ win (+${pnl}) — streak reset", asset=asset)
            else:
                consecutive_losses[asset] = consecutive_losses.get(asset, 0) + 1
                n = consecutive_losses[asset]
                Log(f"{asset} {pos['side']} held to resolution ✗ loss (${pnl}) — consecutive={n}/{MAX_CONSECUTIVE_LOSSES}", asset=asset)
            journal("RESOLUTION", asset, side=pos["side"], entry=round(pos["entry"], 4),
                    exit=1.0 if won else 0.0, size=sz, won=won, pnl=pnl, ticker=pos.get("ticker"))
            del positions[asset]
            asset_phase[asset] = "WAIT_WINDOW"
            return

        # Stop-loss
        if cur <= EXIT:
            asset_phase[asset] = "STOP_LOSS"  # prevent re-entry on next poll
            Log(f"{asset} ⚡ stop-loss @ {fmt(cur)} (entry={fmt(pos['entry'])})", asset=asset)
            sold = False
            for i in range(1, 4):
                if sell_position(asset, pos["side"], cur):
                    Log(f"{asset} Position closed (stop-loss)", asset=asset)
                    sold = True
                    break
                cur = up if pos["side"] == "UP" else down  # refresh price each retry
                if i < 3:
                    time.sleep(1)
            if not sold:
                Log(f"{asset} Stop-loss sell failed — holding to resolution", asset=asset)
                asset_phase[asset] = "BUY_FAILED"
            sz = pos.get("size", ASSET_ORDER_SIZE.get(asset, ORDER_SIZE))
            pnl = compute_pnl(pos["entry"], cur, sz)
            journal("STOP_LOSS", asset, side=pos["side"], entry=round(pos["entry"], 4),
                    exit=round(cur, 4), size=sz, pnl=pnl, sold=sold, ticker=pos.get("ticker"))
            del positions[asset]
            consecutive_losses[asset] = consecutive_losses.get(asset, 0) + 1
            n = consecutive_losses[asset]
            Log(f"{asset} Stop-loss PnL: ${pnl} | Consecutive losses: {n}/{MAX_CONSECUTIVE_LOSSES}", asset=asset)
            asset_phase[asset] = "BUY_FAILED"
        else:
            log_once(asset, "HOLDING", f"{asset} Holding {pos['side']} @ {fmt(cur)}{ws_tag}")
        return

    # ---- Terminal phase guards ----
    phase = asset_phase.get(asset, "WAIT_WINDOW")
    if phase in ("BUY_FAILED", "STOP_LOSS"):
        if mins_left <= 2.0 and asset_reentry_count.get(asset, 0) < 1:
            log_once(asset, "LAST2_REENTRY", f"{asset} ⚡ Last-2m re-entry allowed (was {phase})")
        else:
            if asset_reentry_count.get(asset, 0) >= 1:
                log_once(asset, "REENTRY_LIMIT", f"{asset} Re-entry limit reached — no more entries this session")
            else:
                log_once(asset, "BUY_FAILED", f"{asset} No re-entry this session")
            return
    if phase == "PAUSED":
        log_once(asset, "PAUSED", f"{asset} ⏸ Paused ({consecutive_losses.get(asset,0)} losses) — resuming next session")
        return
    if phase == "HAS_POSITION":
        log_once(asset, "HAS_POSITION", f"{asset} Existing position — waiting for next session")
        return

    # ---- Wait for entry window ----
    if mins_left > TIME_WINDOW:
        log_once(asset, "WAIT_WINDOW", f"{asset} Waiting for last {TIME_WINDOW:.0f}m | {mins_left:.1f}m left")
        return

    # ---- Monitoring for entry ----
    if phase != "MONITORING":
        asset_phase[asset] = "MONITORING"
        log_once_keys.pop(f"{asset}|WAIT_WINDOW", None)

    # Get signal from signals.py
    sig = get_signal(f"KX{asset}15M", strike=snapshot.get("strike")) if SIGNALS_AVAILABLE else None

    log_once(asset, "MONITORING_LOG",
             f"{asset} Monitoring for entry > {ENTRY} | UP: {fmt(up)} DOWN: {fmt(down)}"
             f"{ws_tag} | Signal: {sig.signal if sig else 'N/A'}"
             + (f" conviction={sig.conviction:.2f}" if sig else ""))

    # ---- Entry condition ----
    side        = ""
    entry_price = 0.0

    if up >= ENTRY and up >= down:
        side, entry_price = "UP", up
    elif down >= ENTRY and down > up:
        side, entry_price = "DOWN", down

    if not side:
        return

    # Last-2-min fallback: skip signal check if enabled and price is strong enough
    bypass = LAST2_BYPASS_SIGNAL or (asset in LAST2_BYPASS_SIGNAL_ASSETS)
    if mins_left <= 2.0 and entry_price >= ENTRY_LAST2 and bypass:
        log_once(asset, f"LAST2_{side}",
                 f"{asset} ⚡ Last-2m entry {side} @ {fmt(entry_price)} (no signal required)")
    elif sig:
        # Normal signal confirmation
        if sig.signal != side:
            log_once(asset, f"SIG_DISAGREES_{side}",
                     f"{asset} ⚠ SIGNAL {sig.signal} — ss={sig.settlement_score:+.2f}")
            return
        # settlement_score must confirm direction
        ss = sig.settlement_score
        ss_ok = (ss >= MIN_SETTLEMENT_SCORE) if side == "UP" else (ss <= -MIN_SETTLEMENT_SCORE)
        if not ss_ok:
            log_once(asset, f"SS_WEAK_{side}",
                     f"{asset} ⚠ Settlement score too weak (ss={ss:+.2f}) for {side}")
            return
        if sig.conviction < MIN_CONVICTION:
            log_once(asset, f"CONV_WEAK_{side}",
                     f"{asset} ⚠ Conviction too weak ({sig.conviction:.2f} < {MIN_CONVICTION}) for {side}")
            return
        log_once_keys.pop(f"{asset}|SIG_DISAGREES_{side}", None)
        log_once_keys.pop(f"{asset}|SS_WEAK_{side}", None)
        log_once_keys.pop(f"{asset}|CONV_WEAK_{side}", None)
        log_once(asset, f"SIG_OK_{side}",
                 f"{asset} ✓ Signal {sig.signal} conviction={sig.conviction:.2f} ss={sig.settlement_score:+.2f}")

    if entry_price > 0.98:
        log_once(asset, "PRICE_HIGH", f"{asset} Price too high ({fmt(entry_price)}), skipping")
        return

    # ---- d2 Edge filter: only enter when fair value > market price ----
    # NOTE: d2_edge expects the YES (UP) price as contract_price — always pass `up`
    edge_info = None
    if USE_EDGE_FILTER and SIGNALS_AVAILABLE and snapshot.get("strike"):
        btc_p = get_live_price(asset)
        if btc_p:
            edge_info = d2_edge(
                btc_price=btc_p,
                strike=snapshot["strike"],
                contract_price=up,
                mins_left=mins_left,
                ticks=get_ticks(asset),
                asset=asset,
                min_edge=MIN_EDGE,
            )
            has_edge = edge_info["has_edge_up"] if side == "UP" else edge_info["has_edge_down"]
            edge_val = edge_info["edge_up"] if side == "UP" else edge_info["edge_down"]
            if not has_edge:
                log_once(asset, f"NO_EDGE_{side}",
                         f"{asset} ⚠ No edge for {side} — fair={edge_info['fair_prob']:.2f} "
                         f"market={fmt(entry_price)} edge={edge_val:+.3f} (need ≥ {MIN_EDGE})")
                return
            log_once_keys.pop(f"{asset}|NO_EDGE_{side}", None)
            Log(f"{asset} ✓ Edge OK {side}: fair={edge_info['fair_prob']:.2f} "
                f"market={fmt(entry_price)} edge={edge_val:+.3f}", asset=asset)

    if not can_place_order_now(asset):
        log_once(asset, "NO_CREDS", f"{asset} Missing credentials")
        return

    if has_existing_position(ticker, asset=asset):
        asset_phase[asset] = "HAS_POSITION"
        Log(f"{asset} Existing position found, waiting for next session", asset=asset)
        return

    sig_info = f" conviction={sig.conviction:.2f} ss={sig.settlement_score:+.2f}" if sig else ""
    Log(f"{asset} {side} triggered @ {fmt(entry_price)}{ws_tag}{sig_info} — placing order", asset=asset)
    buy_confirmed = False

    for i in range(1, 5):
        if i > 1 and has_existing_position(ticker, asset=asset):
            Log(f"{asset} Position detected before retry {i}, skipping", asset=asset)
            buy_confirmed = True
            break
        if i > 1:
            fresh = get_kalshi_market_snapshot(asset)
            if fresh:
                entry_price = fresh["up"] if side == "UP" else fresh["down"]
        if buy_position(asset, ticker, side, entry_price, retry=i):
            Log(f"{asset} Position confirmed", asset=asset)
            buy_confirmed = True
            break
        if i < 4:
            Log(f"{asset} Retry {i}/3", asset=asset)
            time.sleep(1)

    if buy_confirmed:
        prev_phase = asset_phase.get(asset)
        if prev_phase in ("BUY_FAILED", "STOP_LOSS"):
            asset_reentry_count[asset] = asset_reentry_count.get(asset, 0) + 1
        size = ASSET_ORDER_SIZE.get(asset, ORDER_SIZE)
        positions[asset] = {
            "side":          side,
            "entry":         entry_price,
            "ticker":        ticker,
            "quarter_index": current_quarter_index(),
            "size":          size,
        }
        asset_phase[asset] = "IN_POSITION"
        journal("ENTRY", asset, side=side, entry=round(entry_price, 4),
                size=size, ticker=ticker,
                cost=round(entry_price * size, 2),
                conviction=(sig.conviction if sig else None),
                settlement_score=(sig.settlement_score if sig else None),
                fair_prob=(edge_info["fair_prob"] if edge_info else None),
                edge=(edge_info["edge_up"] if edge_info and side == "UP" else
                      edge_info["edge_down"] if edge_info else None))
        Log(f"{asset} Holding {side} @ {fmt(entry_price)} — stop-loss @ {EXIT}", asset=asset)
    else:
        asset_phase[asset] = "BUY_FAILED"
        Log(f"{asset} Order not filled after 4 attempts, skipping session", asset=asset)

# =========================
# Entry Point
# =========================
def main():
    global ASSETS

    # ---- Parse CLI args ----
    valid = set(ASSET_SERIES_MAP.keys())
    args  = [a.upper() for a in sys.argv[1:] if a.upper() in valid]
    if args:
        ASSETS = args

    pid_suffix = "_".join(a.lower() for a in sorted(ASSETS)) if ASSETS else "all"
    pid_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), f"bot_{pid_suffix}.pid")
    if os.path.exists(pid_file):
        with open(pid_file) as f:
            old_pid = f.read().strip()
        try:
            os.kill(int(old_pid), 0)
            print(f"bot.py already running (PID {old_pid}). Exiting.")
            sys.exit(1)
        except (OSError, ValueError):
            pass
    with open(pid_file, "w") as f:
        f.write(str(os.getpid()))
    import atexit
    atexit.register(lambda: os.path.exists(pid_file) and os.remove(pid_file))

    load_kalshi_credentials()
    load_persistent()
    sizes = {a: ASSET_ORDER_SIZE.get(a, ORDER_SIZE) for a in ASSETS}
    Log(
        f"Momentum Bot Started | "
        f"entry={ENTRY} sl={EXIT} window={TIME_WINDOW:.0f}m sizes={sizes} | "
        f"Assets: {ASSETS} | WS={'enabled' if WS_AVAILABLE else 'DISABLED'}"
    )
    startup_credential_check()

    if SIGNALS_AVAILABLE:
        start_feed(ASSETS)

    # Start persistent WebSocket feed per asset
    for asset in ASSETS:
        start_ws_feed(asset)

    poll_secs = POLL_INTERVAL_MS / 1000.0
    while True:
        for asset in ASSETS:
            try:
                process_asset(asset)
            except Exception as e:
                Log(f"Unhandled error processing {asset}: {e}")

        # Dump shared state for dashboard
        try:
            state = {
                "ts": time.time(),
                "assets": ASSETS,
                "snapshots": _latest_snapshot,
                "signals":   _latest_signal,
                "filters":   _latest_filters,
                "positions": positions,
                "phases": asset_phase,
                "consecutive_losses": consecutive_losses,
                "max_consecutive_losses": MAX_CONSECUTIVE_LOSSES,
                "config": {
                    "ENTRY": ENTRY, "ENTRY_LAST2": ENTRY_LAST2, "EXIT": EXIT,
                    "TIME_WINDOW": TIME_WINDOW,
                    "MIN_SETTLEMENT_SCORE": MIN_SETTLEMENT_SCORE,
                    "MIN_CONVICTION": MIN_CONVICTION,
                    "MIN_EDGE": MIN_EDGE,
                    "USE_EDGE_FILTER": USE_EDGE_FILTER,
                    "ASSET_ORDER_SIZE": ASSET_ORDER_SIZE,
                },
            }
            write_state(state)
            save_persistent()
        except Exception as e:
            Log(f"State write error: {e}")

        time.sleep(poll_secs)


if __name__ == "__main__":
    main()
