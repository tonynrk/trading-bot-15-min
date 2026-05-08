"""
Synthetic BRTI Signal Engine
------------------------------
Sources BTC/ETH prices from Kraken WS (fallback: REST composite).
Builds real-time 15-min candles and computes trading signals.

Standalone:
    python3 signals.py

Integration with bot.py:
    from signals import start_feed, get_signal, SignalResult
"""

import asyncio
import json
import logging
import math
import ssl
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import certifi
import requests
import websockets

_log = logging.getLogger(__name__)

# =============================================================================
# Configuration
# =============================================================================
CANDLE_MINUTES       = 15
POLL_INTERVAL_S      = 1.0
VELOCITY_WINDOW_S    = 30
NOISE_RANGE_MIN_PCT  = 0.0008   # below this → too tight (no movement)
CONVICTION_THRESHOLD = 0.45   # raised from 0.30 — only emit UP/DOWN when signal is strong

# Free price sources per asset
ASSET_SOURCES = {
    "BTC": {
        "kraken":   "https://api.kraken.com/0/public/Ticker?pair=XBTUSD",
        "coinbase": "https://api.coinbase.com/v2/prices/BTC-USD/spot",
        "bitstamp": "https://www.bitstamp.net/api/v2/ticker/btcusd/",
    },
    "ETH": {
        "kraken":   "https://api.kraken.com/0/public/Ticker?pair=ETHUSD",
        "coinbase": "https://api.coinbase.com/v2/prices/ETH-USD/spot",
        "bitstamp": "https://www.bitstamp.net/api/v2/ticker/ethusd/",
    },
}
# default for backwards compat
SOURCES = ASSET_SOURCES["BTC"]

# =============================================================================
# Data structures
# =============================================================================
@dataclass
class Tick:
    price: float
    ts:    float    # unix seconds

@dataclass
class Candle:
    open:   float
    high:   float
    low:    float
    close:  float
    ticks:  int  = 0
    period: int  = 0

@dataclass
class SignalResult:
    # Live price
    synthetic_price:    float   # averaged across sources
    source_prices:      dict    # {"kraken": x, ...}

    # Current 15-min candle
    candle:             Candle
    minutes_left:       float

    # Raw metrics
    momentum:           float   # -1..+1  (candle direction × body strength)
    velocity:           float   # $/sec over last 30s
    position_score:     float   # 0..1  (0=at candle low, 1=at candle high)
    noise_score:        float   # 0..1  (1=choppy/doji)
    body_strength:      float   # 0..1  (candle body size relative to range)
    last_min_trend:     float   # -1..+1  price direction in last 60s

    # Settlement factor (Black-Scholes inspired)
    settlement_score:   float   # -1..+1  (>0 = BTC above strike, time-adjusted)

    # Prior candle context
    prior_candle_dir:   float   # -1=down, 0=flat, +1=up (previous completed candle)

    # Composite
    conviction:         float   # 0..1
    signal:             str     # "UP" | "DOWN" | "NEUTRAL"
    reason:             str

# =============================================================================
# Time helpers
# =============================================================================
def _utc_now() -> datetime:
    return datetime.now(timezone.utc)

def _quarter_index(dt: Optional[datetime] = None) -> int:
    dt = dt or _utc_now()
    return (dt.hour * 60 + dt.minute) // CANDLE_MINUTES

def _minutes_remaining() -> float:
    now = _utc_now()
    return CANDLE_MINUTES - (now.minute % CANDLE_MINUTES) - (now.second / 60.0)


# =============================================================================
# Free price fetchers
# =============================================================================
_fetch_session = requests.Session()
_fetch_session.headers["User-Agent"] = "Mozilla/5.0"

def _fetch_url_kraken(url: str) -> Optional[float]:
    try:
        r = _fetch_session.get(url, timeout=1.5)
        if r.status_code == 200:
            data = r.json()
            result = data.get("result", {})
            pair = next(iter(result.values()), {})
            return float(pair["c"][0])
    except Exception:
        pass
    return None

def _fetch_url_coinbase(url: str) -> Optional[float]:
    try:
        r = _fetch_session.get(url, timeout=1.5)
        if r.status_code == 200:
            return float(r.json()["data"]["amount"])
    except Exception:
        pass
    return None



def _fetch_url_bitstamp(url: str) -> Optional[float]:
    try:
        r = _fetch_session.get(url, timeout=1.5)
        if r.status_code == 200:
            return float(r.json()["last"])
    except Exception:
        pass
    return None


def fetch_composite_price(asset: str = "BTC") -> tuple[Optional[float], dict]:
    """Fetch all sources for the given asset in parallel."""
    sources = ASSET_SOURCES.get(asset, ASSET_SOURCES["BTC"])
    results: dict[str, Optional[float]] = {}
    threads = []
    lock = threading.Lock()

    def _fetch(name, url):
        if "kraken" in name:
            p = _fetch_url_kraken(url)
        elif "bitstamp" in name:
            p = _fetch_url_bitstamp(url)
        else:
            p = _fetch_url_coinbase(url)
        with lock:
            results[name] = p

    for name, url in sources.items():
        t = threading.Thread(target=_fetch, args=(name, url), daemon=True)
        t.start()
        threads.append(t)
    for t in threads:
        t.join(timeout=2)

    valid = {k: v for k, v in results.items() if v is not None}
    if not valid:
        return None, results
    composite = sum(valid.values()) / len(valid)
    return composite, results

# =============================================================================
# Candle + settlement tracker
# =============================================================================
class CandleTracker:
    def __init__(self):
        self._lock         = threading.Lock()
        self._candle: Optional[Candle] = None
        self._prev_candle: Optional[Candle] = None
        self._ticks:  deque[Tick]      = deque(maxlen=30000)  # ~75min @ 6/s — fits 60m vol window
        self._last_price               = 0.0
        self._source_prices: dict      = {}

    def push(self, price: float, sources: dict, ts: Optional[float] = None):
        ts  = ts or time.time()
        q   = _quarter_index()
        with self._lock:
            self._last_price    = price
            self._source_prices = sources
            if self._candle is None or self._candle.period != q:
                # save completed candle before starting new one
                if self._candle is not None:
                    c = self._candle
                    self._prev_candle = Candle(
                        open=c.open, high=c.high, low=c.low,
                        close=c.close, ticks=c.ticks, period=c.period,
                    )
                self._candle = Candle(
                    open=price, high=price, low=price, close=price,
                    ticks=1, period=q,
                )
            else:
                c = self._candle
                c.high  = max(c.high,  price)
                c.low   = min(c.low,   price)
                c.close = price
                c.ticks += 1
            self._ticks.append(Tick(price=price, ts=ts))

    def snapshot(self) -> tuple[Optional[Candle], float, dict, deque, Optional[Candle]]:
        with self._lock:
            c = self._candle
            snap = Candle(open=c.open, high=c.high, low=c.low,
                          close=c.close, ticks=c.ticks, period=c.period) if c else None
            prev = self._prev_candle
            prev_snap = Candle(open=prev.open, high=prev.high, low=prev.low,
                               close=prev.close, ticks=prev.ticks, period=prev.period) if prev else None
            return snap, self._last_price, dict(self._source_prices), deque(self._ticks), prev_snap

# =============================================================================
# Signal computation
# =============================================================================
def _velocity(ticks: deque) -> float:
    now    = time.time()
    cutoff = now - VELOCITY_WINDOW_S
    recent = [t for t in ticks if t.ts >= cutoff]
    if len(recent) < 2:
        return 0.0
    dt = recent[-1].ts - recent[0].ts
    return (recent[-1].price - recent[0].price) / dt if dt > 0.5 else 0.0

def _last_min_trend(ticks: deque) -> float:
    """Direction of price over the last 60 seconds, normalised to -1..+1."""
    now    = time.time()
    recent = [t for t in ticks if t.ts >= now - 60]
    if len(recent) < 2:
        return 0.0
    span   = recent[-1].price - recent[0].price
    rng    = max(t.price for t in recent) - min(t.price for t in recent)
    return max(-1.0, min(span / rng, 1.0)) if rng > 1 else 0.0

def _noise_score(candle: Candle, ticks: deque) -> float:
    # tight: range too narrow = market not moving
    tight = 0.0
    if candle.open > 0:
        rng_pct = (candle.high - candle.low) / candle.open
        tight = 1.0 - min(rng_pct / NOISE_RANGE_MIN_PCT, 1.0)
    # chop: price reversing frequently = no clear direction
    recent = list(ticks)[-60:]
    revs   = sum(
        1 for i in range(2, len(recent))
        if (recent[i-1].price - recent[i-2].price) *
           (recent[i].price   - recent[i-1].price) < 0
    )
    chop = revs / max(len(recent) - 2, 1)
    return round(min(0.5 * tight + 0.5 * chop, 1.0), 3)


_DEFAULT_VOL  = {"BTC": 0.55, "ETH": 0.85}
_VEL_NORM     = {"BTC": 50.0, "ETH": 5.0}   # $/sec = "extreme" per asset

def _norm_cdf(x: float) -> float:
    """Standard normal CDF via math.erfc — exact, no scipy needed."""
    return 0.5 * math.erfc(-x / math.sqrt(2))

def _settlement_score(price: float, strike: float, mins_left: float, asset: str) -> float:
    """
    N(d2): risk-neutral probability BTC finishes above strike.
    Returns -1..+1 (mapped from 0..1): >0 = above strike (UP favoured).
    0.0 if strike unknown.
    """
    if not strike or strike <= 0 or mins_left <= 0 or price <= 0:
        return 0.0
    vol = _DEFAULT_VOL.get(asset, 0.55)
    T   = mins_left / 525_960.0          # mins → fraction of year
    sigma_sqrt_T = vol * math.sqrt(T)
    if sigma_sqrt_T < 1e-9:
        return 0.0
    d2 = math.log(price / strike) / sigma_sqrt_T
    # map N(d2) from 0..1 to -1..+1 so neutral (ATM) = 0
    return round(2 * _norm_cdf(d2) - 1.0, 3)

def compute_signal(
    tracker: CandleTracker,
    market_ticker: str = "",
    strike: Optional[float] = None,
) -> Optional[SignalResult]:
    candle, price, sources, ticks, prev_candle = tracker.snapshot()
    if candle is None or price == 0:
        return None

    mins_left = _minutes_remaining()
    rng       = candle.high - candle.low

    body          = abs(candle.close - candle.open)
    body_strength = min(body / rng, 1.0) if rng > 1 else 0.0
    direction     = 1.0 if candle.close >= candle.open else -1.0
    momentum      = round(direction * body_strength, 3)
    velocity      = round(_velocity(ticks), 2)
    noise         = _noise_score(candle, ticks)
    lmt           = round(_last_min_trend(ticks), 3)
    position_score = (price - candle.low) / rng if rng > 1 else 0.5

    asset = _asset_from_ticker(market_ticker)
    ss    = _settlement_score(price, strike, mins_left, asset)

    prior_candle_dir = 0.0
    if prev_candle is not None:
        prior_candle_dir = 1.0 if prev_candle.close >= prev_candle.open else -1.0

    # direction from ss; each indicator scores 0..1 alignment with ss direction
    ss_dir = 1.0 if ss >= 0 else -1.0

    vel_scale = _VEL_NORM.get(asset, 50.0)
    vel_norm  = max(-1.0, min(velocity / vel_scale, 1.0))

    ss_score  = abs(ss)
    mom_score = max(0.0, momentum   * ss_dir)
    vel_score = max(0.0, vel_norm   * ss_dir)
    pos_score = position_score if ss_dir > 0 else (1.0 - position_score)
    body_score = body_strength

    # weighted conviction — ss leads, others confirm
    raw = (
        0.35 * ss_score
      + 0.25 * mom_score
      + 0.20 * vel_score
      + 0.10 * pos_score
      + 0.10 * body_score
    )
    conviction = round(raw * (1.0 - noise * 0.4), 3)

    if noise > 0.75:
        signal = "NEUTRAL"
        reason = f"noisy (noise={noise:.2f})"
    elif conviction < CONVICTION_THRESHOLD:
        signal = "NEUTRAL"
        reason = f"low conviction ({conviction:.2f})"
    elif ss > 0:
        signal = "UP"
        reason = f"ss={ss:+.2f} mom={mom_score:.2f} vel={vel_score:.2f} pos={pos_score:.2f} body={body_score:.2f}"
    elif ss < 0:
        signal = "DOWN"
        reason = f"ss={ss:+.2f} mom={mom_score:.2f} vel={vel_score:.2f} pos={pos_score:.2f} body={body_score:.2f}"
    else:
        signal = "NEUTRAL"
        reason = f"ATM (ss={ss:+.2f})"

    return SignalResult(
        synthetic_price   = round(price, 2),
        source_prices     = {k: round(v, 2) for k, v in sources.items() if v},
        candle            = candle,
        minutes_left      = round(mins_left, 2),
        momentum          = momentum,
        velocity          = velocity,
        position_score    = round(position_score, 3),
        noise_score       = noise,
        body_strength     = round(body_strength, 3),
        last_min_trend    = lmt,
        settlement_score  = ss,
        prior_candle_dir  = prior_candle_dir,
        conviction        = conviction,
        signal            = signal,
        reason            = reason,
    )

# =============================================================================
# Module-level singletons (one tracker per asset)
# =============================================================================
_trackers: dict[str, CandleTracker] = {}
_running = False
_threads: list[threading.Thread] = []

# ── WS state per asset ───────────────────────────────────────────────────────
_ws_last_push_ts: dict[str, float] = {}  # asset -> last time we pushed to tracker
_ws_lock_sig = threading.Lock()
_WS_FALLBACK_AFTER_S  = 5.0
_WS_MIN_PUSH_INTERVAL = 0.15   # max ~6 pushes/sec
_WS_MIN_PRICE_CHANGE  = 0.10   # ignore sub-$0.10 updates

def _ws_maybe_push(asset: str, tracker: CandleTracker, price: float):
    """Push Kraken price to tracker if price moved enough or time elapsed."""
    with _ws_lock_sig:
        now        = time.time()
        last_ts    = _ws_last_push_ts.get(asset, 0)
        last_price = tracker.snapshot()[1]

        time_ok  = (now - last_ts) >= _WS_MIN_PUSH_INTERVAL
        price_ok = abs(price - last_price) >= _WS_MIN_PRICE_CHANGE
        if not (time_ok or price_ok):
            return

        tracker.push(price, {"kraken": round(price, 2)})
        _ws_last_push_ts[asset] = now

# ── WebSocket symbols per exchange ───────────────────────────────────────────
_KRAKEN_SYMBOL = {"BTC": "BTC/USD", "ETH": "ETH/USD"}

_ssl_ctx = ssl.create_default_context(cafile=certifi.where())

async def _kraken_ws(asset: str, tracker: CandleTracker):
    symbol = _KRAKEN_SYMBOL.get(asset)
    if not symbol:
        return
    while _running:
        try:
            async with websockets.connect("wss://ws.kraken.com/v2", ssl=_ssl_ctx, ping_interval=20) as ws:
                await ws.send(json.dumps({
                    "method": "subscribe",
                    "params": {"channel": "ticker", "symbol": [symbol]},
                }))
                async for raw in ws:
                    if not _running:
                        return
                    try:
                        data = json.loads(raw)
                        if data.get("channel") == "ticker":
                            for item in data.get("data", []):
                                p = item.get("last")
                                if p:
                                    _ws_maybe_push(asset, tracker, float(p))
                    except Exception:
                        pass
        except Exception as e:
            _log.debug(f"Kraken WS ({asset}): {e} — reconnecting")
            await asyncio.sleep(3)

async def _ws_feed_async(assets: list):
    tasks = [_kraken_ws(asset, _trackers[asset]) for asset in assets]
    await asyncio.gather(*tasks)

def _ws_feed_thread(assets: list):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(_ws_feed_async(assets))

def _rest_fallback_loop(asset: str):
    """REST polling kicks in only when WS has been silent too long."""
    while _running:
        age = time.time() - _ws_last_push_ts.get(asset, 0)
        if age >= _WS_FALLBACK_AFTER_S:
            try:
                price, sources = fetch_composite_price(asset)
                if price:
                    _trackers[asset].push(price, sources)
                    _log.debug(f"REST fallback push ({asset}) — WS silent {age:.1f}s")
            except Exception as e:
                _log.debug(f"REST fallback error ({asset}): {e}")
        time.sleep(POLL_INTERVAL_S)

def start_feed(assets: list = None):
    global _running, _threads
    if _running:
        return
    if assets is None:
        assets = ["BTC"]
    _running = True
    for asset in assets:
        if asset not in _trackers:
            _trackers[asset] = CandleTracker()

    # WebSocket feed (Kraken + Coinbase, composite push)
    t_ws = threading.Thread(target=_ws_feed_thread, args=(assets,), name="signals-ws", daemon=True)
    t_ws.start()
    _threads.append(t_ws)

    # REST fallback per asset (only fires when WS is silent)
    for asset in assets:
        t = threading.Thread(target=_rest_fallback_loop, args=(asset,), name=f"signals-rest-{asset}", daemon=True)
        t.start()
        _threads.append(t)

    _log.info(f"Signal feed started for {assets} (WS: Kraken, REST fallback)")

def stop_feed():
    global _running
    _running = False

def _asset_from_ticker(market_ticker: str) -> str:
    if "ETH" in market_ticker:
        return "ETH"
    return "BTC"

def get_signal(market_ticker: str = "", strike: Optional[float] = None) -> Optional[SignalResult]:
    asset = _asset_from_ticker(market_ticker)
    tracker = _trackers.get(asset)
    if tracker is None:
        tracker = _trackers.get("BTC")
    if tracker is None:
        return None
    return compute_signal(tracker, market_ticker, strike)

def d2_edge(
    btc_price: float,
    strike: float,
    contract_price: float,   # the YES (UP) price, 0..1
    mins_left: float,
    ticks: Optional[deque] = None,
    asset: str = "BTC",
    min_edge: float = 0.03,
) -> dict:
    """
    Black-Scholes d2 probability filter for binary markets.

    Returns:
        d2          – standardised distance to strike
        fair_prob   – P(BTC > strike at expiry) via Normal CDF
        edge_up     – fair_prob - contract_price  (>0 = UP is cheap)
        edge_down   – (1 - fair_prob) - (1 - contract_price)
        has_edge_up / has_edge_down – edge > min_edge
    """
    if strike <= 0 or btc_price <= 0:
        return {
            "d2": 0.0, "fair_prob": 0.5,
            "edge_up": 0.0, "edge_down": 0.0,
            "has_edge_up": False, "has_edge_down": False,
            "annualized_vol": _DEFAULT_VOL.get(asset, 0.80),
        }
    # At/after expiry: outcome is deterministic — collapse to 1.0 or 0.0
    # rather than 0.5, which would falsely signal a coin-flip in the final seconds.
    if mins_left <= 0.05:
        fair_prob = 1.0 if btc_price > strike else 0.0
        return {
            "d2": 0.0, "fair_prob": fair_prob,
            "edge_up": round(fair_prob - contract_price, 4),
            "edge_down": round((1.0 - fair_prob) - (1.0 - contract_price), 4),
            "has_edge_up": False, "has_edge_down": False,
            "annualized_vol": _DEFAULT_VOL.get(asset, 0.80),
        }

    # Realised vol from 1-minute close-to-close returns over the last 15 minutes.
    # Mirrors TradingView Polymarket Quant indicator's default "Volatility Lookback: 15"
    # (15 bars on 1-min chart). Reference indicator description: uses ATR/StDev length 15.
    # Statistical caveat: ~14 returns gives noisy σ (±27% standard error) but matches TV's
    # reactivity. TV is the trusted reference — accept the noise to keep numbers aligned.
    annualized_vol = _DEFAULT_VOL.get(asset, 0.55)
    if ticks:
        cutoff = time.time() - 900           # 15 min window (matches TV default)
        # Group ticks by minute-bucket and take the last price in each bucket
        # as that minute's "close". dict insertion order preserves time order.
        closes_by_min: dict[int, float] = {}
        for t in ticks:
            if t.ts >= cutoff:
                closes_by_min[int(t.ts // 60)] = t.price
        # Use only consecutive minutes — gaps would inflate the return at the gap.
        keys = sorted(closes_by_min.keys())
        rets: list[float] = []
        for i in range(1, len(keys)):
            if keys[i] == keys[i-1] + 1:
                p_prev = closes_by_min[keys[i-1]]
                p_curr = closes_by_min[keys[i]]
                if p_prev > 0:
                    rets.append(math.log(p_curr / p_prev))
        if len(rets) >= 5:
            mean_r   = sum(rets) / len(rets)
            variance = sum((r - mean_r) ** 2 for r in rets) / (len(rets) - 1)
            # Annualize: per-minute variance × minutes-per-year
            annualized_vol = max(0.05, math.sqrt(variance * 525_960.0))

    T  = mins_left / 525_960.0          # mins → fraction of year
    d2 = math.log(btc_price / strike) / (annualized_vol * math.sqrt(T))
    fair_prob  = _norm_cdf(d2)
    edge_up    = round(fair_prob - contract_price, 4)
    edge_down  = round((1.0 - fair_prob) - (1.0 - contract_price), 4)

    return {
        "d2":            round(d2, 3),
        "fair_prob":     round(fair_prob, 3),
        "edge_up":       edge_up,
        "edge_down":     edge_down,
        "has_edge_up":   edge_up   >= min_edge,
        "has_edge_down": edge_down >= min_edge,
        "annualized_vol": round(annualized_vol, 3),
    }

def get_live_price(asset: str = "BTC") -> Optional[float]:
    tracker = _trackers.get(asset)
    if tracker is None:
        return None
    _, price, _, _, _ = tracker.snapshot()
    return price if price > 0 else None


def get_ticks(asset: str = "BTC"):
    """Return recent tick deque for realized vol calc in d2_edge."""
    tracker = _trackers.get(asset)
    if tracker is None:
        return None
    _, _, _, ticks, _ = tracker.snapshot()
    return ticks

# =============================================================================
# Standalone dashboard
# =============================================================================
_C = {"UP": "\033[92m", "DOWN": "\033[91m", "NEUTRAL": "\033[93m",
      "RESET": "\033[0m", "BOLD": "\033[1m", "DIM": "\033[2m"}

def _bar(val: float, width: int = 24, lo: float = 0.0, hi: float = 1.0) -> str:
    pct    = max(0.0, min((val - lo) / max(hi - lo, 1e-9), 1.0))
    filled = round(pct * width)
    return "█" * filled + "░" * (width - filled)


def _dashboard(sig: SignalResult):
    print("\033[2J\033[H", end="")
    c   = sig.candle
    col = _C.get(sig.signal, "")
    now = _utc_now().strftime("%Y-%m-%d %H:%M:%S UTC")
    B, R, D = _C["BOLD"], _C["RESET"], _C["DIM"]

    print(f"{B}{'─'*58}{R}")
    print(f"  Synthetic BRTI  │  {now}")
    print(f"{B}{'─'*58}{R}")

    # Sources
    src_str = "  ".join(f"{k}=${v:,.2f}" for k, v in sig.source_prices.items())
    print(f"  Sources  : {D}{src_str}{R}")
    print(f"  Composite: {B}${sig.synthetic_price:>12,.2f}{R}")
    print()

    # Candle
    body_dir = "▲" if c.close >= c.open else "▼"
    print(f"  Candle ({c.ticks} ticks, {sig.minutes_left:.1f}m left)")
    print(f"    O:{c.open:>10,.2f}  H:{c.high:>10,.2f}")
    print(f"    C:{c.close:>10,.2f}  L:{c.low:>10,.2f}  {body_dir}")
    print()

    # Metrics
    print(f"  {'Momentum':16} {sig.momentum:>+6.3f}  {_bar(sig.momentum, lo=-1, hi=1)}")
    print(f"  {'Velocity $/s':16} {sig.velocity:>+6.1f}  {_bar(sig.velocity, lo=-50, hi=50)}")
    print(f"  {'Position':16} {sig.position_score:>6.3f}  {_bar(sig.position_score)}")
    print(f"  {'Body Strength':16} {sig.body_strength:>6.3f}  {_bar(sig.body_strength)}")
    print(f"  {'Noise':16} {sig.noise_score:>6.3f}  {_bar(sig.noise_score)}")
    print(f"  {'Last-min Trend':16} {sig.last_min_trend:>+6.3f}  {_bar(sig.last_min_trend, lo=-1, hi=1)}")
    print(f"  {'Conviction':16} {sig.conviction:>6.3f}  {_bar(sig.conviction)}")
    print()
    print(f"  Signal  : {col}{B}{sig.signal:>7}{R}")
    print(f"  Reason  : {sig.reason}")
    print(f"{B}{'─'*58}{R}")


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.WARNING)

    asset = sys.argv[1].upper() if len(sys.argv) > 1 else "BTC"
    if asset not in ASSET_SOURCES:
        print(f"Unknown asset '{asset}'. Available: {list(ASSET_SOURCES.keys())}")
        sys.exit(1)

    start_feed([asset])
    print(f"Waiting for first {asset} prices…")
    time.sleep(2.5)

    try:
        while True:
            sig = get_signal(f"KX{asset}15M")
            if sig:
                _dashboard(sig)
            else:
                print("\rWaiting for data…", end="", flush=True)
            time.sleep(1)
    except KeyboardInterrupt:
        stop_feed()
        print("\nStopped.")
