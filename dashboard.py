"""
Web Dashboard for Kalshi BTC Bot
---------------------------------
Standalone process. Reuses signals.py for live BTC/ETH price + signal,
polls Kalshi public REST for contract prices, tails bot_<asset>.log
for trade events.

Run on VPS:
    pip install fastapi uvicorn
    python3 dashboard.py            # listens 0.0.0.0:8000

View from MacBook:
    http://<VPS_IP>:8000
"""

import asyncio
import json
import os
import sys
import time
from collections import deque
from typing import Optional

import requests
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
import uvicorn

from signals import start_feed, get_signal, d2_edge, get_live_price, get_ticks
from filter_logic import evaluate_filters, compute_z_score, EXIT_PRICE, MIN_EDGE

# ---- Config ---------------------------------------------------------------
ASSETS = ["BTC"]
PORT = 8080   # matches old project's web_dashboard.py — Windows Firewall already open
KALSHI_API_BASE = "https://api.elections.kalshi.com"
ASSET_SERIES_MAP = {"BTC": "KXBTC15M", "ETH": "KXETH15M"}
LOG_DIR = os.path.dirname(os.path.abspath(__file__))

app = FastAPI()

# ---- Live snapshot from bot.py shared state ------------------------------
STATE_FILE = os.path.join(LOG_DIR, "bot_state.json")

def load_bot_state() -> dict:
    """Read shared state written by bot.py — single source of truth."""
    if not os.path.exists(STATE_FILE):
        return {}
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def fetch_kalshi(asset: str) -> Optional[dict]:
    """Read live Kalshi snapshot from bot.py's shared state file (real-time WS data)."""
    state = load_bot_state()
    snap = state.get("snapshots", {}).get(asset)
    if not snap:
        return None
    return {
        "ticker":     snap.get("ticker", ""),
        "up":         snap.get("up", 0),
        "down":       snap.get("down", 0),
        "close_time": snap.get("close_time"),
        "strike":     snap.get("strike"),
        "source":     snap.get("source"),
        "minutes_left": snap.get("minutes_left"),
    }


def tail_log(asset: str, n: int = 30) -> list:
    path = os.path.join(LOG_DIR, f"bot_{asset.lower()}.log")
    if not os.path.exists(path):
        return []
    try:
        with open(path, "rb") as f:
            f.seek(0, 2)
            size = f.tell()
            chunk = min(size, 16384)
            f.seek(size - chunk)
            data = f.read().decode("utf-8", errors="ignore")
        lines = [ln for ln in data.splitlines() if ln.strip()]
        return lines[-n:]
    except Exception:
        return []


JOURNAL_FILE = os.path.join(LOG_DIR, "trades.jsonl")

def load_journal() -> list:
    if not os.path.exists(JOURNAL_FILE):
        return []
    out = []
    try:
        with open(JOURNAL_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        out.append(json.loads(line))
                    except Exception:
                        pass
    except Exception:
        pass
    return out


def compute_stats(asset: str) -> dict:
    records = [r for r in load_journal() if r.get("asset") == asset]
    entries = [r for r in records if r["event"] == "ENTRY"]
    exits   = [r for r in records if r["event"] in ("STOP_LOSS", "RESOLUTION")]

    today_start = time.time() - 86400
    today_exits = [r for r in exits if r["ts"] >= today_start]

    wins = [r for r in exits if (r.get("won") is True) or (r.get("pnl", 0) > 0)]
    losses = [r for r in exits if r not in wins]

    total_pnl  = round(sum(r.get("pnl", 0) for r in exits), 2)
    today_pnl  = round(sum(r.get("pnl", 0) for r in today_exits), 2)
    win_rate   = round(100 * len(wins) / len(exits), 1) if exits else 0.0

    today_contracts = sum(r.get("size", 0) for r in today_exits)
    total_contracts = sum(r.get("size", 0) for r in exits)

    recent = exits[::-1]   # all exits, newest first

    return {
        "n_entries":       len(entries),
        "n_exits":         len(exits),
        "n_wins":          len(wins),
        "n_losses":        len(losses),
        "win_rate":        win_rate,
        "total_pnl":       total_pnl,
        "today_pnl":       today_pnl,
        "today_n":         len(today_exits),
        "today_contracts": today_contracts,
        "total_contracts": total_contracts,
        "recent":          recent,
    }


def find_open_position(asset: str) -> Optional[dict]:
    """Find last ENTRY without matching exit (STOP_LOSS or RESOLUTION)."""
    records = [r for r in load_journal() if r.get("asset") == asset]
    last_entry = None
    for r in records:
        if r["event"] == "ENTRY":
            last_entry = r
        elif r["event"] in ("STOP_LOSS", "RESOLUTION"):
            last_entry = None
    return last_entry


def compute_filters(asset: str, snap: Optional[dict], sig) -> dict:
    """Use shared filter_logic.evaluate_filters — single source of truth."""
    if not snap or not sig:
        return {"checks": [], "all_pass": False}

    # Compute fair_prob + realized vol via signals.d2_edge (uses realized vol from ticks)
    fair_prob = None
    realized_vol = None
    z_score = None
    if snap.get("strike"):
        btc_p = get_live_price(asset)
        ticks = get_ticks(asset)
        if btc_p:
            e = d2_edge(btc_p, snap["strike"], snap["up"], sig.minutes_left,
                        ticks=ticks, asset=asset, min_edge=MIN_EDGE)
            fair_prob = e["fair_prob"]
            realized_vol = e.get("annualized_vol")
            z_score = compute_z_score(btc_p, snap["strike"], sig.minutes_left, realized_vol)

    result = evaluate_filters(
        up=snap["up"], down=snap["down"],
        mins_left=sig.minutes_left,
        signal=sig.signal,
        conviction=sig.conviction,
        settlement_score=sig.settlement_score,
        fair_prob=fair_prob,
    )
    result["z_score"] = z_score
    result["realized_vol"] = realized_vol
    return result


def build_state(asset: str) -> dict:
    bot_state = load_bot_state()
    snap = fetch_kalshi(asset)

    # Prefer bot's cached signal (computed at bot's tick rate); fall back to live re-compute
    sig_dict = bot_state.get("signals", {}).get(asset)
    sig = get_signal(f"KX{asset}15M", strike=snap.get("strike") if snap else None)
    if sig and not sig_dict:
        sig_dict = {
            "signal": sig.signal, "conviction": sig.conviction,
            "settlement_score": sig.settlement_score, "momentum": sig.momentum,
            "velocity": sig.velocity, "noise": sig.noise_score,
            "minutes_left": sig.minutes_left, "synthetic_price": sig.synthetic_price,
            "reason": sig.reason,
            "candle": {"open": sig.candle.open, "high": sig.candle.high,
                       "low": sig.candle.low, "close": sig.candle.close,
                       "ticks": sig.candle.ticks},
        }

    # Open position from bot's authoritative state (real-time, includes size/side/quarter)
    bot_pos = bot_state.get("positions", {}).get(asset)
    open_pos = None
    if bot_pos and snap:
        cur_price = snap["up"] if bot_pos["side"] == "UP" else snap["down"]
        size = bot_pos.get("size", 0)
        entry = bot_pos.get("entry", 0)
        live_pnl = round((cur_price - entry) * size, 2)
        open_pos = {
            **bot_pos,
            "current_price": cur_price,
            "live_pnl": live_pnl,
            "distance_to_sl": round(cur_price - EXIT_PRICE, 3),
        }
    elif not bot_pos:
        # Fallback to journal scan if bot state unavailable
        je = find_open_position(asset)
        if je and snap:
            cur_price = snap["up"] if je["side"] == "UP" else snap["down"]
            live_pnl = round((cur_price - je["entry"]) * je.get("size", 0), 2)
            open_pos = {**je, "current_price": cur_price, "live_pnl": live_pnl,
                        "distance_to_sl": round(cur_price - EXIT_PRICE, 3),
                        "hold_seconds": int(time.time() - je["ts"])}

    # Prefer bot's filter result (computed every loop with WS data); fall back to local
    bot_filters = bot_state.get("filters", {}).get(asset)
    filters = bot_filters if bot_filters else compute_filters(asset, snap, sig)
    # Always enrich with z-score + realized vol (needed by HTML even when bot didn't compute)
    if filters and "z_score" not in filters:
        local = compute_filters(asset, snap, sig)
        filters["z_score"] = local.get("z_score")
        filters["realized_vol"] = local.get("realized_vol")

    return {
        "asset": asset,
        "ts": time.time(),
        "kalshi": snap,
        "signal": sig_dict,
        "log": tail_log(asset, 25),
        "stats": compute_stats(asset),
        "filters": filters,
        "position": open_pos,
        "bot_phase": bot_state.get("phases", {}).get(asset),
        "loss_streak": bot_state.get("consecutive_losses", {}).get(asset, 0),
        "max_losses": bot_state.get("max_consecutive_losses", 2),
        "bot_age": round(time.time() - bot_state.get("ts", 0), 1) if bot_state.get("ts") else None,
        "config": bot_state.get("config", {}),
    }


# ---- WebSocket: push state every 1s ---------------------------------------
@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    try:
        while True:
            payload = {a: build_state(a) for a in ASSETS}
            await ws.send_text(json.dumps(payload, default=str))
            await asyncio.sleep(1.0)
    except WebSocketDisconnect:
        pass
    except Exception:
        pass


# ---- Single-page HTML -----------------------------------------------------
HTML = """<!DOCTYPE html>
<html><head>
<meta charset="utf-8"><title>Kalshi Bot Dashboard</title>
<style>
  body { background:#0e1116; color:#d8dee4; font-family:'SF Mono',Menlo,monospace; padding:20px; margin:0; }
  h1 { font-size:18px; margin:0 0 12px; color:#fff; }
  .grid { display:grid; grid-template-columns:1fr 1fr; gap:16px; max-width:1100px; }
  .card { background:#161b22; border:1px solid #2a3038; border-radius:8px; padding:14px; }
  .card h2 { font-size:13px; margin:0 0 10px; color:#8b949e; text-transform:uppercase; letter-spacing:0.5px; }
  .row { display:flex; justify-content:space-between; padding:4px 0; font-size:13px; }
  .row span:first-child { color:#8b949e; }
  .row span:last-child { color:#e6edf3; font-weight:500; }
  .big { font-size:32px; font-weight:600; color:#fff; margin:6px 0; }
  .pill { display:inline-block; padding:3px 10px; border-radius:12px; font-size:12px; font-weight:600; }
  .UP { background:#1a4d2e; color:#3fb950; }
  .DOWN { background:#4d1a1a; color:#f85149; }
  .NEUTRAL { background:#3a3a1a; color:#d4a72c; }
  .bar { height:6px; background:#21262d; border-radius:3px; overflow:hidden; margin-top:3px; }
  .bar > div { height:100%; background:#3fb950; transition:width 0.3s; }
  .log { font-size:11px; max-height:280px; overflow-y:auto; line-height:1.5; color:#a5acb5; }
  .log div { white-space:pre-wrap; padding:1px 0; border-bottom:1px solid #1a1f26; }
  .stale { opacity:0.4; }
</style>
</head><body>
<h1>📊 Kalshi BTC Bot Dashboard <span id="ts" style="color:#8b949e;font-weight:normal;font-size:12px"></span></h1>
<div id="root"></div>
<script>
function pct(v, lo, hi) { return Math.max(0, Math.min(100, (v-lo)/(hi-lo)*100)); }
function fmt(v, d=3) { return v==null ? "—" : Number(v).toFixed(d); }
function color(v) { return v>0 ? "#3fb950" : v<0 ? "#f85149" : "#8b949e"; }

function render(data) {
  const root = document.getElementById("root");
  const html = Object.values(data).map(s => {
    const k = s.kalshi || {};
    const sig = s.signal || {};
    const conv = sig.conviction || 0;
    const ss = sig.settlement_score || 0;
    return `
    <div class="grid">
      <div class="card">
        <h2>${s.asset} — Kalshi Contract <span style="font-size:10px;color:${k.source==='WS'?'#3fb950':'#d4a72c'}">[${k.source||'?'}]</span></h2>
        <div class="big">UP ${fmt(k.up,2)} / DOWN ${fmt(k.down,2)}</div>
        <div class="row"><span>Ticker</span><span style="font-size:10px">${k.ticker||"—"}</span></div>
        <div class="row"><span>Strike</span><span>${k.strike||"—"}</span></div>
        <div class="row"><span>Mins left</span><span>${fmt(sig.minutes_left,1)}</span></div>
        <div class="row"><span>Live BTC</span><span>$${fmt(sig.synthetic_price,2)}</span></div>
        <div class="row"><span>Bot phase</span><span>${s.bot_phase||'—'}</span></div>
        <div class="row"><span>Loss streak</span><span style="color:${s.loss_streak>=s.max_losses?'#f85149':s.loss_streak>0?'#d4a72c':'#3fb950'}">${s.loss_streak||0} / ${s.max_losses}</span></div>
        <div class="row"><span>Bot heartbeat</span><span style="color:${s.bot_age!=null && s.bot_age<3?'#3fb950':'#f85149'}">${s.bot_age!=null?s.bot_age+'s ago':'STALE'}</span></div>
      </div>
      <div class="card">
        <h2>Signal</h2>
        <div class="big"><span class="pill ${sig.signal||'NEUTRAL'}">${sig.signal||"…"}</span></div>
        <div class="row"><span>Conviction</span><span>${fmt(conv,3)}</span></div>
        <div class="bar"><div style="width:${pct(conv,0,1)}%"></div></div>
        <div class="row"><span>Settlement</span><span style="color:${color(ss)}">${ss>=0?'+':''}${fmt(ss,3)}</span></div>
        <div class="row"><span>Momentum</span><span style="color:${color(sig.momentum)}">${fmt(sig.momentum,3)}</span></div>
        <div class="row"><span>Velocity</span><span>${fmt(sig.velocity,2)} $/s</span></div>
        <div class="row"><span>Noise</span><span>${fmt(sig.noise,3)}</span></div>
        <div class="row" style="margin-top:6px"><span>Reason</span><span style="font-size:10px">${sig.reason||"—"}</span></div>
      </div>
      <div class="card" style="grid-column:1/-1">
        <h2>Entry Filter Checklist ${s.filters?.all_pass ? '<span style="color:#3fb950">✓ ALL PASS — entry would fire</span>' : ''}</h2>
        ${(s.filters?.checks||[]).map(c=>`
          <div class="row"><span style="color:${c.pass?'#3fb950':'#f85149'}">${c.pass?'✓':'✗'} ${c.name}</span><span>${c.value}</span></div>
        `).join("")}
        ${s.filters?.fair_prob != null ? (() => {
          const fp = s.filters.fair_prob;            // P(UP) by convention
          const upWin = fp >= 0.5;
          const dispProb = upWin ? fp : (1 - fp);
          const dispSide = upWin ? 'UP' : 'DOWN';
          const zRaw = s.filters.z_score;
          // Signed Z: positive when UP wins, negative when DOWN wins
          const zStr = zRaw != null ? `${zRaw>=0?'+':''}${zRaw.toFixed(2)}σ ${dispSide}` : '—';
          const sideColor = upWin ? '#3fb950' : '#f85149';
          return `
          <div style="margin-top:8px;padding-top:8px;border-top:1px solid #2a3038">
            <div class="row"><span>Fair Prob</span><span style="color:${sideColor};font-weight:600">${(dispProb*100).toFixed(1)}% ${dispSide}</span></div>
            <div class="row"><span>Z-Score</span><span style="color:${sideColor}">${zStr}</span></div>
            <div class="row"><span>Market Price</span><span>${fmt(s.filters.market_price,2)} (${s.filters.side})</span></div>
            <div class="row"><span>Edge</span><span style="color:${color(s.filters.edge)};font-weight:600">${s.filters.edge>=0?'+':''}${fmt(s.filters.edge,3)}</span></div>
            <div class="row"><span>Realized Vol (15m)</span><span>${s.filters.realized_vol!=null?(s.filters.realized_vol*100).toFixed(0)+'%':'—'}</span></div>
          </div>`;
        })() : ''}
      </div>
      ${s.position ? `
      <div class="card" style="grid-column:1/-1;border:1px solid #d4a72c">
        <h2>🟡 Open Position</h2>
        <div class="big">${s.position.side} @ ${fmt(s.position.entry,2)} <span style="font-size:14px;color:#8b949e">× ${s.position.size}</span></div>
        <div class="row"><span>Current price</span><span>${fmt(s.position.current_price,2)}</span></div>
        <div class="row"><span>Live PnL</span><span style="color:${color(s.position.live_pnl)};font-size:16px;font-weight:600">$${fmt(s.position.live_pnl,2)}</span></div>
        <div class="row"><span>Distance to SL (0.50)</span><span>${fmt(s.position.distance_to_sl,3)}</span></div>
        <div class="row"><span>Held</span><span>${Math.floor(s.position.hold_seconds/60)}m ${s.position.hold_seconds%60}s</span></div>
        <div class="row"><span>Entry conviction</span><span>${fmt(s.position.conviction,2)}</span></div>
        <div class="row"><span>Entry edge</span><span>${s.position.edge!=null?fmt(s.position.edge,3):'—'}</span></div>
      </div>` : ''}
      <div class="card" style="grid-column:1/-1">
        <h2>P&L / Trade Stats</h2>
        <div class="row"><span>Total P&L (all)</span><span style="color:${color(s.stats?.total_pnl||0)};font-size:18px;font-weight:600">$${fmt(s.stats?.total_pnl,2)}</span></div>
        <div class="row"><span>Today P&L</span><span style="color:${color(s.stats?.today_pnl||0)}">$${fmt(s.stats?.today_pnl,2)} (${s.stats?.today_n||0} trades)</span></div>
        <div class="row"><span>Trades</span><span>${s.stats?.n_exits||0} closed (${s.stats?.n_entries||0} entries)</span></div>
        <div class="row"><span>Contracts traded</span><span>${s.stats?.today_contracts||0} today · ${s.stats?.total_contracts||0} total</span></div>
        <div class="row"><span>Order size (per trade)</span><span>${(s.config?.ASSET_ORDER_SIZE||{})[s.asset]||'—'} contracts</span></div>
        <div class="row"><span>Win rate</span><span>${fmt(s.stats?.win_rate,1)}% (${s.stats?.n_wins||0}W / ${s.stats?.n_losses||0}L)</span></div>
        <div style="margin-top:10px;font-size:11px;color:#8b949e">Recent trades:</div>
        <div style="margin-top:4px;font-size:10px;color:#6e7681">Showing all ${(s.stats?.recent||[]).length} closed trades (scroll)</div>
        <div class="log" style="max-height:360px">${(s.stats?.recent||[]).map(r=>{
          const pnl = r.pnl||0;
          const c = pnl>0?'#3fb950':pnl<0?'#f85149':'#8b949e';
          // iso is NY-labeled wall clock (e.g. "2026-05-08T00:39:11-04:00"); display date + H:M:S as-is
          let t;
          if (r.iso) {
            const [datePart, rest] = r.iso.split('T');
            const tm = rest.split(/[+-]/)[0].slice(0,8);
            const [hh,mm,ss] = tm.split(':');
            const h = parseInt(hh,10);
            const ampm = h >= 12 ? 'PM' : 'AM';
            const h12 = ((h + 11) % 12) + 1;
            const [yy,mo,dd] = datePart.split('-');
            t = `${parseInt(mo,10)}/${parseInt(dd,10)} ${h12}:${mm}:${ss} ${ampm}`;
          } else {
            t = new Date(r.ts*1000).toLocaleString();
          }
          return `<div>${t} · ${r.event} ${r.side||''} entry=${fmt(r.entry,2)} exit=${fmt(r.exit,2)} size=${r.size} <span style="color:${c}">$${fmt(pnl,2)}</span></div>`;
        }).join("")}</div>
      </div>
      <div class="card" style="grid-column:1/-1">
        <h2>Candle (15m)</h2>
        <div class="row"><span>Open</span><span>$${fmt(sig.candle?.open,2)}</span></div>
        <div class="row"><span>High</span><span style="color:#3fb950">$${fmt(sig.candle?.high,2)}</span></div>
        <div class="row"><span>Low</span><span style="color:#f85149">$${fmt(sig.candle?.low,2)}</span></div>
        <div class="row"><span>Close</span><span>$${fmt(sig.candle?.close,2)}</span></div>
        <div class="row"><span>Ticks</span><span>${sig.candle?.ticks||0}</span></div>
      </div>
      <div class="card" style="grid-column:1/-1">
        <h2>Bot Log (recent)</h2>
        <div class="log">${(s.log||[]).map(l=>`<div>${l.replace(/</g,'&lt;')}</div>`).reverse().join("")}</div>
      </div>
    </div>`;
  }).join("");
  root.innerHTML = html;
}

let ws;
function connect() {
  ws = new WebSocket(`ws://${location.host}/ws`);
  ws.onmessage = e => {
    document.getElementById("ts").textContent = "· live · " + new Date().toLocaleTimeString();
    document.body.classList.remove("stale");
    render(JSON.parse(e.data));
  };
  ws.onclose = () => {
    document.body.classList.add("stale");
    setTimeout(connect, 2000);
  };
}
connect();
</script>
</body></html>"""


@app.get("/", response_class=HTMLResponse)
async def root():
    return HTML


# ---- Main -----------------------------------------------------------------
def main():
    global ASSETS
    valid = set(ASSET_SERIES_MAP.keys())
    args = [a.upper() for a in sys.argv[1:] if a.upper() in valid]
    if args:
        ASSETS = args

    print(f"Starting signal feed for {ASSETS}…")
    start_feed(ASSETS)
    time.sleep(2)

    # Detect VPS public IP + LAN IP
    public_ip = "?"
    try:
        public_ip = requests.get("https://api.ipify.org", timeout=3).text.strip()
    except Exception:
        try:
            public_ip = requests.get("https://ifconfig.me", timeout=3).text.strip()
        except Exception:
            pass

    lan_ip = "?"
    try:
        import socket
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        lan_ip = s.getsockname()[0]
        s.close()
    except Exception:
        pass

    print()
    print("=" * 60)
    print(f"  Dashboard ready on port {PORT}")
    print("=" * 60)
    print(f"  Public (from MacBook):  http://{public_ip}:{PORT}")
    print(f"  LAN/local:              http://{lan_ip}:{PORT}")
    print(f"  Localhost (on VPS):     http://127.0.0.1:{PORT}")
    print("=" * 60)
    print()
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="warning")


if __name__ == "__main__":
    main()
