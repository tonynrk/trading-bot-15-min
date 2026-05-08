#!/usr/bin/env python3
"""
Recover trades.jsonl from bot_btc.log.

Parses ORDER payloads (entries) and resolution/stop-loss log lines (exits)
to reconstruct trade journal entries matching the schema written by bot.journal().

Usage:
    python3 recover_trades.py [--log bot_btc.log] [--out trades.recovered.jsonl]
"""
import argparse, json, re
from datetime import datetime, timezone

TS_RE = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})")
ORDER_PAYLOAD_RE = re.compile(r"ORDER payload: (\{.*\})")
RESOLUTION_WIN_RE = re.compile(
    r"(\w+) (UP|DOWN) held to resolution .*win \(\+\$([0-9.]+)\)"
)
RESOLUTION_LOSS_RE = re.compile(
    r"(\w+) (UP|DOWN) held to resolution .*loss \(\$?-?([0-9.]+)\)"
)
STOPLOSS_RE = re.compile(r"Stop-loss PnL: \$?(-?[0-9.]+)")
SIGNAL_OK_RE = re.compile(
    r"(\w+) ✓ Signal (UP|DOWN) conviction=([0-9.]+) ss=([+\-][0-9.]+)"
)
EDGE_OK_RE = re.compile(
    r"(\w+) ✓ Edge OK (UP|DOWN): fair=([0-9.]+) market=([0-9.]+) edge=([+\-][0-9.]+)"
)


def parse_ts(line: str) -> float | None:
    m = TS_RE.match(line)
    if not m:
        return None
    # log times are local — assume UTC offset matches when bot runs; treat as UTC
    dt = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    return dt.timestamp()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--log", default="bot_btc.log")
    ap.add_argument("--out", default="trades.recovered.jsonl")
    ap.add_argument("--asset", default="BTC")
    args = ap.parse_args()

    out_lines = []
    # track last entry per ticker so we can emit RESOLUTION with entry price
    open_pos: dict[str, dict] = {}
    # latest signal/edge context per (asset, side) — attached to next ENTRY
    sig_ctx: dict[tuple, dict] = {}
    edge_ctx: dict[tuple, dict] = {}

    with open(args.log, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            ts = parse_ts(line)
            if ts is None:
                continue
            iso = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()

            ms_sig = SIGNAL_OK_RE.search(line)
            if ms_sig:
                a, sd, conv, ss = ms_sig.groups()
                sig_ctx[(a, sd)] = {"conviction": float(conv), "settlement_score": float(ss)}
                continue

            me = EDGE_OK_RE.search(line)
            if me:
                a, sd, fair, _mkt, edge = me.groups()
                edge_ctx[(a, sd)] = {"fair_prob": float(fair), "edge": float(edge)}
                continue

            m = ORDER_PAYLOAD_RE.search(line)
            if m:
                try:
                    p = json.loads(m.group(1))
                except Exception:
                    continue
                if p.get("action") != "buy":
                    continue
                side_yn = p.get("side")  # 'yes' or 'no'
                ticker = p.get("ticker", "")
                count = p.get("count", 0)
                price_cents = p.get("yes_price") if side_yn == "yes" else p.get("no_price")
                if price_cents is None:
                    continue
                entry = round(price_cents / 100.0, 4)
                side = "UP" if side_yn == "yes" else "DOWN"
                cost = round(entry * count, 4)
                sc = sig_ctx.pop((args.asset, side), {})
                ec = edge_ctx.pop((args.asset, side), {})
                rec = {
                    "ts": ts, "iso": iso, "event": "ENTRY", "asset": args.asset,
                    "side": side, "entry": entry, "size": count, "ticker": ticker,
                    "cost": cost,
                    "conviction": sc.get("conviction"),
                    "settlement_score": sc.get("settlement_score"),
                    "fair_prob": ec.get("fair_prob"),
                    "edge": ec.get("edge"),
                }
                out_lines.append(rec)
                open_pos[ticker_key(ticker)] = rec
                continue

            mw = RESOLUTION_WIN_RE.search(line)
            ml = RESOLUTION_LOSS_RE.search(line)
            if mw or ml:
                won = bool(mw)
                m2 = mw or ml
                side = m2.group(2)
                pnl = float(m2.group(3)) * (1 if won else -1)
                # find matching open pos by side (latest)
                pos = find_open(open_pos, side)
                if not pos:
                    continue
                rec = {
                    "ts": ts, "iso": iso, "event": "RESOLUTION", "asset": args.asset,
                    "side": pos["side"], "entry": pos["entry"],
                    "exit": 1.0 if won else 0.0, "size": pos["size"],
                    "won": won, "pnl": round(pnl, 4), "ticker": pos["ticker"],
                }
                out_lines.append(rec)
                open_pos.pop(ticker_key(pos["ticker"]), None)
                continue

            ms = STOPLOSS_RE.search(line)
            if ms:
                pnl = float(ms.group(1))
                # ambiguous side — pick any open
                pos = next(iter(open_pos.values()), None)
                if not pos:
                    continue
                rec = {
                    "ts": ts, "iso": iso, "event": "STOP_LOSS", "asset": args.asset,
                    "side": pos["side"], "entry": pos["entry"],
                    "exit": None, "size": pos["size"],
                    "pnl": round(pnl, 4), "sold": None, "ticker": pos["ticker"],
                }
                out_lines.append(rec)
                open_pos.pop(ticker_key(pos["ticker"]), None)

    out_lines.sort(key=lambda r: r["ts"])
    with open(args.out, "w", encoding="utf-8") as f:
        for r in out_lines:
            f.write(json.dumps(r) + "\n")

    n_entry = sum(1 for r in out_lines if r["event"] == "ENTRY")
    n_res = sum(1 for r in out_lines if r["event"] == "RESOLUTION")
    n_sl = sum(1 for r in out_lines if r["event"] == "STOP_LOSS")
    print(f"Recovered: {n_entry} ENTRY, {n_res} RESOLUTION, {n_sl} STOP_LOSS → {args.out}")


def ticker_key(t: str) -> str:
    return t


def find_open(open_pos: dict, side: str):
    for k, v in reversed(list(open_pos.items())):
        if v["side"] == side:
            return v
    return None


if __name__ == "__main__":
    main()
