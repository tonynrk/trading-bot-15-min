"""
Quick arbitrage scanner — checks if YES + NO < 1.00 on Kalshi BTC 15m.
Run: python3 test_arb.py
"""
import json, time, sys
sys.path.insert(0, ".")
from bot import load_kalshi_credentials, get_kalshi_market_snapshot, get_kalshi_orderbook_imbalance, _to_dollars, kalshi_signed_request, current_time_millis

load_kalshi_credentials()

print("Scanning Kalshi BTC 15m for arbitrage (15 min)...\n")
print(f"{'Time':8}  {'Ticker':30}  {'YES':6}  {'NO':6}  {'SUM':6}  {'GAP':7}  {'OB':7}")
print("─" * 80)

start = time.time()
min_sum = 1.0
max_gap = 0.0

while time.time() - start < 900:
    snap = get_kalshi_market_snapshot("BTC")
    if snap:
        yes   = snap["up"]
        no    = snap["down"]
        s     = round(yes + no, 4)
        gap   = round(1.0 - s, 4)
        ticker = snap["market_ticker"]
        ob    = get_kalshi_orderbook_imbalance(ticker)
        ob_str = f"{ob:+.3f}" if ob is not None else "  n/a"
        mins  = snap["minutes_left"] or 0

        if s < min_sum:
            min_sum = s
        if gap > max_gap:
            max_gap = gap

        arb = " ← ARB!" if gap > 0.01 else ""
        t = time.strftime("%H:%M:%S")
        print(f"{t}  {ticker:30}  {yes:.3f}  {no:.3f}  {s:.4f}  {gap:+.4f}  {ob_str}  {mins:.1f}m left{arb}")
    else:
        print(f"{time.strftime('%H:%M:%S')}  No market data")

    time.sleep(5)

print(f"\n── Summary ──")
print(f"Min pair sum : {min_sum:.4f}")
print(f"Max gap      : {max_gap:.4f}  {'← arbitrage opportunity!' if max_gap > 0.01 else '← no arb found'}")
