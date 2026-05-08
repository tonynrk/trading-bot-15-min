"""
Test WebSocket price feed in signals.py
Run: python test_signals_ws.py [BTC|ETH]
"""
import sys
import time
import logging

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s | %(message)s", datefmt="%H:%M:%S")

asset = sys.argv[1].upper() if len(sys.argv) > 1 else "BTC"

from signals import start_feed, get_signal, _trackers, _ws_last_push_ts as _ws_last_push

print(f"Starting WS feed for {asset}...")
start_feed([asset])

print("Waiting 5s for connections...")
time.sleep(5)

last_tick_count = 0
for i in range(30):
    tracker = _trackers.get(asset)
    ws_age  = time.time() - _ws_last_push.get(asset, 0)
    sig     = get_signal(f"KX{asset}15M")

    if tracker:
        _, price, sources, ticks = tracker.snapshot()
        tick_count = len(ticks)
        new_ticks  = tick_count - last_tick_count
        last_tick_count = tick_count

        src_str = "  ".join(f"{k}=${v:,.2f}" for k, v in sources.items() if v)
        ws_str  = f"WS age={ws_age:.1f}s" if ws_age < 9999 else "WS: no data yet"

        if sig:
            print(f"[{i+1:02d}] price=${price:,.2f} | {src_str} | ticks={tick_count} (+{new_ticks}) | {ws_str}")
            print(f"      signal={sig.signal} conviction={sig.conviction:.3f} noise={sig.noise_score:.3f} vel={sig.velocity:+.2f}$/s")
        else:
            print(f"[{i+1:02d}] price=${price:,.2f} | {ws_str} | ticks={tick_count} (+{new_ticks}) | signal=None (warming up)")
    else:
        print(f"[{i+1:02d}] No tracker yet")

    time.sleep(1)

print("\nDone.")
