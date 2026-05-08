"""
Unit tests for math + filter logic.
Run:  python3 test_math.py
"""
import math
import sys

# Make module imports work from any cwd
sys.path.insert(0, ".")

from signals import _norm_cdf, _settlement_score, d2_edge
from filter_logic import (
    evaluate_filters, compute_z_score, compute_pnl, is_win, pick_side,
)

# ────────────────────────────────────────────────────────────────────────────
# Tiny test framework
# ────────────────────────────────────────────────────────────────────────────
PASS = 0
FAIL = 0

def approx(a, b, tol=1e-3):
    return abs(a - b) <= tol

def assert_eq(name, actual, expected, tol=None):
    global PASS, FAIL
    ok = approx(actual, expected, tol) if tol is not None else (actual == expected)
    if ok:
        PASS += 1
        print(f"  ✓ {name}")
    else:
        FAIL += 1
        print(f"  ✗ {name}\n      expected {expected}\n      got      {actual}")

def section(title):
    print(f"\n── {title} " + "─" * (60 - len(title)))


# ────────────────────────────────────────────────────────────────────────────
# 1. Normal CDF
# ────────────────────────────────────────────────────────────────────────────
section("Normal CDF")
assert_eq("N(0) = 0.5",          _norm_cdf(0),     0.5,    tol=1e-9)
assert_eq("N(-∞) → 0",           _norm_cdf(-10),   0.0,    tol=1e-6)
assert_eq("N(+∞) → 1",           _norm_cdf(10),    1.0,    tol=1e-6)
assert_eq("N(-1.96) ≈ 0.025",    _norm_cdf(-1.96), 0.025,  tol=1e-3)
assert_eq("N(+1.96) ≈ 0.975",    _norm_cdf(1.96),  0.975,  tol=1e-3)
assert_eq("N(-1.72) ≈ 0.043",    _norm_cdf(-1.72), 0.043,  tol=1e-3)


# ────────────────────────────────────────────────────────────────────────────
# 2. Settlement score
# ────────────────────────────────────────────────────────────────────────────
section("Settlement score (signed -1..+1)")
# At-the-money → ss ≈ 0
assert_eq("ATM → 0",
          _settlement_score(80000, 80000, 5, "BTC"),
          0.0, tol=1e-3)

# BTC well above strike with little time → ss → +1
ss_above = _settlement_score(80500, 80000, 1, "BTC")
assert_eq("BTC >> strike, 1m → ss high (>0.5)",
          ss_above > 0.5, True)

# BTC well below strike → ss → -1
ss_below = _settlement_score(79500, 80000, 1, "BTC")
assert_eq("BTC << strike, 1m → ss low (<-0.5)",
          ss_below < -0.5, True)

# Strike unknown → 0
assert_eq("No strike → 0",
          _settlement_score(80000, 0, 5, "BTC"),
          0.0)


# ────────────────────────────────────────────────────────────────────────────
# 3. d2_edge — the buggy area
# ────────────────────────────────────────────────────────────────────────────
section("d2_edge")
# Case: BTC slightly above strike, ATM-ish, 5 mins left
e = d2_edge(80000, 80000, contract_price=0.50, mins_left=5, asset="BTC")
assert_eq("ATM fair_prob ≈ 0.5", e["fair_prob"], 0.5, tol=0.01)
# edge_up + edge_down should sum to 0 (always — it's by construction)
assert_eq("edge_up + edge_down = 0", e["edge_up"] + e["edge_down"], 0, tol=1e-3)

# Case: market overpriced for UP (UP is 0.80 but fair only 0.50)
e2 = d2_edge(80000, 80000, contract_price=0.80, mins_left=5, asset="BTC")
assert_eq("UP overpriced → edge_up negative", e2["edge_up"] < 0, True)
assert_eq("UP overpriced → edge_down positive", e2["edge_down"] > 0, True)
assert_eq("has_edge_up False", e2["has_edge_up"], False)

# Case: market underpriced for UP (UP is 0.40 but fair 0.50)
e3 = d2_edge(80000, 80000, contract_price=0.40, mins_left=5, asset="BTC")
assert_eq("UP underpriced → edge_up positive", e3["edge_up"] > 0, True)
assert_eq("UP underpriced → has_edge_up True", e3["has_edge_up"], True)


# ────────────────────────────────────────────────────────────────────────────
# 4. evaluate_filters — entry filter chain
# ────────────────────────────────────────────────────────────────────────────
section("evaluate_filters")

# All-pass UP entry
f = evaluate_filters(
    up=0.85, down=0.15, mins_left=4.0,
    signal="UP", conviction=0.65, settlement_score=0.5,
    fair_prob=0.90,                    # fair > market → edge +0.05 ≥ 0.03 ✓
)
assert_eq("All-pass UP", f["all_pass"], True)
assert_eq("Picked UP",   f["side"], "UP")

# Fail: price < 0.82
f = evaluate_filters(
    up=0.78, down=0.22, mins_left=4.0,
    signal="UP", conviction=0.65, settlement_score=0.5, fair_prob=0.90,
)
assert_eq("Price below entry → fail", f["all_pass"], False)
price_check = next(c for c in f["checks"] if c["name"].startswith("Price"))
assert_eq("Price check explicitly fails", price_check["pass"], False)

# Fail: signal disagrees
f = evaluate_filters(
    up=0.85, down=0.15, mins_left=4.0,
    signal="DOWN", conviction=0.65, settlement_score=0.5, fair_prob=0.90,
)
assert_eq("Signal mismatch → fail", f["all_pass"], False)

# Fail: edge negative (overpriced)
f = evaluate_filters(
    up=0.90, down=0.10, mins_left=4.0,
    signal="UP", conviction=0.65, settlement_score=0.5,
    fair_prob=0.85,                    # fair < market → edge -0.05
)
assert_eq("UP overpriced → fail", f["all_pass"], False)
edge_check = next(c for c in f["checks"] if c["name"].startswith("Edge"))
assert_eq("Edge check fails", edge_check["pass"], False)

# DOWN side picked correctly: market DOWN ≥ entry, fair_UP low → edge_down positive
f = evaluate_filters(
    up=0.15, down=0.85, mins_left=4.0,
    signal="DOWN", conviction=0.65, settlement_score=-0.5,
    fair_prob=0.10,                    # market UP=0.15, fair UP=0.10 → edge_down = up - fair = +0.05
)
assert_eq("Picked DOWN side", f["side"], "DOWN")
assert_eq("DOWN entry passes all", f["all_pass"], True)


# ────────────────────────────────────────────────────────────────────────────
# 5. PnL math
# ────────────────────────────────────────────────────────────────────────────
section("PnL")
# Win at resolution: bought at 0.82, resolved 1.00, size 10 → +$1.80
assert_eq("Win PnL", compute_pnl(0.82, 1.0, 10), 1.80)
# Stop-loss: bought 0.82, sold 0.50, size 10 → -$3.20
assert_eq("Stop-loss PnL", compute_pnl(0.82, 0.5, 10), -3.20)
# Break-even
assert_eq("Break-even", compute_pnl(0.50, 0.5, 100), 0.0)
# Loss at resolution: bought 0.85, resolved 0
assert_eq("Resolution loss", compute_pnl(0.85, 0.0, 10), -8.50)


# ────────────────────────────────────────────────────────────────────────────
# 6. is_win
# ────────────────────────────────────────────────────────────────────────────
section("is_win")
assert_eq("Resolved 1.0 = win", is_win(1.0), True)
assert_eq("Resolved 0.95 = win (boundary)", is_win(0.95), True)
assert_eq("Resolved 0.94 = loss",  is_win(0.94), False)
assert_eq("Resolved 0.0  = loss",  is_win(0.0),  False)


# ────────────────────────────────────────────────────────────────────────────
# 7. pick_side
# ────────────────────────────────────────────────────────────────────────────
section("pick_side")
assert_eq("UP > DOWN",         pick_side(0.7, 0.3), "UP")
assert_eq("DOWN > UP",         pick_side(0.2, 0.8), "DOWN")
assert_eq("Tie → UP (default)", pick_side(0.5, 0.5), "UP")


# ────────────────────────────────────────────────────────────────────────────
# 8. Z-score
# ────────────────────────────────────────────────────────────────────────────
section("Z-score (TradingView Polymarket-style)")
# Reproduce screenshot: BTC=79999, strike=80285, T=1min, vol≈1.5 → Z ≈ -1.72σ
z = compute_z_score(79999, 80285, mins_left=1.0, vol=1.50)
assert_eq("Z ≈ -1.72σ (matches TradingView)", z, -1.72, tol=0.05)

# ATM → z near 0
z = compute_z_score(80000, 80000, mins_left=5.0, vol=0.5)
assert_eq("ATM Z ≈ 0", z, 0.0, tol=0.01)

# Below strike → negative
z = compute_z_score(79500, 80000, mins_left=5.0, vol=0.5)
assert_eq("BTC below strike → Z<0", z < 0, True)


# ────────────────────────────────────────────────────────────────────────────
# 9. Mathematical invariants (sanity guards)
# ────────────────────────────────────────────────────────────────────────────
section("Invariants")
# fair_prob from d2 must be in [0,1] for all reasonable inputs
for price, strike, mins in [(80000, 80000, 5), (75000, 80000, 1), (85000, 80000, 14)]:
    e = d2_edge(price, strike, 0.5, mins, asset="BTC")
    assert_eq(f"fair_prob ∈ [0,1] for ({price},{strike},{mins}m)",
              0 <= e["fair_prob"] <= 1, True)

# evaluate_filters with None inputs shouldn't crash
f = evaluate_filters(
    up=0.85, down=0.15, mins_left=4.0,
    signal=None, conviction=None, settlement_score=None, fair_prob=None,
)
assert_eq("None inputs don't crash", "all_pass" in f, True)
assert_eq("None inputs → not all_pass", f["all_pass"], False)


# ────────────────────────────────────────────────────────────────────────────
# Summary
# ────────────────────────────────────────────────────────────────────────────
print()
print("=" * 60)
total = PASS + FAIL
print(f"  {PASS}/{total} tests passed", "✅" if FAIL == 0 else f"  ❌ {FAIL} FAILED")
print("=" * 60)
sys.exit(0 if FAIL == 0 else 1)
