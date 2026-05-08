"""
Single source of truth for entry filter evaluation.
Used by bot.py (to gate orders) and dashboard.py (to display checklist).

Pure functions only — no I/O, no globals.
"""
import math
from typing import Optional

# These thresholds are imported by both bot.py and dashboard.py
ENTRY_PRICE          = 0.82
ENTRY_LAST2          = 0.85
EXIT_PRICE           = 0.50
TIME_WINDOW          = 6.0
MIN_SETTLEMENT_SCORE = 0.40
MIN_CONVICTION       = 0.55
MIN_EDGE             = 0.03
MAX_CONSECUTIVE_LOSSES = 2

# Order sizing
ORDER_SIZE = 10
ASSET_ORDER_SIZE = {"BTC": 22, "ETH": 18}


def pick_side(up: float, down: float) -> str:
    """Return 'UP' or 'DOWN' — whichever has higher price."""
    return "UP" if up >= down else "DOWN"


def evaluate_filters(
    *,
    up: float,
    down: float,
    mins_left: float,
    signal: Optional[str],
    conviction: Optional[float],
    settlement_score: Optional[float],
    fair_prob: Optional[float],
) -> dict:
    """
    Return pass/fail per filter + summary.
    Inputs are plain numbers — no objects — so this is trivially testable.
    """
    side = pick_side(up, down)
    market_price = up if side == "UP" else down

    # Edge: derive from fair_prob (which assumes YES/UP price reference)
    edge_val = None
    if fair_prob is not None:
        edge_up   = fair_prob - up
        edge_down = up - fair_prob          # = -edge_up
        edge_val  = edge_up if side == "UP" else edge_down

    ss_ok = False
    if settlement_score is not None:
        ss_ok = (settlement_score >= MIN_SETTLEMENT_SCORE) if side == "UP" \
                else (settlement_score <= -MIN_SETTLEMENT_SCORE)

    checks = [
        {"name": f"Price ≥ {ENTRY_PRICE}",
         "pass": market_price >= ENTRY_PRICE,
         "value": f"{side}={market_price:.2f}"},

        {"name": f"In window (≤ {TIME_WINDOW}m)",
         "pass": (mins_left is not None and mins_left <= TIME_WINDOW),
         "value": f"{mins_left:.1f}m left" if mins_left is not None else "n/a"},

        {"name": f"Signal matches {side}",
         "pass": (signal == side),
         "value": signal or "—"},

        {"name": f"Conviction ≥ {MIN_CONVICTION}",
         "pass": (conviction is not None and conviction >= MIN_CONVICTION),
         "value": f"{conviction:.2f}" if conviction is not None else "—"},

        {"name": f"Settlement ≥ {MIN_SETTLEMENT_SCORE} for {side}",
         "pass": ss_ok,
         "value": f"{settlement_score:+.2f}" if settlement_score is not None else "—"},

        {"name": f"Edge ≥ {MIN_EDGE}",
         "pass": (edge_val is not None and edge_val >= MIN_EDGE),
         "value": f"{edge_val:+.3f}" if edge_val is not None else "n/a"},
    ]

    return {
        "checks":       checks,
        "all_pass":     all(c["pass"] for c in checks),
        "side":         side,
        "market_price": market_price,
        "edge":         edge_val,
        "fair_prob":    fair_prob,
    }


def compute_z_score(btc_price: float, strike: float, mins_left: float, vol: float) -> Optional[float]:
    """Standardized distance: (price - strike) / (price * vol * sqrt(T))."""
    if not (btc_price and strike and mins_left and mins_left > 0 and vol and vol > 0):
        return None
    T = mins_left / 525_960.0
    sigma_T = btc_price * vol * math.sqrt(T)
    if sigma_T <= 0:
        return None
    return round((btc_price - strike) / sigma_T, 2)


def compute_pnl(entry: float, exit_price: float, size: int) -> float:
    """PnL for a binary contract position (in dollars)."""
    return round((exit_price - entry) * size, 2)


def is_win(exit_price: float, threshold: float = 0.95) -> bool:
    """Win = contract resolved near $1."""
    return exit_price >= threshold
