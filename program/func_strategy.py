"""Pure strategy math shared by live trading, exits, and offline backtests.

This module intentionally has no network or dYdX SDK dependencies so the
economic invariants can be tested in isolation.
"""


def hedge_weighted_sizes(base_price, quote_price, hedge_ratio, base_notional):
    """Return quantities that replicate ``P1 - hedge_ratio * P2``.

    One unit of leg 1 must be paired with ``hedge_ratio`` units of leg 2.
    ``base_notional`` controls the dollar size of leg 1; leg 2 notional is a
    consequence of the statistical hedge and is deliberately not forced to be
    equal-dollar.
    """
    base_price = float(base_price)
    quote_price = float(quote_price)
    hedge_ratio = float(hedge_ratio)
    base_notional = float(base_notional)
    if base_price <= 0 or quote_price <= 0:
        raise ValueError("prices must be positive")
    if hedge_ratio <= 0:
        raise ValueError("hedge_ratio must be positive")
    if base_notional <= 0:
        raise ValueError("base_notional must be positive")
    base_qty = base_notional / base_price
    quote_qty = hedge_ratio * base_qty
    return base_qty, quote_qty


def hedge_notionals(base_qty, quote_qty, base_price, quote_price):
    """Return absolute entry notionals for both legs."""
    return (
        abs(float(base_qty) * float(base_price)),
        abs(float(quote_qty) * float(quote_price)),
    )


def spread_convergence_progress(entry_spread, target_spread, current_spread):
    """Signed fraction of the entry-to-target move already completed.

    0.0 is the entry spread, 1.0 is the frozen target, and values above 1.0
    mean the spread crossed beyond that target. A negative value means the
    spread moved away from the target.
    """
    entry_spread = float(entry_spread)
    target_spread = float(target_spread)
    current_spread = float(current_spread)
    distance = target_spread - entry_spread
    if abs(distance) < 1e-12:
        return 0.0
    return (current_spread - entry_spread) / distance


def estimate_round_trip_cost(total_entry_notional, cost_rate):
    """Estimate open+close cost for both legs from a one-way rate."""
    return 2.0 * abs(float(total_entry_notional)) * float(cost_rate)


def fee_with_fallback(fee, notional, fee_rate):
    """Return ``(fee_value, estimated)`` for one leg.

    Market IOC legs should not silently contribute a zero fee merely because
    the indexer omitted it. Each missing leg is estimated independently.
    """
    fee = float(fee or 0.0)
    notional = abs(float(notional or 0.0))
    if fee > 0.0:
        return fee, False
    if notional > 0.0:
        return notional * float(fee_rate), True
    return 0.0, False


def conservative_close_price(close_side, best_bid, best_ask, oracle_price, reserve_bps=0.0):
    """Estimate a close at the adverse top of book plus a movement reserve."""
    side = str(close_side).upper()
    oracle = float(oracle_price or 0.0)
    bid = float(best_bid or 0.0)
    ask = float(best_ask or 0.0)
    reserve = max(0.0, float(reserve_bps)) / 10_000.0
    if side == "SELL":
        touch = bid or oracle
        return touch * (1.0 - reserve), "book_bid" if bid else "oracle"
    touch = ask or oracle
    return touch * (1.0 + reserve), "book_ask" if ask else "oracle"
