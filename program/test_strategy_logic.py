#!/usr/bin/env python3
"""Offline tests for economic invariants; no SDK/network dependencies."""

from func_strategy import (
    conservative_close_price,
    hedge_weighted_sizes,
    hedge_notionals,
    spread_convergence_progress,
    estimate_round_trip_cost,
    fee_with_fallback,
)


def close(a, b, tol=1e-9):
    return abs(a - b) <= tol


def test_hedge_ratio_is_replicated():
    q1, q2 = hedge_weighted_sizes(45.0, 0.84, 51.5, 30.0)
    assert close(q2 / q1, 51.5)


def test_notionals_are_consequence_of_hedge():
    q1, q2 = hedge_weighted_sizes(10.0, 2.0, 4.0, 30.0)
    n1, n2 = hedge_notionals(q1, q2, 10.0, 2.0)
    assert close(n1, 30.0)
    assert close(n2, 24.0)


def test_frozen_convergence_progress():
    assert close(spread_convergence_progress(3.0, 1.0, 3.0), 0.0)
    assert close(spread_convergence_progress(3.0, 1.0, 2.0), 0.5)
    assert close(spread_convergence_progress(3.0, 1.0, 1.0), 1.0)
    assert close(spread_convergence_progress(3.0, 1.0, 0.0), 1.5)


def test_round_trip_cost_uses_actual_total_notional():
    assert close(estimate_round_trip_cost(54.0, 0.0005), 0.054)


def test_missing_fee_is_estimated_per_leg():
    f1, e1 = fee_with_fallback(0.015, 30.0, 0.0005)
    f2, e2 = fee_with_fallback(0.0, 30.0, 0.0005)
    assert close(f1 + f2, 0.030)
    assert e1 is False and e2 is True


def test_conservative_close_price_uses_adverse_touch():
    sell_px, sell_src = conservative_close_price("SELL", 99.8, 100.2, 100.0, 20)
    buy_px, buy_src = conservative_close_price("BUY", 99.8, 100.2, 100.0, 20)
    assert close(sell_px, 99.8 * 0.998)
    assert close(buy_px, 100.2 * 1.002)
    assert (sell_src, buy_src) == ("book_bid", "book_ask")


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for test in tests:
        test()
        print(f"PASS {test.__name__}")
    print(f"{len(tests)} strategy tests passed")
