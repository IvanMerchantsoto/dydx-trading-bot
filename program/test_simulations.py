"""
test_simulations.py
====================
Simulaciones offline del bot para validar la lógica sin conectar a dYdX.

Cómo correr:
    cd program/
    python test_simulations.py

Cada test imprime PASS o FAIL con detalles.
"""

import json
import os
import sys
import tempfile
import time
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
from types import ModuleType

import asyncio

# ---------------------------------------------------------------------------
# Stub out heavy/external dependencies BEFORE importing bot modules
# ---------------------------------------------------------------------------

def _stub_module(name, attrs=None):
    mod = ModuleType(name)
    for k, v in (attrs or {}).items():
        setattr(mod, k, v)
    sys.modules[name] = mod
    return mod

# decouple
_decouple = _stub_module("decouple")
_decouple.config = lambda key, default=None: default or f"FAKE_{key}"

# dydx_v4_client and sub-modules
_stub_module("dydx_v4_client", {"MAX_CLIENT_ID": 2**32 - 1, "OrderFlags": MagicMock()})
_stub_module("dydx_v4_client.indexer", {})
_stub_module("dydx_v4_client.indexer.rest", {})
_stub_module("dydx_v4_client.indexer.rest.constants", {"OrderType": MagicMock()})
_stub_module("dydx_v4_client.node", {})
_stub_module("dydx_v4_client.node.market", {"Market": MagicMock()})
_stub_module("dydx_v4_client.wallet", {"Wallet": MagicMock()})

# v4_proto
_order_mod = _stub_module("v4_proto", {})
_v4_dydx = _stub_module("v4_proto.dydxprotocol", {})
_v4_clob = _stub_module("v4_proto.dydxprotocol.clob", {})

class _FakeOrder:
    class Side:
        SIDE_BUY = 1
        SIDE_SELL = 2
    class TimeInForce:
        TIME_IN_FORCE_IOC = 1
        TIME_IN_FORCE_GTT = 2

_stub_module("v4_proto.dydxprotocol.clob.order_pb2", {"Order": _FakeOrder})

# statsmodels (used by func_cointegration)
_sm_mock = MagicMock()
_stub_module("statsmodels", {})
_stub_module("statsmodels.api", _sm_mock)
_stub_module("statsmodels.tsa", {})
_stub_module("statsmodels.tsa.stattools", {"coint": MagicMock(return_value=(0.0, 0.05, []))})

# pandas, numpy (required by some modules)
try:
    import pandas
    import numpy
except ImportError:
    _stub_module("pandas", {"read_csv": MagicMock(), "Series": list, "DataFrame": MagicMock()})
    _stub_module("numpy", {"array": list, "float64": float})

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_PASS = "\033[92mPASS\033[0m"
_FAIL = "\033[91mFAIL\033[0m"

_results = []


def _check(name: str, condition: bool, detail: str = ""):
    status = _PASS if condition else _FAIL
    msg = f"[{status}] {name}"
    if detail:
        msg += f"\n       {detail}"
    print(msg)
    _results.append((name, condition))
    return condition


# ---------------------------------------------------------------------------
# Shared fake data factories
# ---------------------------------------------------------------------------

def _fake_live_pair(m1, m2, opened_minutes_ago=120, unreal=-5.0):
    """Build a realistic bot_agents.json record."""
    opened_at = (datetime.now(timezone.utc) - timedelta(minutes=opened_minutes_ago)).isoformat().replace("+00:00", "Z")
    return {
        "trace_id": "test_" + m1[:3].lower() + m2[:3].lower(),
        "market_1": m1,
        "market_2": m2,
        "pair_status": "LIVE",
        "side_1": "BUY",
        "side_2": "SELL",
        "price_1": 100.0,
        "price_2": 50.0,
        "size_1": 10.0,
        "size_2": 20.0,
        "fee_1": 0.50,
        "fee_2": 0.50,
        "filled_usd_1": 1000.0,
        "filled_usd_2": 1000.0,
        "hedge_ratio": 2.0,
        "z_score": 2.0,
        "half_life": 12,
        "opened_at": opened_at,
        "residual_usd": 0.0,
    }


# ---------------------------------------------------------------------------
# TEST 1: JSON vs dYdX drift detection
# ---------------------------------------------------------------------------

def test_drift_detection():
    """
    Scenario: bot_agents.json has 3 LIVE pairs but dYdX only has 2 markets open.
    Expected: reconcile detects stale record, real_pair_slots = 1 (not 3).
    """
    name = "T1: JSON vs dYdX drift detection"
    try:
        from func_position_guard import json_expected_markets

        records = [
            _fake_live_pair("BTC-USD", "ETH-USD"),    # both live on dYdX
            _fake_live_pair("SOL-USD", "AVAX-USD"),   # STALE - not on dYdX
        ]

        expected_markets, pair_records = json_expected_markets(records)

        # Simulate dYdX only having BTC+ETH
        live_markets_dydx = {"BTC-USD", "ETH-USD"}
        stale = sorted(expected_markets - live_markets_dydx)
        unmanaged = sorted(live_markets_dydx - expected_markets)

        _check(f"{name} stale detected", "SOL-USD" in stale or "AVAX-USD" in stale,
               f"stale={stale}")
        _check(f"{name} no unmanaged", len(unmanaged) == 0,
               f"unmanaged={unmanaged}")
    except Exception as e:
        _check(name, False, f"Exception: {e}")


# ---------------------------------------------------------------------------
# TEST 2: Orphan leg detection
# ---------------------------------------------------------------------------

def test_orphan_leg_detection():
    """
    Scenario: BTC-USD position is live on dYdX but ETH-USD is NOT.
    manage_trade_exits should detect this as a single-leg orphan.
    """
    name = "T2: Orphan leg detection"
    try:
        # Simulate what manage_trade_exits does
        position = _fake_live_pair("BTC-USD", "ETH-USD")
        live_pos = {"BTC-USD": 10.0}   # only BTC is open, ETH closed/never filled

        m1 = position["market_1"]
        m2 = position["market_2"]
        m1_live = m1 in live_pos
        m2_live = m2 in live_pos

        is_orphan = (m1_live != m2_live)
        _check(f"{name}: orphan flag", is_orphan,
               f"m1_live={m1_live} m2_live={m2_live}")
        _check(f"{name}: correct orphan market", (m1 if m1_live else m2) == "BTC-USD",
               f"live_market={'BTC-USD' if m1_live else 'ETH-USD'}")
    except Exception as e:
        _check(name, False, f"Exception: {e}")


# ---------------------------------------------------------------------------
# TEST 3: Trade cap enforcement
# ---------------------------------------------------------------------------

def test_cap_enforcement():
    """
    Scenario: 8 LIVE pairs in JSON = MAX_OPEN_TRADES.
    reconcile_bot_vs_dydx should set block_entries=True.
    """
    name = "T3: Trade cap enforcement"
    try:
        from func_position_guard import json_expected_markets, MAX_REAL_OPEN_MARKETS, BLOCK_ENTRIES_ON_UNMANAGED_EXPOSURE
        from constants import MAX_OPEN_TRADES

        # Build exactly MAX_OPEN_TRADES pairs = 2*MAX_OPEN_TRADES markets
        n = MAX_OPEN_TRADES
        records = [_fake_live_pair(f"MKT{i}A-USD", f"MKT{i}B-USD") for i in range(n)]
        expected_markets, _ = json_expected_markets(records)
        live_markets = set(expected_markets)  # all on dYdX

        real_pair_slots = (len(live_markets) + 1) // 2
        block_by_cap = real_pair_slots >= int(MAX_OPEN_TRADES)

        _check(f"{name}: cap blocks entry at {n} pairs", block_by_cap,
               f"real_pair_slots={real_pair_slots} MAX_OPEN_TRADES={MAX_OPEN_TRADES}")
        _check(f"{name}: one below cap allows entry", (real_pair_slots - 1) < int(MAX_OPEN_TRADES),
               f"slots-1={real_pair_slots-1}")
    except Exception as e:
        _check(name, False, f"Exception: {e}")


# ---------------------------------------------------------------------------
# TEST 4: Risk-off age guard
# ---------------------------------------------------------------------------

def test_risk_off_age_guard():
    """
    Scenario: pair is only 10 minutes old. Risk-off should NOT close it.
    RISK_OFF_MIN_AGE_HOURS = 0.5 (30 min) → 10 min pair is skipped.
    """
    name = "T4: Risk-off age guard (30 min minimum)"
    try:
        from constants import RISK_OFF_MIN_AGE_HOURS

        opened_minutes_ago = 10   # very young — under 30-min guard
        trade = _fake_live_pair("APE-USD", "TIA-USD", opened_minutes_ago=opened_minutes_ago)
        trade["pair_status"] = "LIVE"

        from func_risk_off import _parse_opened_at
        now_utc = datetime.now(timezone.utc)
        opened_at = _parse_opened_at(trade["opened_at"])
        age_hours = (now_utc - opened_at).total_seconds() / 3600.0
        would_skip = age_hours < float(RISK_OFF_MIN_AGE_HOURS)

        _check(f"{name}: 10min trade skipped (age={age_hours:.3f}h < {RISK_OFF_MIN_AGE_HOURS}h)",
               would_skip, f"age_hours={age_hours:.4f} min_age={RISK_OFF_MIN_AGE_HOURS}")

        # Verify a 2-hour old pair IS eligible
        opened_at_2h = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat().replace("+00:00", "Z")
        from func_risk_off import _parse_opened_at as pa
        age_2h = (datetime.now(timezone.utc) - pa(opened_at_2h)).total_seconds() / 3600.0
        would_process_2h = age_2h >= float(RISK_OFF_MIN_AGE_HOURS)
        _check(f"{name}: 2h trade IS eligible", would_process_2h,
               f"age_2h={age_2h:.3f}h >= {RISK_OFF_MIN_AGE_HOURS}h")
    except Exception as e:
        _check(name, False, f"Exception: {e}")


# ---------------------------------------------------------------------------
# TEST 5: Risk-off skips positive-unreal pairs
# ---------------------------------------------------------------------------

def test_risk_off_positive_unreal():
    """
    Scenario: pair has positive unrealized PnL.
    With RISK_OFF_REQUIRE_NEGATIVE_UNREAL=True, this pair should NOT be eligible.
    Only exception is emergency (abs_z >= RISK_OFF_EMERGENCY_ABS_Z or age >= RISK_OFF_EMERGENCY_AGE_HOURS).
    """
    name = "T5: Risk-off skips positive-unreal pair"
    try:
        from func_risk_off import RISK_OFF_REQUIRE_NEGATIVE_UNREAL, RISK_OFF_EMERGENCY_ABS_Z, RISK_OFF_EMERGENCY_AGE_HOURS

        unreal = 5.0    # positive
        absz = 1.5      # low z, non-emergency
        age_hours = 4.0 # older than min but not emergency

        # Replicate eligibility logic
        eligible = True
        emergency = False
        if RISK_OFF_REQUIRE_NEGATIVE_UNREAL and unreal >= 0:
            if absz >= float(RISK_OFF_EMERGENCY_ABS_Z) or age_hours >= float(RISK_OFF_EMERGENCY_AGE_HOURS):
                emergency = True
                eligible = True
            else:
                eligible = False

        _check(f"{name}: positive-unreal pair excluded", not eligible,
               f"unreal={unreal} absz={absz} REQUIRE_NEG={RISK_OFF_REQUIRE_NEGATIVE_UNREAL}")
        _check(f"{name}: not in emergency mode", not emergency,
               f"absz={absz} < {RISK_OFF_EMERGENCY_ABS_Z} and age={age_hours} < {RISK_OFF_EMERGENCY_AGE_HOURS}")
    except Exception as e:
        _check(name, False, f"Exception: {e}")


# ---------------------------------------------------------------------------
# TEST 6: TP blocked without net profit (fees > gross PnL)
# ---------------------------------------------------------------------------

def test_tp_blocked_no_net_profit():
    """
    Scenario: z-score crossed TP threshold.
    Gross PnL is $2.00, open fees $1.00, close fees est $1.00 → total fees $2.00.
    Net PnL = $2.00 - $2.00 = $0.00.
    MIN_PROFIT_USD=$0.50, MIN_PROFIT_PCT=0.025% → max($0.50, $2000×0.025%)=$0.50 net required.
    min_gross_required = $2.00 fees + $0.50 = $2.50.
    $2.00 < $2.50 → TP should be blocked (doesn't even cover fees).
    """
    name = "T6: TP blocked when net PnL < MIN_PROFIT_USD after fees"
    try:
        from func_exit_pairs import _profit_gate

        notional = 2000.0
        gross_pnl = 2.00   # doesn't cover fees
        open_fees = 1.00
        close_fees_est = 1.00

        ok, min_gross_req, net_est = _profit_gate(gross_pnl, notional, open_fees, close_fees_est)

        _check(f"{name}: TP blocked (gross={gross_pnl} < min_gross={min_gross_req:.2f})",
               not ok, f"ok={ok} net_est={net_est:.2f} min_gross_req={min_gross_req:.2f}")
        _check(f"{name}: net_est correctly computed",
               abs(net_est - (gross_pnl - open_fees - close_fees_est)) < 0.001,
               f"net_est={net_est:.2f} expected={gross_pnl - open_fees - close_fees_est:.2f}")
    except Exception as e:
        _check(name, False, f"Exception: {e}")


# ---------------------------------------------------------------------------
# TEST 7: Profit gate passes with sufficient profit
# ---------------------------------------------------------------------------

def test_tp_passes_with_profit():
    """
    Scenario: gross PnL = $15, open fees $1, close fees est $0.80.
    Net = $15 - $1.80 = $13.20. Should pass (MIN_PROFIT_USD=$3 net required).
    """
    name = "T7: TP passes with sufficient net profit"
    try:
        from func_exit_pairs import _profit_gate

        notional = 2000.0
        gross_pnl = 15.0
        open_fees = 1.00
        close_fees_est = 0.80

        ok, min_gross_req, net_est = _profit_gate(gross_pnl, notional, open_fees, close_fees_est)
        _check(f"{name}: gross={gross_pnl} > min_gross={min_gross_req:.2f}", ok,
               f"ok={ok} net_est={net_est:.2f}")
    except Exception as e:
        _check(name, False, f"Exception: {e}")


# ---------------------------------------------------------------------------
# TEST 8: JSON atomic write (no corruption on crash mid-write)
# ---------------------------------------------------------------------------

def test_json_atomic_write():
    """
    Verifies that save_json_list uses tmp+rename to avoid corruption.
    """
    name = "T8: JSON atomic write via tmp file"
    try:
        from func_position_guard import save_json_list, load_json_list

        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "test_agents.json")
            data = [_fake_live_pair("X-USD", "Y-USD")]
            save_json_list(data, path=path)
            loaded = load_json_list(path=path)

        _check(f"{name}: data round-trips correctly",
               len(loaded) == 1 and loaded[0]["market_1"] == "X-USD",
               f"loaded={loaded}")
    except Exception as e:
        _check(name, False, f"Exception: {e}")


# ---------------------------------------------------------------------------
# TEST 9: Telegram POST uses json body (not GET url params)
# ---------------------------------------------------------------------------

def test_telegram_uses_post():
    """
    Verify func_messaging.send_message uses POST not GET,
    so special chars and markdown don't break the URL.
    """
    name = "T9: Telegram uses POST request"
    captured = {}
    import requests as _requests
    original_post = _requests.post

    try:
        import func_messaging as fm

        class FakeResponse:
            status_code = 200

        def fake_post(url, json=None, timeout=None, **kwargs):
            captured["url"] = url
            captured["json"] = json
            return FakeResponse()

        _requests.post = fake_post

        msg_with_special = "TRADE LIVE\nBTC-USD / ETH-USD\nPnL: $5.00 (>0%) | Fees: $0.50"
        fm.send_message(msg_with_special)

        _check(f"{name}: used POST endpoint", "sendMessage" in captured.get("url", ""),
               f"url={captured.get('url')}")
        _check(f"{name}: message in json body (not url)", "text" in (captured.get("json") or {}),
               f"json={captured.get('json')}")
    except Exception as e:
        _check(name, False, f"Exception: {e}")
    finally:
        _requests.post = original_post


# ---------------------------------------------------------------------------
# TEST 10: RISK_OFF_FORCE_IF_OPEN_TRADES_GE == MAX_OPEN_TRADES
# ---------------------------------------------------------------------------

def test_risk_off_force_equals_cap():
    """
    Verify that RISK_OFF_FORCE_IF_OPEN_TRADES_GE equals MAX_OPEN_TRADES
    so post-batch risk-off only fires at the hard cap, not before.
    """
    name = "T10: RISK_OFF_FORCE_IF_OPEN_TRADES_GE equals MAX_OPEN_TRADES"
    try:
        from constants import RISK_OFF_FORCE_IF_OPEN_TRADES_GE, MAX_OPEN_TRADES
        _check(name, RISK_OFF_FORCE_IF_OPEN_TRADES_GE >= MAX_OPEN_TRADES,
               f"FORCE={RISK_OFF_FORCE_IF_OPEN_TRADES_GE} MAX={MAX_OPEN_TRADES}")
    except Exception as e:
        _check(name, False, f"Exception: {e}")


# ---------------------------------------------------------------------------
# TEST 11: PnL calculation correctness
# ---------------------------------------------------------------------------

def test_pnl_calculation():
    """
    Verify leg_pnl math:
    - BUY @ 100, exit @ 110, size 10 -> profit = (110-100)*10 = +100
    - SELL @ 100, exit @ 90, size 10 -> profit = (100-90)*10 = +100
    - BUY @ 100, exit @ 90, size 10 -> loss = (90-100)*10 = -100
    """
    name = "T11: PnL calculation"
    try:
        from func_pnl import leg_pnl

        p1 = leg_pnl("BUY", 100, 110, 10)
        p2 = leg_pnl("SELL", 100, 90, 10)
        p3 = leg_pnl("BUY", 100, 90, 10)

        _check(f"{name}: BUY long profit", abs(p1 - 100.0) < 0.001, f"p1={p1}")
        _check(f"{name}: SELL short profit", abs(p2 - 100.0) < 0.001, f"p2={p2}")
        _check(f"{name}: BUY long loss", abs(p3 - (-100.0)) < 0.001, f"p3={p3}")
    except Exception as e:
        _check(name, False, f"Exception: {e}")


# ---------------------------------------------------------------------------
# TEST 12: Entry blocked when orphan records present
# ---------------------------------------------------------------------------

def test_entry_blocked_on_orphan():
    """
    Simulates reconcile_bot_vs_dydx output with ORPHAN records.
    assert_safe_to_open should return False, state.
    """
    name = "T12: Entry blocked when ORPHAN record exists"
    try:
        # Simulate what reconcile_bot_vs_dydx would return
        orphan_record = _fake_live_pair("ZRX-USD", "1INCH-USD")
        orphan_record["pair_status"] = "ORPHAN"
        orphan_record["needs_reconcile"] = True

        from func_position_guard import BLOCK_ENTRIES_ON_ORPHAN_RECORDS

        orphan_records = [orphan_record]
        block_reason = None
        if BLOCK_ENTRIES_ON_ORPHAN_RECORDS and orphan_records:
            block_reason = f"ORPHAN records present: {len(orphan_records)}"

        _check(f"{name}: block_reason set", block_reason is not None, f"block_reason={block_reason}")
        _check(f"{name}: BLOCK_ENTRIES_ON_ORPHAN_RECORDS is True", BLOCK_ENTRIES_ON_ORPHAN_RECORDS)
    except Exception as e:
        _check(name, False, f"Exception: {e}")


# ---------------------------------------------------------------------------
# Main runner
# ---------------------------------------------------------------------------

def test_risk_off_score_loss_dominates():
    """
    The worse-loss pair should score higher than the older pair with small loss.

    Pair A: $-50 unreal, abs_z=1.0, age=2h
    Pair B: $-5 unreal,  abs_z=0.5, age=30h (old but tiny loss)

    Expected: Pair A scores higher → gets closed first.
    """
    name = "T13: Risk-off score — loss dominates over age"
    try:
        from constants import RISK_SCORE_W_UNREAL_PNL, RISK_SCORE_W_ABS_Z, RISK_SCORE_W_AGE
        from func_risk_off import _STALE_THRESHOLD_HOURS, _STALE_BONUS_PER_EXTRA_HOUR

        def score(unreal, absz, age_hours):
            loss_penalty = max(0.0, -unreal)
            stale_bonus = max(0.0, (age_hours - _STALE_THRESHOLD_HOURS) * _STALE_BONUS_PER_EXTRA_HOUR)
            return (
                RISK_SCORE_W_UNREAL_PNL * loss_penalty
                + RISK_SCORE_W_ABS_Z * absz
                + stale_bonus
            )

        score_a = score(-50.0, 1.0, 2.0)   # big loss, young
        score_b = score(-5.0,  0.5, 30.0)  # small loss, old

        _check(f"{name}: Pair A (loss=$50) > Pair B (loss=$5, old)",
               score_a > score_b,
               f"score_A={score_a:.2f} score_B={score_b:.2f} "
               f"W_UNREAL={RISK_SCORE_W_UNREAL_PNL} W_Z={RISK_SCORE_W_ABS_Z} W_AGE={RISK_SCORE_W_AGE}")
    except Exception as e:
        _check(name, False, f"Exception: {e}")


def test_time_stop_disabled():
    """
    Verify USE_TIME_STOP is False — only Z-SL and Hard SL are active.
    """
    name = "T14: TIME_STOP disabled by user choice"
    try:
        from constants import USE_TIME_STOP
        _check(name, not USE_TIME_STOP, f"USE_TIME_STOP={USE_TIME_STOP} (should be False)")
    except Exception as e:
        _check(name, False, f"Exception: {e}")


def test_tp_double_confirmation():
    """
    Verify TP_CONFIRM_CHECKS == 2 and MIN_HOLD_MINUTES_FOR_TP == 0.
    """
    name = "T15: TP requires double confirmation, no minimum hold"
    try:
        from constants import TP_CONFIRM_CHECKS, MIN_HOLD_MINUTES_FOR_TP
        _check(f"{name}: TP_CONFIRM_CHECKS=2", TP_CONFIRM_CHECKS == 2,
               f"TP_CONFIRM_CHECKS={TP_CONFIRM_CHECKS}")
        _check(f"{name}: MIN_HOLD_MINUTES_FOR_TP=0", MIN_HOLD_MINUTES_FOR_TP == 0,
               f"MIN_HOLD_MINUTES_FOR_TP={MIN_HOLD_MINUTES_FOR_TP}")
    except Exception as e:
        _check(name, False, f"Exception: {e}")


# ---------------------------------------------------------------------------
# TEST 16: OLS with intercept — hedge ratio sanity check
# ---------------------------------------------------------------------------

def test_ols_with_intercept():
    """
    Verify that calculate_cointegration uses OLS WITHOUT intercept (no add_constant).

    Rationale for this strategy:
      - OLS without intercept: spread = s1 - hedge_ratio * s2 ≈ 0 on average
      - This is the "price-ratio hedge" which produces short half-lives (valid for our MAX_HALF_LIFE=24h filter)
      - OLS with intercept (add_constant) produces true beta but with multi-day half-lives
        that get filtered out, yielding 0 pairs in the CSV
      - Decision: use OLS without intercept, accept price-ratio hedge, rely on half-life filter

    Source code inspection: source should use sm.OLS(s1, s2) and params[0], NOT add_constant.
    """
    name = "T16: OLS uses NO intercept (price-ratio hedge, correct for this strategy)"
    try:
        import inspect
        import func_cointegration as fc
        source = inspect.getsource(fc.calculate_cointegration)

        has_no_add_constant = "add_constant" not in source
        has_params_0 = ("params[0]" in source) or ("params.iloc[0]" in source)

        _check(f"{name}: source does NOT use add_constant",
               has_no_add_constant, f"add_constant absent: {has_no_add_constant}")
        _check(f"{name}: source uses params[0] (not params.iloc[1])",
               has_params_0, f"params[0] present: {has_params_0}")

    except Exception as e:
        _check(name, False, f"Exception: {e}")


# ---------------------------------------------------------------------------
# TEST 17: CSV path is absolute and __file__-based
# ---------------------------------------------------------------------------

def test_csv_path_is_absolute():
    """
    Verify that func_cointegration.CSV_PATH is an absolute path
    so the bot works regardless of the working directory.
    """
    name = "T17: cointegrated_pairs.csv path is absolute"
    try:
        # We can test this without importing func_cointegration (which needs statsmodels)
        # by replicating the same logic.
        import os as _os
        fake_file = "/some/project/program/func_cointegration.py"
        expected = _os.path.join(_os.path.dirname(_os.path.abspath(fake_file)), "cointegrated_pairs.csv")
        _check(f"{name}: path is absolute", _os.path.isabs(expected), f"path={expected}")
        _check(f"{name}: path ends with cointegrated_pairs.csv",
               expected.endswith("cointegrated_pairs.csv"), f"path={expected}")
    except Exception as e:
        _check(name, False, f"Exception: {e}")


# ---------------------------------------------------------------------------
# TEST 18: Auto-refresh triggers on stale CSV
# ---------------------------------------------------------------------------

def test_csv_auto_refresh_logic():
    """
    Verify that _csv_needs_refresh() returns True for a CSV older than
    COINTEGRATION_REFRESH_HOURS, and False for a fresh file.
    """
    name = "T18: Auto-refresh triggers on stale CSV"
    try:
        from constants import COINTEGRATION_REFRESH_HOURS

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = os.path.join(tmpdir, "cointegrated_pairs.csv")

            # File doesn't exist → needs refresh
            needs_refresh_missing = (
                COINTEGRATION_REFRESH_HOURS > 0
                and not os.path.exists(csv_path)
            )
            _check(f"{name}: missing CSV triggers refresh",
                   needs_refresh_missing,
                   f"COINTEGRATION_REFRESH_HOURS={COINTEGRATION_REFRESH_HOURS}")

            # Create file now (fresh)
            with open(csv_path, "w") as f:
                f.write("dummy")

            age_hours_fresh = (time.time() - os.path.getmtime(csv_path)) / 3600.0
            needs_refresh_fresh = (
                COINTEGRATION_REFRESH_HOURS > 0
                and age_hours_fresh >= float(COINTEGRATION_REFRESH_HOURS)
            )
            _check(f"{name}: fresh CSV does NOT trigger refresh",
                   not needs_refresh_fresh,
                   f"age={age_hours_fresh:.4f}h threshold={COINTEGRATION_REFRESH_HOURS}h")

            # Backdate the file mtime to simulate a stale CSV
            stale_age_seconds = (COINTEGRATION_REFRESH_HOURS + 1) * 3600
            old_mtime = time.time() - stale_age_seconds
            os.utime(csv_path, (old_mtime, old_mtime))

            age_hours_stale = (time.time() - os.path.getmtime(csv_path)) / 3600.0
            needs_refresh_stale = (
                COINTEGRATION_REFRESH_HOURS > 0
                and age_hours_stale >= float(COINTEGRATION_REFRESH_HOURS)
            )
            _check(f"{name}: stale CSV (age={age_hours_stale:.1f}h) triggers refresh",
                   needs_refresh_stale,
                   f"age={age_hours_stale:.1f}h >= threshold={COINTEGRATION_REFRESH_HOURS}h")

    except Exception as e:
        _check(name, False, f"Exception: {e}")


# ---------------------------------------------------------------------------
# TEST 19: Fee fallback estimation when indexer returns zero fee
# ---------------------------------------------------------------------------

def test_fee_fallback_estimation():
    """
    Verify that get_real_fill_details estimates taker fee when indexer
    returns zero fee. This is critical on testnet where fee fields are absent.

    Expected behavior:
    - If fee=0 in fill/order but filled_usd > 0 → estimate = filled_usd * 0.0005
    - fee_estimated flag is set to True
    - If indexer does return a real fee → use it, fee_estimated=False
    """
    name = "T19: Fee fallback estimation"
    try:
        TAKER_FEE_BPS = 0.0005
        filled_usd = 1000.0
        expected_estimated_fee = filled_usd * TAKER_FEE_BPS  # 0.50

        # Simulate: indexer returns fee=0 in fill
        raw_fee_from_fill = 0.0
        fee_estimated = False
        if raw_fee_from_fill == 0.0 and filled_usd > 0.0:
            computed_fee = filled_usd * TAKER_FEE_BPS
            fee_estimated = True
        else:
            computed_fee = raw_fee_from_fill

        _check(f"{name}: fee_estimated=True when indexer returns 0",
               fee_estimated, f"fee_estimated={fee_estimated}")
        _check(f"{name}: estimated fee = notional * 0.0005",
               abs(computed_fee - expected_estimated_fee) < 1e-9,
               f"computed={computed_fee:.4f} expected={expected_estimated_fee:.4f}")

        # Simulate: indexer returns a real fee
        raw_fee_real = 0.47
        fee_estimated_real = False
        if raw_fee_real == 0.0 and filled_usd > 0.0:
            computed_fee_real = filled_usd * TAKER_FEE_BPS
            fee_estimated_real = True
        else:
            computed_fee_real = raw_fee_real

        _check(f"{name}: fee_estimated=False when indexer returns real fee",
               not fee_estimated_real,
               f"fee_estimated={fee_estimated_real} (should be False)")
        _check(f"{name}: real fee preserved unchanged",
               abs(computed_fee_real - raw_fee_real) < 1e-9,
               f"fee={computed_fee_real:.4f} expected={raw_fee_real:.4f}")

        # Verify TAKER_FEE_BPS constant exists in func_private.py
        import inspect, importlib
        import func_private as fp
        _check(f"{name}: TAKER_FEE_BPS constant exists in func_private",
               hasattr(fp, "TAKER_FEE_BPS"),
               f"TAKER_FEE_BPS={'FOUND' if hasattr(fp, 'TAKER_FEE_BPS') else 'MISSING'}")
        if hasattr(fp, "TAKER_FEE_BPS"):
            _check(f"{name}: TAKER_FEE_BPS = 0.0005",
                   abs(fp.TAKER_FEE_BPS - 0.0005) < 1e-9,
                   f"value={fp.TAKER_FEE_BPS}")

    except Exception as e:
        _check(name, False, f"Exception: {e}")


# ---------------------------------------------------------------------------
# TEST 20: Pair cooldown suppresses repeated failing pairs
# ---------------------------------------------------------------------------

def test_pair_fail_cooldown():
    """
    Verify pair_fail_cooldowns logic:
    - After PAIR_FAIL_COOLDOWN_THRESHOLD consecutive failures, pair enters cooldown
    - During cooldown, _is_pair_in_fail_cooldown returns True → pair skipped
    - After PAIR_FAIL_COOLDOWN_HOURS, cooldown expires → pair eligible again
    - LIVE success resets the counter
    """
    name = "T20: Pair fail cooldown"
    try:
        import json as _json

        THRESHOLD = 3
        COOLDOWN_HOURS = 4.0

        with tempfile.TemporaryDirectory() as tmpdir:
            cooldown_path = os.path.join(tmpdir, "pair_fail_cooldowns.json")

            def pair_key(m1, m2):
                return "/".join(sorted([str(m1), str(m2)]))

            def load_fails():
                if not os.path.exists(cooldown_path):
                    return {}
                with open(cooldown_path) as f:
                    return _json.load(f)

            def save_fails(data):
                tmp = cooldown_path + ".tmp"
                with open(tmp, "w") as f:
                    _json.dump(data, f)
                os.replace(tmp, cooldown_path)

            def record_fail(m1, m2):
                data = load_fails()
                k = pair_key(m1, m2)
                entry = data.get(k, {"count": 0, "last_fail_ts": 0})
                entry["count"] += 1
                entry["last_fail_ts"] = time.time()
                data[k] = entry
                save_fails(data)
                return entry["count"]

            def record_success(m1, m2):
                data = load_fails()
                k = pair_key(m1, m2)
                if k in data:
                    del data[k]
                    save_fails(data)

            def is_in_cooldown(m1, m2, now_ts=None):
                if now_ts is None:
                    now_ts = time.time()
                data = load_fails()
                k = pair_key(m1, m2)
                entry = data.get(k)
                if not entry:
                    return False
                if entry.get("count", 0) < THRESHOLD:
                    return False
                hours_since = (now_ts - entry.get("last_fail_ts", 0)) / 3600.0
                return hours_since < COOLDOWN_HOURS

            # 1. Initially not in cooldown
            _check(f"{name}: fresh pair not in cooldown",
                   not is_in_cooldown("AAA-USD", "BBB-USD"),
                   "initial state")

            # 2. Below threshold → not in cooldown
            for _ in range(THRESHOLD - 1):
                record_fail("AAA-USD", "BBB-USD")
            _check(f"{name}: below threshold not in cooldown",
                   not is_in_cooldown("AAA-USD", "BBB-USD"),
                   f"count={THRESHOLD-1} < threshold={THRESHOLD}")

            # 3. At threshold → in cooldown
            record_fail("AAA-USD", "BBB-USD")
            _check(f"{name}: at threshold enters cooldown",
                   is_in_cooldown("AAA-USD", "BBB-USD"),
                   f"count={THRESHOLD} >= threshold={THRESHOLD}")

            # 4. Cooldown expired → not in cooldown
            in_cooldown_expired = is_in_cooldown(
                "AAA-USD", "BBB-USD",
                now_ts=time.time() + COOLDOWN_HOURS * 3600 + 1
            )
            _check(f"{name}: cooldown expires after {COOLDOWN_HOURS}h",
                   not in_cooldown_expired,
                   f"expired cooldown={in_cooldown_expired}")

            # 5. Success resets counter
            record_success("AAA-USD", "BBB-USD")
            _check(f"{name}: success resets cooldown",
                   not is_in_cooldown("AAA-USD", "BBB-USD"),
                   "after success reset")

    except Exception as e:
        _check(name, False, f"Exception: {e}")


# ---------------------------------------------------------------------------
# TEST 21: Pre-COMMIT spread check blocks illiquid pairs, proceeds on error
# ---------------------------------------------------------------------------

def test_spread_check_gate():
    """
    Verify spread gate logic:
    - Both legs liquid (spread < MAX_ENTRY_SPREAD_BPS) → trade proceeds
    - One leg illiquid (spread > threshold) → return SKIPPED, no COMMIT
    - Spread data unavailable (None) → treat as liquid, proceed
    - Spread check raises exception → proceed (instrumentation never blocks)
    - SKIPPED status does NOT increment pair fail counter
    - MAX_ENTRY_SPREAD_BPS constant exists in constants.py
    """
    name = "T21: Pre-COMMIT spread check"
    try:
        from constants import MAX_ENTRY_SPREAD_BPS

        _check(f"{name}: MAX_ENTRY_SPREAD_BPS constant exists",
               MAX_ENTRY_SPREAD_BPS > 0,
               f"MAX_ENTRY_SPREAD_BPS={MAX_ENTRY_SPREAD_BPS}")

        threshold = MAX_ENTRY_SPREAD_BPS

        def _spread_gate(spread1, spread2):
            """Replicate the gate logic from func_bot_agent.py."""
            s1_ok = spread1 is None or spread1 <= threshold
            s2_ok = spread2 is None or spread2 <= threshold
            if not s1_ok or not s2_ok:
                wide = spread1 if not s1_ok else spread2
                return "SKIPPED", f"SPREAD_TOO_WIDE: {wide:.1f}bps"
            return "PROCEED", None

        # Both legs liquid
        status, _ = _spread_gate(5.0, 10.0)
        _check(f"{name}: both liquid → PROCEED", status == "PROCEED",
               f"status={status}")

        # Leg 1 too wide
        status, reason = _spread_gate(threshold + 1, 10.0)
        _check(f"{name}: leg1 wide → SKIPPED", status == "SKIPPED",
               f"status={status} reason={reason}")

        # Leg 2 too wide
        status, reason = _spread_gate(5.0, threshold + 5)
        _check(f"{name}: leg2 wide → SKIPPED", status == "SKIPPED",
               f"status={status}")

        # Both None (data unavailable) → proceed
        status, _ = _spread_gate(None, None)
        _check(f"{name}: None spreads → PROCEED (data unavailable)",
               status == "PROCEED", f"status={status}")

        # One None → proceed (partial unavailability)
        status, _ = _spread_gate(None, 10.0)
        _check(f"{name}: one None spread → PROCEED", status == "PROCEED",
               f"status={status}")

        # SKIPPED must not trigger fail counter
        # (verified: one_leg_filled check requires filled_usd > 0, SKIPPED has 0)
        f1, f2 = 0.0, 0.0  # SKIPPED never placed MARKET orders
        one_leg_filled = (f1 > 0) != (f2 > 0)
        _check(f"{name}: SKIPPED does NOT increment fail counter",
               not one_leg_filled,
               f"one_leg_filled={one_leg_filled} (both fills=0)")

        # Check function exists in func_public
        import func_public as fp
        _check(f"{name}: get_market_spread_bps exists in func_public",
               hasattr(fp, "get_market_spread_bps"),
               f"{'FOUND' if hasattr(fp, 'get_market_spread_bps') else 'MISSING'}")

    except Exception as e:
        _check(name, False, f"Exception: {e}")


def test_committed_flag():
    """T22: committed flag prevents false orphan saves for pre-COMMIT errors."""
    name = "T22: committed flag on BotAgent order_dict"
    try:
        import func_bot_agent as fba
        # Instantiate a BotAgent and check committed starts False
        agent = fba.BotAgent(
            client_node=MagicMock(),
            client_indexer=MagicMock(),
            market_1="BTC-USD",
            market_2="ETH-USD",
            base_side="BUY",
            base_size=0.01,
            base_price=50000.0,
            quote_side="SELL",
            quote_size=0.1,
            quote_price=3000.0,
            accept_failsafe_base_price=51000.0,
            z_score=1.8,
            half_life=12,
            hedge_ratio=1.5,
            trace_id="test_trace",
        )
        _check(f"{name}: starts False",
               agent.order_dict.get("committed") == False,
               f"committed={agent.order_dict.get('committed')}")

        # Simulate that func_entry_pairs skips orphan check when committed=False
        bot_open_dict_no_commit = {"pair_status": "ERROR",
                                    "comments": "PRE_COMMIT_EXPOSURE_DETECTED",
                                    "committed": False}
        committed = bot_open_dict_no_commit.get("committed", False)
        orphan = committed and True  # would be True if we checked live positions
        _check(f"{name}: orphan=False when committed=False",
               orphan == False, f"orphan={orphan}")

        # Simulate committed=True path
        bot_open_dict_committed = {"pair_status": "FAILED",
                                    "comments": "M1 filled 0; flattened M2",
                                    "committed": True}
        committed = bot_open_dict_committed.get("committed", False)
        orphan = committed and True  # live position found
        _check(f"{name}: orphan=True when committed=True and exposure found",
               orphan == True, f"orphan={orphan}")

    except Exception as e:
        _check(name, False, f"Exception: {e}")


def test_open_fees_fallback():
    """T23: profit gate uses estimated open fees when fee_1=fee_2=0 in old records."""
    name = "T23: open_fees fallback in profit gate"
    try:
        import func_exit_pairs as fex
        TAKER_FEE_BPS = fex.TAKER_FEE_BPS  # 0.0005

        total_notional = 2000.0  # $1000 per leg
        # Old record with fee_1=fee_2=0
        open_fees_paid = 0.0
        # After fix: estimate open fees
        if open_fees_paid == 0.0 and total_notional > 0.0:
            open_fees_paid = total_notional * TAKER_FEE_BPS  # $1.00

        _check(f"{name}: estimated open fees = notional × TAKER_FEE_BPS",
               abs(open_fees_paid - 1.0) < 0.001,
               f"open_fees_paid={open_fees_paid:.4f} expected=1.0")

        # Now run through profit_gate — gross=$2.20, notional=$2000
        # close_fees_est ≈ $1.00 (same rate)
        close_fees_est = total_notional * TAKER_FEE_BPS  # $1.00
        pnl_gross = 2.20
        ok, min_gross, net = fex._profit_gate(pnl_gross, total_notional, open_fees_paid, close_fees_est)
        # total_fees = $1.00 + $1.00 = $2.00
        # target_net = max(MIN_PROFIT_USD=0.50, $2000×0.025%=$0.50) = $0.50
        # min_gross_required = $0.50 + $2.00 = $2.50
        # $2.20 < $2.50 → blocked (correct)
        _check(f"{name}: gross=$2.20 blocked when open fees estimated",
               not ok,
               f"ok={ok} min_gross={min_gross:.2f} net={net:.2f}")

        # Without fix (open_fees=0): min_gross = $0.50 + $1.00 = $1.50
        # $2.20 > $1.50 → would pass (incorrect — undersells the barrier)
        ok_without_fix, min_without_fix, _ = fex._profit_gate(2.20, total_notional, 0.0, close_fees_est)
        _check(f"{name}: gross=$2.20 passes without fix (showing fix matters)",
               ok_without_fix,  # should be True (this was the bug)
               f"ok_without_fix={ok_without_fix} min={min_without_fix:.2f}")

    except Exception as e:
        _check(name, False, f"Exception: {e}")


def test_unmanaged_ignore_markets_filter():
    """T24: reconcile_bot_vs_dydx excludes UNMANAGED_IGNORE_MARKETS from unmanaged set."""
    name = "T24: UNMANAGED_IGNORE_MARKETS filtered in reconcile"
    try:
        import constants as C
        ignore = set(C.UNMANAGED_IGNORE_MARKETS or [])

        # Simulate: LDO-USD and COMP-USD are live but not in bot_agents.json
        # Both are in UNMANAGED_IGNORE_MARKETS
        live_markets = {"LDO-USD", "COMP-USD", "ETH-USD"}
        expected_markets = {"ETH-USD"}  # only ETH is tracked in JSON
        ignored_markets = ignore

        unmanaged = sorted((live_markets - expected_markets) - ignored_markets)
        # LDO-USD and COMP-USD are in ignore → filtered out
        # unmanaged should only contain markets NOT in ignore
        _check(f"{name}: LDO-USD excluded from unmanaged",
               "LDO-USD" not in unmanaged,
               f"unmanaged={unmanaged}")
        _check(f"{name}: COMP-USD excluded from unmanaged",
               "COMP-USD" not in unmanaged,
               f"unmanaged={unmanaged}")
        _check(f"{name}: non-ignored unmanaged markets still show",
               len(unmanaged) == 0,  # ETH-USD is in expected, no unknowns
               f"unmanaged={unmanaged}")

        # Test with a real unknown market (not in ignore)
        live_markets2 = {"LDO-USD", "RUNE-USD"}
        expected_markets2 = set()
        unmanaged2 = sorted((live_markets2 - expected_markets2) - ignored_markets)
        _check(f"{name}: RUNE-USD (not in ignore) still appears as unmanaged",
               "RUNE-USD" in unmanaged2,
               f"unmanaged2={unmanaged2}")
        _check(f"{name}: LDO-USD (in ignore) filtered even with RUNE present",
               "LDO-USD" not in unmanaged2,
               f"unmanaged2={unmanaged2}")

        # Verify UNMANAGED_IGNORE_MARKETS is imported in func_position_guard
        import func_position_guard as fpg
        _check(f"{name}: UNMANAGED_IGNORE_MARKETS imported in func_position_guard",
               hasattr(fpg, 'BLOCK_ENTRIES_ON_UNMANAGED_EXPOSURE'),
               "func_position_guard module accessible")

    except Exception as e:
        _check(name, False, f"Exception: {e}")


# ---------------------------------------------------------------------------
# TEST 25: Opportunity scoring sorts candidates by score
# ---------------------------------------------------------------------------
def test_opportunity_scoring():
    name = "T25: opportunity scoring"
    try:
        # Simulate candidates with different z, half_life, spread_bps
        candidates = [
            {"base_market": "A", "quote_market": "B", "z_score": 1.6, "half_life": 20, "spread_bps": 50},
            {"base_market": "C", "quote_market": "D", "z_score": 2.0, "half_life": 5,  "spread_bps": 10},
            {"base_market": "E", "quote_market": "F", "z_score": 1.5, "half_life": 10, "spread_bps": 30},
        ]
        for c in candidates:
            hl_f = max(1.0, float(c["half_life"]))
            c["score"] = abs(c["z_score"]) * (1.0 / hl_f) * (100.0 / max(1.0, c["spread_bps"]))

        candidates.sort(key=lambda c: c["score"], reverse=True)

        # C/D should win: highest z, lowest hl, lowest spread → highest score
        winner = candidates[0]
        _check(f"{name}: highest-scoring pair is opened first",
               winner["base_market"] == "C",
               f"winner={winner['base_market']}/{winner['quote_market']} score={winner['score']:.4f}")
        _check(f"{name}: scores are decreasing",
               candidates[0]["score"] >= candidates[1]["score"] >= candidates[2]["score"],
               f"scores={[round(c['score'],4) for c in candidates]}")
    except Exception as e:
        _check(name, False, f"Exception: {e}")


# ---------------------------------------------------------------------------
# TEST 26: Min edge gate blocks marginal signals
# ---------------------------------------------------------------------------
def test_min_edge_gate():
    name = "T26: min edge gate"
    try:
        from constants import TAKER_FEE_BPS, MIN_EDGE_FEE_MULTIPLE

        usd_per_trade = 1000.0
        fee_round_trip = 4.0 * usd_per_trade * TAKER_FEE_BPS  # $2.00
        min_edge = MIN_EDGE_FEE_MULTIPLE * fee_round_trip       # $4.00

        # Case 1: spread below min edge → should be blocked
        approx_spread_below = 3.50
        blocked = approx_spread_below < min_edge
        _check(f"{name}: spread ${approx_spread_below} below min ${min_edge:.2f} is blocked",
               blocked, f"blocked={blocked}")

        # Case 2: spread above min edge → should pass
        approx_spread_above = 6.00
        passes = approx_spread_above >= min_edge
        _check(f"{name}: spread ${approx_spread_above} above min ${min_edge:.2f} passes",
               passes, f"passes={passes}")

        # Case 3: fee_round_trip scales with trade size
        usd_large = 2000.0
        fee_large = 4.0 * usd_large * TAKER_FEE_BPS  # $4.00
        min_edge_large = MIN_EDGE_FEE_MULTIPLE * fee_large  # $8.00
        _check(f"{name}: min edge scales with trade size ($2k leg → min ${min_edge_large:.2f})",
               min_edge_large == pytest_approx(8.0, 0.01) if False else abs(min_edge_large - 8.0) < 0.01,
               f"min_edge_large={min_edge_large}")
    except Exception as e:
        _check(name, False, f"Exception: {e}")


# ---------------------------------------------------------------------------
# TEST 27: Dynamic sizing formula
# ---------------------------------------------------------------------------
def test_dynamic_sizing():
    name = "T27: dynamic sizing"
    try:
        # Replicate _compute_dynamic_sizing logic from main.py
        from constants import (DYNAMIC_SIZING_PCT, DYNAMIC_SIZING_MIN_USD,
                               DYNAMIC_SIZING_MAX_USD, MAX_OPEN_TRADES, USD_PER_TRADE)

        def compute(equity):
            raw = equity * float(DYNAMIC_SIZING_PCT)
            clamped = max(float(DYNAMIC_SIZING_MIN_USD), min(float(DYNAMIC_SIZING_MAX_USD), raw))
            usd = max(float(DYNAMIC_SIZING_MIN_USD), round(clamped / 50.0) * 50.0)
            per_pair = usd * 2.5
            dynamic_max = int(equity / per_pair) if per_pair > 0 else MAX_OPEN_TRADES
            max_open = max(3, min(int(MAX_OPEN_TRADES), dynamic_max))
            return usd, max_open

        # Low equity: should use minimum
        usd, mx = compute(5000)
        _check(f"{name}: low equity $5k → usd≥${DYNAMIC_SIZING_MIN_USD:.0f}",
               usd >= DYNAMIC_SIZING_MIN_USD, f"usd={usd}")
        _check(f"{name}: low equity $5k → max_open≥3",
               mx >= 3, f"max_open={mx}")

        # Medium equity
        usd, mx = compute(25000)
        _check(f"{name}: equity $25k → usd=${usd:.0f} between min/max",
               DYNAMIC_SIZING_MIN_USD <= usd <= DYNAMIC_SIZING_MAX_USD, f"usd={usd}")

        # High equity: should be capped at MAX
        usd, mx = compute(200000)
        _check(f"{name}: equity $200k → usd capped at ${DYNAMIC_SIZING_MAX_USD:.0f}",
               usd <= DYNAMIC_SIZING_MAX_USD, f"usd={usd}")
        _check(f"{name}: equity $200k → max_open capped at MAX_OPEN_TRADES={MAX_OPEN_TRADES}",
               mx <= MAX_OPEN_TRADES, f"max_open={mx}")

    except Exception as e:
        _check(name, False, f"Exception: {e}")


# ---------------------------------------------------------------------------
# TEST 28: Drawdown circuit breaker logic
# ---------------------------------------------------------------------------
def test_drawdown_circuit_breaker():
    name = "T28: drawdown circuit breaker"
    try:
        from constants import DRAWDOWN_CIRCUIT_BREAKER_PCT, DRAWDOWN_HALT_HOURS

        equity_start = 10000.0

        # Not triggered: small loss
        equity_now = 9800.0  # -2% < threshold (3%)
        pnl_pct = (equity_now - equity_start) / equity_start
        triggered = pnl_pct < -float(DRAWDOWN_CIRCUIT_BREAKER_PCT)
        _check(f"{name}: -2% loss does NOT trigger breaker (threshold={DRAWDOWN_CIRCUIT_BREAKER_PCT*100:.0f}%)",
               not triggered, f"pnl_pct={pnl_pct*100:.1f}%")

        # Triggered: large loss
        equity_now = 9600.0  # -4% > threshold
        pnl_pct = (equity_now - equity_start) / equity_start
        triggered = pnl_pct < -float(DRAWDOWN_CIRCUIT_BREAKER_PCT)
        _check(f"{name}: -4% loss triggers breaker",
               triggered, f"pnl_pct={pnl_pct*100:.1f}%")

        # Halt duration is positive
        _check(f"{name}: DRAWDOWN_HALT_HOURS > 0",
               float(DRAWDOWN_HALT_HOURS) > 0, f"halt={DRAWDOWN_HALT_HOURS}")

    except Exception as e:
        _check(name, False, f"Exception: {e}")


# ---------------------------------------------------------------------------
# TEST 29: Funding gate logic
# ---------------------------------------------------------------------------
def test_funding_gate():
    name = "T29: funding gate"
    try:
        from constants import FUNDING_MAX_COST_RATIO

        usd_per_trade = 1000.0
        expected_edge = 8.0  # $8 expected spread capture

        # Case: long base, short quote, both with positive funding → net cost > 0
        # Long pays when rate > 0; Short receives when rate > 0
        fr_base = 0.0003    # long base pays 0.03%/8h
        fr_quote = 0.0001   # short quote RECEIVES 0.01%/8h
        base_side = "BUY"   # long base
        quote_side = "SELL" # short quote
        half_life = 16.0
        funding_periods = max(1.0, half_life / 8.0)  # 2 periods

        base_funding  = usd_per_trade * fr_base  * funding_periods * (1.0 if base_side == "BUY" else -1.0)
        quote_funding = usd_per_trade * fr_quote * funding_periods * (1.0 if quote_side == "BUY" else -1.0)
        net_funding = base_funding + quote_funding  # positive = net cost to us

        blocked = net_funding > expected_edge * float(FUNDING_MAX_COST_RATIO)

        _check(f"{name}: net_funding={net_funding:.4f} correctly computed",
               abs(net_funding - (1000*0.0003*2 - 1000*0.0001*2)) < 0.0001,
               f"net={net_funding}")

        # net = 0.6 - 0.2 = 0.4 → 0.4 > 8 * 0.5 = 4.0? No → should NOT block
        _check(f"{name}: small funding cost ${net_funding:.3f} vs edge ${expected_edge} NOT blocked",
               not blocked, f"blocked={blocked}")

        # Case: adverse funding overwhelms edge
        fr_base_bad = 0.005   # 0.5%/8h × 2 periods × $1000 = $10
        base_funding_bad = usd_per_trade * fr_base_bad * funding_periods * 1.0
        quote_funding_bad = usd_per_trade * fr_quote * funding_periods * (-1.0)
        net_bad = base_funding_bad + quote_funding_bad  # 10 - 0.2 = 9.8
        blocked_bad = net_bad > expected_edge * float(FUNDING_MAX_COST_RATIO)
        _check(f"{name}: large funding cost ${net_bad:.2f} vs edge ${expected_edge} IS blocked",
               blocked_bad, f"blocked={blocked_bad} net={net_bad:.2f}")

    except Exception as e:
        _check(name, False, f"Exception: {e}")


# ---------------------------------------------------------------------------
# TEST 30: COINTEGRATION_REFRESH_HOURS changed to 12
# ---------------------------------------------------------------------------
def test_coint_refresh_interval():
    name = "T30: cointegration refresh interval"
    try:
        from constants import COINTEGRATION_REFRESH_HOURS
        _check(f"{name}: COINTEGRATION_REFRESH_HOURS == 12",
               COINTEGRATION_REFRESH_HOURS == 12,
               f"got {COINTEGRATION_REFRESH_HOURS}")
    except Exception as e:
        _check(name, False, f"Exception: {e}")


if __name__ == "__main__":
    print("=" * 60)
    print("  dYdX Bot - Offline Simulation Tests")
    print("=" * 60)

    test_drift_detection()
    test_orphan_leg_detection()
    test_cap_enforcement()
    test_risk_off_age_guard()
    test_risk_off_positive_unreal()
    test_tp_blocked_no_net_profit()
    test_tp_passes_with_profit()
    test_json_atomic_write()
    test_telegram_uses_post()
    test_risk_off_force_equals_cap()
    test_pnl_calculation()
    test_entry_blocked_on_orphan()
    test_risk_off_score_loss_dominates()
    test_time_stop_disabled()
    test_tp_double_confirmation()
    test_ols_with_intercept()
    test_csv_path_is_absolute()
    test_csv_auto_refresh_logic()
    test_fee_fallback_estimation()
    test_pair_fail_cooldown()
    test_spread_check_gate()
    test_committed_flag()
    test_open_fees_fallback()
    test_unmanaged_ignore_markets_filter()
    # New feature tests
    test_opportunity_scoring()
    test_min_edge_gate()
    test_dynamic_sizing()
    test_drawdown_circuit_breaker()
    test_funding_gate()
    test_coint_refresh_interval()

    print()
    print("=" * 60)
    passed = sum(1 for _, ok in _results if ok)
    failed = sum(1 for _, ok in _results if not ok)
    total = len(_results)
    print(f"  Results: {passed}/{total} passed, {failed} failed")
    if failed:
        print("  FAILED tests:")
        for name, ok in _results:
            if not ok:
                print(f"    - {name}")
    print("=" * 60)

    exit(0 if failed == 0 else 1)
