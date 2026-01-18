from func_messaging import send_message
from func_pnl import leg_pnl
from constants import (
    WALLET_ADDRESS,
    CLOSE_AT_ZSCORE_CROSS,

    # Z exits
    USE_Z_TP, USE_Z_SL, USE_TIME_STOP,
    Z_TP, Z_SL_DELTA, TIME_STOP_HOURS,

    # Monetary TP gate
    USE_MIN_PROFIT_TP,
    MIN_PROFIT_PCT,
    MIN_PROFIT_USD,

    # Optional: clean stale JSON records
    DROP_STALE_RECORDS,
)
from func_utils import format_number
from func_public import get_candles_recent
from func_cointegration import calculate_zscore
from func_private import place_market_order
from v4_proto.dydxprotocol.clob.order_pb2 import Order
import json
import asyncio
from datetime import datetime, timezone


JSON_PATH = "bot_agents.json"


def _sf(x, d=0.0):
    try:
        return float(x)
    except Exception:
        return d


def _parse_opened_at(v):
    """
    Accepts ISO strings like '2026-01-17T15:30:00Z' or unix seconds.
    Returns aware datetime in UTC or None.
    """
    if not v:
        return None
    try:
        if isinstance(v, (int, float)):
            return datetime.fromtimestamp(float(v), tz=timezone.utc)
        if isinstance(v, str):
            s = v.strip()
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
    except Exception:
        return None
    return None


def _has_pnl_fields(position: dict) -> bool:
    side_1 = position.get("side_1") or position.get("entry_side_1")
    side_2 = position.get("side_2") or position.get("entry_side_2")
    p1 = _sf(position.get("price_1") or position.get("entry_price_1"), 0.0)
    p2 = _sf(position.get("price_2") or position.get("entry_price_2"), 0.0)
    s1 = _sf(position.get("size_1") or position.get("entry_size_1"), 0.0)
    s2 = _sf(position.get("size_2") or position.get("entry_size_2"), 0.0)
    return (
        side_1 in ("BUY", "SELL")
        and side_2 in ("BUY", "SELL")
        and p1 > 0 and p2 > 0
        and s1 > 0 and s2 > 0
    )

def _compute_notional(position: dict) -> float:
    """
    Total notional at entry (proxy): price_1*size_1 + price_2*size_2
    """
    p1 = _sf(position.get("price_1") or position.get("entry_price_1"), 0.0)
    p2 = _sf(position.get("price_2") or position.get("entry_price_2"), 0.0)
    s1 = _sf(position.get("size_1") or position.get("entry_size_1"), 0.0)
    s2 = _sf(position.get("size_2") or position.get("entry_size_2"), 0.0)
    if p1 <= 0 or p2 <= 0 or s1 <= 0 or s2 <= 0:
        return 0.0
    return abs(p1 * s1) + abs(p2 * s2)

async def manage_trade_exits(node, indexer, wallet):
    """
    Manage exiting open positions:
     Exits supported:
      ✅ TP by z-score near 0 (abs(z_now) <= Z_TP)
         + optional monetary gate (min profit % / USD) to avoid fee-eaten micro-TPs
      ✅ SL by z-score break (abs(z_now) >= abs(z_entry) + Z_SL_DELTA)
      ✅ Time stop (age_hours >= TIME_STOP_HOURS)
      ✅ Optional CROSS rule (legacy)

    Notes:
      - Closes with reduce-only IOC, using ACTUAL live position sizes from subaccount.
      - PnL is a PROXY using oraclePrice for exit price unless you wire real fills.
      - Saves JSON AFTER processing all items (outside the loop).
      - Optionally drops stale JSON records when one leg is no longer live.
    """

    # -----------------------------
    # Load saved open trades
    # -----------------------------
    try:
        with open(JSON_PATH, "r") as f:
            open_positions_list = json.load(f)
    except Exception:
        return "complete"

    if not open_positions_list:
        return "complete"

    # -----------------------------
    # Pull market metadata once (stepSize, oraclePrice)
    # -----------------------------
    markets_resp = await indexer.markets.get_perpetual_markets()
    markets = markets_resp.get("markets", {}) or {}

    # -----------------------------
    # Pull live open positions once
    # -----------------------------
    try:
        account_resp = await indexer.account.get_subaccount(WALLET_ADDRESS, 0)
        subaccount = account_resp.get("subaccount", {}) or {}
        positions = subaccount.get("openPerpetualPositions", {}) or subaccount.get("perpetualPositions", {}) or {}
    except Exception as e:
        print(f"Error reading subaccount positions: {e}")
        return "error"

    # Only markets with non-zero size
    live_pos = {}
    for m, pdata in (positions or {}).items():
        sz = _sf(pdata.get("size", 0.0))
        if abs(sz) > 0:
            live_pos[m] = sz

    now_utc = datetime.now(timezone.utc)
    save_output = []

    # -----------------------------
    # Loop saved pairs
    # -----------------------------
    for position in open_positions_list:
        is_close = False
        close_reason = None

        m1 = position.get("market_1")
        m2 = position.get("market_2")

        if not m1 or not m2:
            # malformed record
            continue

        # Guard: both legs still exist as live positions
        if m1 not in live_pos or m2 not in live_pos:
            if DROP_STALE_RECORDS:
                msg = f"🧹 Dropping stale record (leg not live) for {m1}/{m2}. Freeing slot."
                print(msg)
                send_message(msg)
                # drop it (do not append to save_output)
                continue
            else:
                warn = f"⚠️ One leg not live anymore for {m1}/{m2}. Keeping record for review."
                print(warn)
                send_message(warn)
                save_output.append(position)
                continue

        hedge_ratio = _sf(position.get("hedge_ratio", 0.0))
        z_entry = _sf(position.get("z_score", 0.0))

        # -----------------------------
        # Compute current z-score
        # -----------------------------
        z_now = None
        try:
            series_1 = await get_candles_recent(indexer, m1)
            series_2 = await get_candles_recent(indexer, m2)

            n = min(len(series_1), len(series_2))
            if n > 10:
                series_1 = series_1[-n:]
                series_2 = series_2[-n:]
                spread = series_1 - (hedge_ratio * series_2)
                z_now = float(calculate_zscore(spread).values.tolist()[-1])
            else:
                z_now = None
        except Exception as e:
            print(f"Error computing z-score for {m1}/{m2}: {e}")
            z_now = None

        print(f"[EXIT CHECK] {m1}/{m2} z_now={z_now} z_entry={z_entry}")

        # -----------------------------
        # PnL proxy + notional (for monetary TP gate)
        # -----------------------------
        can_pnl = _has_pnl_fields(position)

        entry1_side = position.get("side_1") or position.get("entry_side_1")
        entry2_side = position.get("side_2") or position.get("entry_side_2")

        entry1_price = _sf(position.get("price_1") or position.get("entry_price_1"), 0.0)
        entry2_price = _sf(position.get("price_2") or position.get("entry_price_2"), 0.0)

        entry1_size = _sf(position.get("size_1") or position.get("entry_size_1"), 0.0)
        entry2_size = _sf(position.get("size_2") or position.get("entry_size_2"), 0.0)

        exit1_price = _sf(markets.get(m1, {}).get("oraclePrice"), 0.0)
        exit2_price = _sf(markets.get(m2, {}).get("oraclePrice"), 0.0)

        pnl1 = pnl2 = pnl_total = 0.0
        if can_pnl and exit1_price > 0 and exit2_price > 0:
            pnl1 = leg_pnl(entry1_side, entry1_price, exit1_price, entry1_size)
            pnl2 = leg_pnl(entry2_side, entry2_price, exit2_price, entry2_size)
            pnl_total = pnl1 + pnl2

        total_notional = _compute_notional(position)
        min_profit_required = 0.0
        if USE_MIN_PROFIT_TP:
            min_profit_required = max(
                float(MIN_PROFIT_USD),
                float(MIN_PROFIT_PCT) * float(total_notional)
            )

        # -----------------------------
        # ✅ Rule 1: Take Profit near 0 + optional monetary gate to avoid fee-eaten exits
        # -----------------------------
        if (not is_close) and USE_Z_TP and (z_now is not None):
            if abs(z_now) <= float(Z_TP):
                if USE_MIN_PROFIT_TP:
                    if can_pnl and (pnl_total >= min_profit_required):
                        is_close = True
                        close_reason = (
                            f"TP: abs(z)={abs(z_now):.4f}<=Z_TP={float(Z_TP):.4f} | "
                            f"PnL={pnl_total:.2f}>=MinProfit={min_profit_required:.2f}"
                        )
                    else:
                        # Do NOT close yet; wait for better move or time/SL exits
                        # Keep record
                        save_output.append(position)
                        continue
                else:
                    is_close = True
                    close_reason = f"TP: abs(z_now)={abs(z_now):.4f} <= Z_TP={float(Z_TP):.4f}"

        # -----------------------------
        # ✅ Rule 2: Stop Loss relative to entry
        # -----------------------------
        if (not is_close) and USE_Z_SL and (z_now is not None):
            sl_level = abs(z_entry) + float(Z_SL_DELTA)
            if abs(z_now) >= sl_level:
                is_close = True
                close_reason = f"SL: abs(z_now)={abs(z_now):.4f} >= abs(z_entry)+Z_SL_DELTA={sl_level:.4f}"

        # -----------------------------
        # ✅ Rule 3: Time stop
        # -----------------------------
        if (not is_close) and USE_TIME_STOP:
            opened_at = _parse_opened_at(position.get("opened_at"))
            if opened_at is not None:
                age_hours = (now_utc - opened_at).total_seconds() / 3600.0
                if age_hours >= float(TIME_STOP_HOURS):
                    is_close = True
                    close_reason = f"TIME: age_hours={age_hours:.2f} >= TIME_STOP_HOURS={float(TIME_STOP_HOURS):.2f}"

        # -----------------------------
        # (Optional) CROSS rule
        # -----------------------------
        if (not is_close) and CLOSE_AT_ZSCORE_CROSS and (z_now is not None):
            z_score_level_check = abs(z_now) >= abs(z_entry)
            z_score_cross_check = (
                (z_now < 0 and z_entry > 0) or
                (z_now > 0 and z_entry < 0)
            )
            if z_score_level_check and z_score_cross_check:
                is_close = True
                close_reason = f"CROSS: z_now={z_now:.4f} crossed z_entry={z_entry:.4f} with level check"

        # -----------------------------
        # If not closing, keep record
        # -----------------------------
        if not is_close:
            save_output.append(position)
            continue

        # -----------------------------
        # Close both legs (reduce-only) using ACTUAL live sizes
        # -----------------------------
        try:
            # Leg 1
            size_m1 = float(live_pos[m1])
            close_side_m1 = "SELL" if size_m1 > 0 else "BUY"
            close_size_m1 = abs(size_m1)

            # Leg 2
            size_m2 = float(live_pos[m2])
            close_side_m2 = "SELL" if size_m2 > 0 else "BUY"
            close_size_m2 = abs(size_m2)

            # Format sizes to stepSize
            step_m1 = markets.get(m1, {}).get("stepSize")
            step_m2 = markets.get(m2, {}).get("stepSize")
            if step_m1:
                close_size_m1 = float(format_number(close_size_m1, step_m1))
            if step_m2:
                close_size_m2 = float(format_number(close_size_m2, step_m2))

            header = f">>> Closing pair: {m1} & {m2} | Reason: {close_reason}"
            print("\n" + header)
            send_message(header)

            # Report PnL proxy + threshold if available
            if can_pnl and exit1_price > 0 and exit2_price > 0:
                extra = ""
                if USE_MIN_PROFIT_TP and min_profit_required > 0:
                    extra = f"\nMinProfit gate: ${min_profit_required:,.2f} (notional ${total_notional:,.2f})"
                send_message(
                    "📈 *PnL (proxy oraclePrice)*\n"
                    f"{m1}: ${pnl1:,.2f}\n"
                    f"{m2}: ${pnl2:,.2f}\n"
                    f"TOTAL: ${pnl_total:,.2f}{extra}"
                )
            else:
                send_message(
                    "ℹ️ PnL not calculated: Missing fields in bot_agents.json.\n"
                    "You need (per trade): opened_at + side_1/price_1/size_1 + side_2/price_2/size_2."
                )

            print(f"Leg1 close: {close_side_m1} {m1} size={close_size_m1}")
            print(f"Leg2 close: {close_side_m2} {m2} size={close_size_m2}")

            await place_market_order(
                node, indexer, wallet,
                m1, close_side_m1, close_size_m1, markets[m1]["oraclePrice"],
                True,  # reduce_only
                time_in_force_type=Order.TimeInForce.TIME_IN_FORCE_IOC
            )

            await asyncio.sleep(0.5)

            await place_market_order(
                node, indexer, wallet,
                m2, close_side_m2, close_size_m2, markets[m2]["oraclePrice"],
                True,  # reduce_only
                time_in_force_type=Order.TimeInForce.TIME_IN_FORCE_IOC
            )

            print(">>> Closed both legs (reduce-only).")
            send_message(">>> Closed both legs (reduce-only).")

        except Exception as e:
            print(f"Exit failed for {m1} with {m2}: {e}")
            send_message(f"Exit failed for {m1} with {m2}: {e}")
            # Keep record if exit fails
            save_output.append(position)

    # -----------------------------
    # Save remaining items (AFTER loop)
    # -----------------------------
    print(f"{len(save_output)} Items remaining. Saving file...")
    send_message(f"{len(save_output)} Items remaining. Saving file...")
    with open(JSON_PATH, "w") as f:
        json.dump(save_output, f, indent=2)

    return "complete"
