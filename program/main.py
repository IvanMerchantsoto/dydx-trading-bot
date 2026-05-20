import asyncio
import os
import time

from func_connections import connect_dydx
from func_private import abort_all_positions
from func_public import construct_market_prices
from func_entry_pairs import open_positions
from func_cointegration import store_cointegration_results, CSV_PATH as COINT_CSV_PATH
from func_exit_pairs import manage_trade_exits
from func_kpis import send_account_kpis
from func_risk_off import risk_off_close_worst_pair
from func_messaging import send_message
from func_logging import log_event
from func_position_guard import assert_safe_to_open, close_markets_actual

from constants import (
    ABORT_ALL_POSITIONS,
    FIND_COINTEGRATED,
    PLACE_TRADES,
    MANAGE_EXITS,
    EXIT_CHECK_SECONDS,
    KPI_SECONDS,
    SESSION_SUMMARY_SECONDS,
    BATCH_OPEN_TRADES,
    MAX_OPEN_TRADES,
    USD_PER_TRADE,
    RISK_OFF_ENABLED,
    RISK_OFF_FREE_COLLATERAL_TRIGGER,
    RISK_OFF_FORCE_IF_OPEN_TRADES_GE,
    COINTEGRATION_REFRESH_HOURS,
    UNMANAGED_IGNORE_MARKETS,
    WALLET_ADDRESS,
    # Dynamic sizing
    DYNAMIC_SIZING,
    DYNAMIC_SIZING_PCT,
    DYNAMIC_SIZING_MIN_USD,
    DYNAMIC_SIZING_MAX_USD,
    # Drawdown circuit breaker
    DRAWDOWN_CIRCUIT_BREAKER_ENABLED,
    DRAWDOWN_CIRCUIT_BREAKER_PCT,
    DRAWDOWN_HALT_HOURS,
)


def _csv_needs_refresh() -> bool:
    if COINTEGRATION_REFRESH_HOURS <= 0:
        return False
    if not os.path.exists(COINT_CSV_PATH):
        return True
    age_hours = (time.time() - os.path.getmtime(COINT_CSV_PATH)) / 3600.0
    return age_hours >= float(COINTEGRATION_REFRESH_HOURS)


async def _run_cointegration(node, indexer):
    print("Fetching market prices for cointegration, please allow 3 minutes...", flush=True)
    send_message("🔄 Refreshing cointegrated pairs (this takes ~3 min)...")
    df_market_prices = await construct_market_prices(node, indexer)
    try:
        _pkl = os.path.join(os.path.dirname(os.path.abspath(__file__)), "market_prices.pkl")
        df_market_prices.to_pickle(_pkl)
    except Exception:
        pass
    result = store_cointegration_results(df_market_prices)
    if result != "saved":
        raise RuntimeError("store_cointegration_results did not return 'saved'")
    log_event({"type": "cointegration_refresh_done", "csv": COINT_CSV_PATH})
    send_message("✅ Cointegrated pairs refreshed.")


def _compute_dynamic_sizing(equity: float) -> tuple:
    """
    Compute (usd_per_trade, max_open_trades) from current equity.

    usd_per_trade   = clamp(equity × DYNAMIC_SIZING_PCT, MIN, MAX), rounded to $50
    max_open_trades = floor(equity / (usd_per_trade × 2.5)), capped at MAX_OPEN_TRADES
                      2.5× buffer ensures initial margin + SL headroom for all pairs.
    """
    if equity <= 0:
        return float(USD_PER_TRADE), int(MAX_OPEN_TRADES)

    raw = equity * float(DYNAMIC_SIZING_PCT)
    clamped = max(float(DYNAMIC_SIZING_MIN_USD), min(float(DYNAMIC_SIZING_MAX_USD), raw))
    # Round to nearest $50 for clean sizing
    usd_per_trade = max(float(DYNAMIC_SIZING_MIN_USD), round(clamped / 50.0) * 50.0)

    # Max open trades bounded by capital buffer
    per_pair_requirement = usd_per_trade * 2.5
    dynamic_max = int(equity / per_pair_requirement) if per_pair_requirement > 0 else MAX_OPEN_TRADES
    max_open = max(3, min(int(MAX_OPEN_TRADES), dynamic_max))

    return usd_per_trade, max_open


async def main():
    send_message("Bot Launched Successfully")

    try:
        print("Connecting to client...", flush=True)
        node, indexer, wallet = await connect_dydx()
    except Exception as e:
        print("Error connecting to client:", e, flush=True)
        send_message("Failed to connect to DYDX.")
        raise

    if ABORT_ALL_POSITIONS:
        try:
            print("Closing all positions...", flush=True)
            await abort_all_positions(
                node, indexer,
                ignore_markets=UNMANAGED_IGNORE_MARKETS,
            )
        except Exception as e:
            print("Error closing all positions:", e, flush=True)
            send_message("Failed to abort all positions.")
            raise

    if FIND_COINTEGRATED or _csv_needs_refresh():
        reason = "FIND_COINTEGRATED=True" if FIND_COINTEGRATED else f"CSV stale/missing (>{COINTEGRATION_REFRESH_HOURS}h)"
        print(f"[COINT] Running cointegration scan. Reason: {reason}", flush=True)
        try:
            await _run_cointegration(node, indexer)
        except Exception as e:
            print(f"Error cointegrating pairs: {e}", flush=True)
            send_message(f"⚠️ Failed to refresh cointegrated pairs: {e}")
            if not os.path.exists(COINT_CSV_PATH):
                raise

    loop = asyncio.get_event_loop()

    opened_since_exit = 0
    last_kpi_ts = loop.time()
    last_coint_refresh_ts = loop.time()
    last_summary_ts = loop.time()
    session_start_wall = time.time()   # wall-clock start for session age display

    # ── Drawdown circuit breaker state ────────────────────────────────────
    # equity_session_start is seeded on the first successful KPI fetch.
    equity_session_start: float = None
    drawdown_halt_until: float = 0.0   # loop.time() timestamp; 0 = not halted

    # ── Session stats (accumulated across all manage_trade_exits calls) ───
    session_stats = {
        "tp_count": 0,
        "loss_exit_count": 0,
        "hard_sl_count": 0,
        "z_sl_count": 0,
        "orphan_cleanup_count": 0,
        "net_pnl_est_sum": 0.0,
        "total_closed": 0,
    }

    while True:
        now = loop.time()
        print("[D1] loop top", flush=True)

        # ── Periodic cointegration refresh ────────────────────────────────
        print("[D2] coint check", flush=True)
        if COINTEGRATION_REFRESH_HOURS > 0:
            elapsed_coint = (now - last_coint_refresh_ts) / 3600.0
            if elapsed_coint >= float(COINTEGRATION_REFRESH_HOURS):
                print(f"[D2] refreshing coint ({elapsed_coint:.1f}h old)", flush=True)
                try:
                    await _run_cointegration(node, indexer)
                    last_coint_refresh_ts = loop.time()
                except Exception as e_coint:
                    print(f"[D2] coint refresh error: {e_coint}", flush=True)
                    last_coint_refresh_ts = loop.time()

        # ── Manage exits ──────────────────────────────────────────────────
        print(f"[D3] MANAGE_EXITS={MANAGE_EXITS}", flush=True)
        if MANAGE_EXITS:
            try:
                print("[D3] calling manage_trade_exits...", flush=True)
                exit_result = await manage_trade_exits(node, indexer, wallet)
                print("[D3] manage_trade_exits done", flush=True)
                # Accumulate session stats
                if isinstance(exit_result, dict):
                    for k in ("tp_count", "loss_exit_count", "hard_sl_count",
                              "z_sl_count", "orphan_cleanup_count", "total_closed"):
                        session_stats[k] = session_stats.get(k, 0) + int(exit_result.get(k.replace("total_closed", "closed"), 0))
                    session_stats["net_pnl_est_sum"] = (
                        session_stats.get("net_pnl_est_sum", 0.0)
                        + float(exit_result.get("net_pnl_est_sum", 0.0))
                    )
            except Exception as e:
                print(f"[D3] exits error: {e}", flush=True)

        # ── KPIs ──────────────────────────────────────────────────────────
        print(f"[D4] KPI check (elapsed={(now-last_kpi_ts):.0f}s / {KPI_SECONDS}s)", flush=True)
        if now - last_kpi_ts >= KPI_SECONDS:
            try:
                snapshot = await send_account_kpis(indexer)
                last_kpi_ts = now

                if snapshot:
                    current_equity = snapshot.get("equity", 0.0)

                    # Seed session-start equity on first successful read
                    if equity_session_start is None and current_equity > 0:
                        equity_session_start = current_equity
                        log_event({
                            "type": "session_equity_seeded",
                            "equity_session_start": equity_session_start,
                        })

                    # Drawdown circuit breaker evaluation (at KPI time)
                    if (
                        DRAWDOWN_CIRCUIT_BREAKER_ENABLED
                        and equity_session_start
                        and current_equity > 0
                        and drawdown_halt_until <= now
                    ):
                        session_pnl_pct = (current_equity - equity_session_start) / equity_session_start
                        log_event({
                            "type": "drawdown_check",
                            "equity_now": current_equity,
                            "equity_session_start": equity_session_start,
                            "session_pnl_pct": round(session_pnl_pct * 100, 3),
                            "threshold_pct": DRAWDOWN_CIRCUIT_BREAKER_PCT * 100,
                        }, print_terminal=False)

                        if session_pnl_pct < -float(DRAWDOWN_CIRCUIT_BREAKER_PCT):
                            drawdown_halt_until = now + float(DRAWDOWN_HALT_HOURS) * 3600.0
                            log_event({
                                "type": "drawdown_circuit_breaker_triggered",
                                "session_pnl_pct": round(session_pnl_pct * 100, 3),
                                "halt_hours": DRAWDOWN_HALT_HOURS,
                            })
                            send_message(
                                f"⚠️ DRAWDOWN CIRCUIT BREAKER\n"
                                f"Session PnL: {session_pnl_pct*100:.1f}% "
                                f"(threshold: -{DRAWDOWN_CIRCUIT_BREAKER_PCT*100:.0f}%)\n"
                                f"Halting new entries for {DRAWDOWN_HALT_HOURS:.0f}h"
                            )

                    # Risk-off by free collateral
                    if RISK_OFF_ENABLED and snapshot.get("free", 0) < float(RISK_OFF_FREE_COLLATERAL_TRIGGER):
                        await risk_off_close_worst_pair(node, indexer, wallet)

            except Exception as e:
                print(f"[D4] KPI error: {e}", flush=True)
                last_kpi_ts = now

        # ── Session profitability summary ─────────────────────────────────
        print(f"[D4b] summary check (elapsed={(now-last_summary_ts):.0f}s / {SESSION_SUMMARY_SECONDS}s)", flush=True)
        if now - last_summary_ts >= SESSION_SUMMARY_SECONDS:
            last_summary_ts = now
            try:
                session_age_s = time.time() - session_start_wall
                session_age_h = session_age_s / 3600.0
                if session_age_h < 1:
                    age_str = f"{session_age_s/60:.0f}m"
                else:
                    age_str = f"{session_age_h:.1f}h"

                if equity_session_start and equity_session_start > 0:
                    # Fetch current equity for live comparison
                    try:
                        _sresp = await indexer.account.get_subaccount(WALLET_ADDRESS.strip(), 0)
                        _ssub = _sresp.get("subaccount", {}) or {}
                        _cur_eq = float(_ssub.get("equity") or 0)
                    except Exception:
                        _cur_eq = 0.0

                    if _cur_eq > 0:
                        pnl_abs = _cur_eq - equity_session_start
                        pnl_pct = pnl_abs / equity_session_start * 100
                        emoji = "📈" if pnl_abs >= 0 else "📉"
                        sl_count = session_stats.get("hard_sl_count", 0) + session_stats.get("z_sl_count", 0)
                        msg_parts = [
                            f"{emoji} *Sesión {age_str}*",
                            f"Equity: ${equity_session_start:,.0f} → ${_cur_eq:,.2f}",
                            f"PnL real: *${pnl_abs:+.2f}* ({pnl_pct:+.2f}%)",
                            f"Cierres: TP={session_stats.get('tp_count',0)} | loss={session_stats.get('loss_exit_count',0)} | SL={sl_count} | orphan={session_stats.get('orphan_cleanup_count',0)}",
                            f"PnL est cierres: ${session_stats.get('net_pnl_est_sum', 0.0):+.2f}",
                        ]
                        send_message("\n".join(msg_parts))
                        log_event({
                            "type": "session_summary",
                            "age_h": round(session_age_h, 3),
                            "equity_start": equity_session_start,
                            "equity_now": _cur_eq,
                            "pnl_abs": round(pnl_abs, 4),
                            "pnl_pct": round(pnl_pct, 4),
                            **session_stats,
                        }, print_terminal=False)
            except Exception as se:
                print(f"[D4b] session summary error: {se}", flush=True)

        # ── Place trades ──────────────────────────────────────────────────
        print(f"[D5] PLACE_TRADES={PLACE_TRADES}", flush=True)
        if PLACE_TRADES:
            print("[D5] entering PLACE_TRADES block", flush=True)
            try:
                import json as _json
                _json_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bot_agents.json")
                print(f"[D5] reading JSON from {_json_path}", flush=True)
                try:
                    with open(_json_path) as _f:
                        _records = _json.load(_f)
                    open_pairs = len([r for r in _records if isinstance(r, dict)])
                except Exception as je:
                    print(f"[D5] JSON read error: {je}", flush=True)
                    open_pairs = 0

                # ── Dynamic sizing ─────────────────────────────────────────
                eff_usd_per_trade = float(USD_PER_TRADE)
                eff_max_open = int(MAX_OPEN_TRADES)

                if DYNAMIC_SIZING:
                    try:
                        _resp = await indexer.account.get_subaccount(WALLET_ADDRESS.strip(), 0)
                        _sub = _resp.get("subaccount", {}) or {}
                        _equity = float(_sub.get("equity") or 0)
                        if _equity > 0:
                            eff_usd_per_trade, eff_max_open = _compute_dynamic_sizing(_equity)
                            log_event({
                                "type": "dynamic_sizing_computed",
                                "equity": _equity,
                                "usd_per_trade": eff_usd_per_trade,
                                "max_open_trades": eff_max_open,
                            }, print_terminal=False)
                    except Exception as ds_e:
                        print(f"[D5] dynamic sizing error (using defaults): {ds_e}", flush=True)

                print(f"[D5] open_pairs={open_pairs} max={eff_max_open} "
                      f"usd={eff_usd_per_trade:.0f} batch={opened_since_exit}/{BATCH_OPEN_TRADES}", flush=True)

                # ── Drawdown halt check ────────────────────────────────────
                if DRAWDOWN_CIRCUIT_BREAKER_ENABLED and drawdown_halt_until > now:
                    remaining_h = (drawdown_halt_until - now) / 3600.0
                    print(f"[D5] Drawdown circuit breaker — {remaining_h:.2f}h remaining. "
                          f"Skipping new entries.", flush=True)
                    log_event({
                        "type": "drawdown_halt_skip",
                        "remaining_hours": round(remaining_h, 2),
                    }, print_terminal=False)

                elif open_pairs >= eff_max_open:
                    print("[D5] at cap — skipping open", flush=True)
                    if RISK_OFF_ENABLED and open_pairs >= RISK_OFF_FORCE_IF_OPEN_TRADES_GE:
                        await risk_off_close_worst_pair(node, indexer, wallet)

                else:
                    # ── Real reconcile gate (dYdX as source of truth) ──────
                    print("[D5] running reconcile gate...", flush=True)
                    try:
                        safe, reconcile_state = await assert_safe_to_open(indexer)
                    except Exception as rce:
                        print(f"[D5] reconcile gate error (proceeding): {rce}", flush=True)
                        safe = True
                        reconcile_state = {}

                    if not safe:
                        print(f"[D5] reconcile gate blocked entries", flush=True)
                        unmanaged = reconcile_state.get("unmanaged_markets", []) if isinstance(reconcile_state, dict) else []
                        if unmanaged:
                            print(f"[D5] attempting cleanup of unmanaged: {unmanaged}", flush=True)
                            try:
                                await close_markets_actual(
                                    node, indexer, wallet,
                                    unmanaged,
                                    reason="main_loop_unmanaged_cleanup",
                                )
                            except Exception as cme:
                                print(f"[D5] unmanaged cleanup error: {cme}", flush=True)
                    else:
                        remaining_capacity = max(0, eff_max_open - open_pairs)
                        remaining_in_batch = min(
                            max(0, BATCH_OPEN_TRADES - opened_since_exit),
                            remaining_capacity,
                        )
                        print(f"[D5] remaining_in_batch={remaining_in_batch}", flush=True)

                        if remaining_in_batch > 0:
                            print("[D5] calling open_positions...", flush=True)
                            opened_now = await open_positions(
                                node, indexer, wallet,
                                max_new_trades=remaining_in_batch,
                                usd_per_trade=eff_usd_per_trade,
                                max_open_trades_override=eff_max_open,
                            )
                            print(f"[D5] open_positions returned {opened_now}", flush=True)
                            opened_since_exit += int(opened_now or 0)

                        if opened_since_exit >= BATCH_OPEN_TRADES:
                            if MANAGE_EXITS:
                                _batch_exit = await manage_trade_exits(node, indexer, wallet)
                                if isinstance(_batch_exit, dict):
                                    for k in ("tp_count", "loss_exit_count", "hard_sl_count",
                                              "z_sl_count", "orphan_cleanup_count", "total_closed"):
                                        session_stats[k] = session_stats.get(k, 0) + int(_batch_exit.get(k.replace("total_closed", "closed"), 0))
                                    session_stats["net_pnl_est_sum"] = (
                                        session_stats.get("net_pnl_est_sum", 0.0)
                                        + float(_batch_exit.get("net_pnl_est_sum", 0.0))
                                    )
                            opened_since_exit = 0

            except Exception as e:
                print(f"[D5] PLACE_TRADES exception: {e}", flush=True)
                import traceback; traceback.print_exc()

        print(f"[D6] sleeping {EXIT_CHECK_SECONDS}s", flush=True)
        await asyncio.sleep(EXIT_CHECK_SECONDS)


if __name__ == "__main__":
    asyncio.run(main())
