from func_messaging import send_message
from constants import (
    ZSCORE_THRESH, USD_PER_TRADE, USD_MIN_COLLATERAL, MAX_OPEN_TRADES, WINDOW,
    TAKER_FEE_BPS,
    FUNDING_GATE_ENABLED, FUNDING_MAX_COST_RATIO,
    OPPORTUNITY_SCORING,
    MIN_EDGE_FEE_MULTIPLE,
)
from func_utils import format_number
from func_public import get_candles_recent, get_market_spread_bps, get_funding_rates
from func_cointegration import calculate_zscore, CSV_PATH as COINT_CSV_PATH
from datetime import datetime, timezone
from constants import WALLET_ADDRESS
from func_bot_agent import BotAgent
from func_logging import log_event

import pandas as pd
import json
import os
import math
import uuid

JSON_PATH = os.path.join(os.path.dirname(__file__), "bot_agents.json")

# ── Pair-level failure cooldown ───────────────────────────────────────────────
# Si un par falla PAIR_FAIL_COOLDOWN_THRESHOLD veces seguidas (fills=0 o partial
# en ambas legs), se marca en cooldown por PAIR_FAIL_COOLDOWN_HOURS horas.
# Esto evita spamear el mismo par ilíquido en cada ciclo.
PAIR_FAIL_COOLDOWN_PATH = os.path.join(os.path.dirname(__file__), "pair_fail_cooldowns.json")
PAIR_FAIL_COOLDOWN_THRESHOLD = 3    # fallos consecutivos antes de cooldown
PAIR_FAIL_COOLDOWN_HOURS = 4.0      # horas de cooldown tras threshold
PAIR_FAIL_RESET_ON_SUCCESS = True   # resetear contador si hay LIVE

# ── Market concentration limit ────────────────────────────────────────────────
# Máximo de pares abiertos que pueden compartir el mismo mercado.
# Evita que ARKM-USD (presente en 9+ pares del CSV) bloquee todas las señales
# en cuanto 1 par con ARKM está abierto.
# Con MAX_TRADES_PER_MARKET=1: si ADA-USD ya está en un par activo, no se abre
# otro par que use ADA-USD, aunque tenga z muy alto.
MAX_TRADES_PER_MARKET = 1


def _pair_key(m1, m2):
    return "/".join(sorted([str(m1), str(m2)]))


def _load_pair_fails():
    try:
        with open(PAIR_FAIL_COOLDOWN_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_pair_fails(data):
    tmp = PAIR_FAIL_COOLDOWN_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, PAIR_FAIL_COOLDOWN_PATH)


def _is_pair_in_fail_cooldown(m1, m2):
    data = _load_pair_fails()
    key = _pair_key(m1, m2)
    rec = data.get(key)
    if not rec:
        return False

    now_utc = datetime.now(timezone.utc)

    # ── Check post-SL cooldown (set by func_exit_pairs._write_sl_cooldown) ──
    sl_until_str = rec.get("sl_cooldown_until", "")
    if sl_until_str:
        try:
            sl_until = datetime.fromisoformat(sl_until_str.replace("Z", "+00:00"))
            if now_utc < sl_until:
                remaining_h = (sl_until - now_utc).total_seconds() / 3600.0
                log_event({
                    "type": "signal_skip",
                    "reason": "sl_cooldown",
                    "pair": key,
                    "remaining_hours": round(remaining_h, 2),
                    "until": sl_until_str,
                })
                return True
            else:
                # Expired — clear it
                rec.pop("sl_cooldown_until", None)
                rec.pop("sl_cooldown_hours", None)
                rec.pop("sl_ts", None)
                data[key] = rec
                _save_pair_fails(data)
        except Exception:
            pass

    # ── Check execution-failure cooldown (consecutive fails) ──────────────
    count = rec.get("consecutive_fails", 0)
    if count < PAIR_FAIL_COOLDOWN_THRESHOLD:
        return False
    ts_str = rec.get("last_fail_ts", "")
    try:
        last_fail = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        age_h = (now_utc - last_fail).total_seconds() / 3600.0
        if age_h >= PAIR_FAIL_COOLDOWN_HOURS:
            # Cooldown expired — reset counter
            data[key] = {"consecutive_fails": 0, "last_fail_ts": ts_str}
            _save_pair_fails(data)
            return False
        return True
    except Exception:
        return False


def _record_pair_fail(m1, m2):
    data = _load_pair_fails()
    key = _pair_key(m1, m2)
    rec = data.get(key, {"consecutive_fails": 0})
    rec["consecutive_fails"] = int(rec.get("consecutive_fails", 0)) + 1
    rec["last_fail_ts"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    data[key] = rec
    _save_pair_fails(data)
    return rec["consecutive_fails"]


def _record_pair_success(m1, m2):
    if not PAIR_FAIL_RESET_ON_SUCCESS:
        return
    data = _load_pair_fails()
    key = _pair_key(m1, m2)
    if key in data:
        data[key] = {"consecutive_fails": 0, "last_fail_ts": data[key].get("last_fail_ts", "")}
        _save_pair_fails(data)


def _sf(x, d=0.0):
    """Safe float conversion."""
    try:
        return float(x)
    except Exception:
        return d


async def _get_subaccount(indexer):
    resp = await indexer.account.get_subaccount(WALLET_ADDRESS, 0)
    return resp.get("subaccount", {}) or {}


async def _get_live_markets_with_position(indexer):
    """Return a set of markets that currently have a non-zero open position."""
    sub = await _get_subaccount(indexer)
    positions = sub.get("openPerpetualPositions", {}) or sub.get("perpetualPositions", {}) or {}
    live = set()
    for m, p in positions.items():
        if abs(_sf(p.get("size"))) > 0:
            live.add(m)
    return live


def _load_open_pairs_from_json():
    try:
        with open(JSON_PATH, "r") as f:
            data = json.load(f)
        return data if data else []
    except Exception:
        return []


def _count_open_pairs_from_json():
    return len(_load_open_pairs_from_json())


async def open_positions(
    node,
    indexer,
    wallet,
    max_new_trades=1,
    usd_per_trade=None,
    max_open_trades_override=None,
):
    """
    Entry manager — two-phase approach:

    PHASE 1: Collect and score ALL candidates that pass basic filters.
             Score = abs(z) × (1/half_life) × (100/spread_bps) — higher is better.
             Candidates are sorted by score when OPPORTUNITY_SCORING=True.

    PHASE 2: Open the top N candidates, applying funding gate and min-edge gate.

    Parameters
    ----------
    max_new_trades : int
        Maximum new pairs to open this batch.
    usd_per_trade : float or None
        Override USD_PER_TRADE (used for dynamic sizing from main.py).
    max_open_trades_override : int or None
        Override MAX_OPEN_TRADES (used for dynamic sizing from main.py).
    """
    opened_count = 0

    # Effective parameters (may be overridden by dynamic sizing from main.py)
    eff_usd = float(usd_per_trade) if usd_per_trade is not None else float(USD_PER_TRADE)
    eff_max = int(max_open_trades_override) if max_open_trades_override is not None else int(MAX_OPEN_TRADES)

    # Round-trip fee for one pair at this trade size (open+close both legs at taker)
    fee_round_trip = 4.0 * eff_usd * TAKER_FEE_BPS

    # ── 1. Hard cap from JSON ─────────────────────────────────────────────
    open_pairs = _count_open_pairs_from_json()
    if open_pairs >= eff_max:
        print(f"[ENTRY] MAX_OPEN_TRADES reached ({open_pairs}/{eff_max}).")
        return 0

    # ── 2. Optional collateral gate (best-effort) ─────────────────────────
    try:
        sub = await _get_subaccount(indexer)
        free_collateral = _sf(sub.get("freeCollateral"))
        # Min collateral scales with trade size: at least 3 trade legs' worth
        min_coll = max(float(USD_MIN_COLLATERAL), eff_usd * 3.0)
        if free_collateral and free_collateral < min_coll:
            print(f"[ENTRY] Free collateral ${free_collateral:,.2f} < min ${min_coll:,.2f}.")
            log_event({
                "type": "entry_blocked_collateral",
                "free_collateral": free_collateral,
                "min_collateral": min_coll,
            })
            return 0
    except Exception:
        pass

    # ── 3. Live markets with a position (avoid duplicating legs) ──────────
    live_markets = await _get_live_markets_with_position(indexer)

    # ── 4. Load cointegrated pairs CSV ────────────────────────────────────
    if not os.path.exists(COINT_CSV_PATH):
        print(f"[ENTRY] cointegrated_pairs.csv not found. Run FIND_COINTEGRATED=True first.")
        return 0
    df = pd.read_csv(COINT_CSV_PATH)

    # ── 5. Market metadata (one API call) ─────────────────────────────────
    markets_response = await indexer.markets.get_perpetual_markets()
    markets = markets_response.get("markets", {}) or {}

    # ── 6. Fetch funding rates upfront (one API call for all markets) ──────
    funding_rates = {}
    if FUNDING_GATE_ENABLED:
        try:
            funding_rates = await get_funding_rates(indexer, list(markets.keys()))
            log_event({
                "type": "funding_rates_fetched",
                "markets_count": len(funding_rates),
            }, print_terminal=False)
        except Exception as fe:
            log_event({"type": "funding_rates_fetch_error", "error": str(fe)})

    # ── 7. Load existing open pairs state ─────────────────────────────────
    bot_agents = _load_open_pairs_from_json()

    def get_safe_min_size(m_name):
        m_data = markets.get(m_name, {})
        val = m_data.get("minOrderSize") or m_data.get("stepSize")
        return float(val) if val else 0.0

    MAX_PRICE_RATIO = 50.0

    # ══════════════════════════════════════════════════════════════════════
    # PHASE 1: Collect and score all candidates
    # ══════════════════════════════════════════════════════════════════════
    candidates = []

    # Counters for scan diagnostics (printed at end of Phase 1)
    _skip_invalid       = 0
    _skip_live          = 0
    _skip_concentration = 0
    _skip_cooldown      = 0
    _skip_price_r       = 0
    _skip_candles       = 0
    _skip_low_z         = 0
    _skip_min_size      = 0
    _max_z_seen         = 0.0   # highest |z| seen (even if below threshold)
    _best_low_z_pair    = ""    # pair closest to threshold

    # Track how many candidates per market in this scan cycle
    # (prevents ARKM-USD appearing in 9 candidates when only 1 can be traded)
    _market_candidate_count: dict = {}

    for _, row in df.iterrows():
        base_market = row["base_market"]
        quote_market = row["quote_market"]
        hedge_ratio = float(row["hedge_ratio"])
        half_life = row.get("half_life")

        # ── Cheap filters (no API calls) ────────────────────────────────
        if base_market not in markets or quote_market not in markets:
            _skip_invalid += 1
            log_event({"type": "signal_skip", "reason": "invalid_market",
                       "base": base_market, "quote": quote_market}, print_terminal=False)
            continue
        if base_market in live_markets or quote_market in live_markets:
            _skip_live += 1
            continue

        # ── Market concentration limit ───────────────────────────────────
        # Si un mercado ya aparece MAX_TRADES_PER_MARKET veces como candidato
        # en este scan, saltamos pares adicionales con ese mercado.
        # Evita que ARKM-USD (presente en 9+ pares del CSV) bloquee todo
        # el capital cuando ya hay 1 candidato con ARKM.
        b_count = _market_candidate_count.get(base_market, 0)
        q_count = _market_candidate_count.get(quote_market, 0)
        if b_count >= MAX_TRADES_PER_MARKET or q_count >= MAX_TRADES_PER_MARKET:
            _skip_concentration += 1
            continue
        if _is_pair_in_fail_cooldown(base_market, quote_market):
            _skip_cooldown += 1
            log_event({"type": "signal_skip", "reason": "pair_fail_cooldown",
                       "base": base_market, "quote": quote_market}, print_terminal=False)
            continue

        try:
            base_price = float(markets[base_market]["oraclePrice"])
            quote_price = float(markets[quote_market]["oraclePrice"])
            min_base = get_safe_min_size(base_market)
            min_quote = get_safe_min_size(quote_market)
            base_step = markets[base_market]["stepSize"]
            quote_step = markets[quote_market]["stepSize"]
            tick_base = markets[base_market]["tickSize"]
        except Exception:
            _skip_invalid += 1
            continue

        price_ratio = max(base_price, quote_price) / max(1e-12, min(base_price, quote_price))
        if price_ratio > MAX_PRICE_RATIO:
            _skip_price_r += 1
            continue

        # ── Candle fetch + z-score (main API cost per candidate) ─────────
        series_1 = await get_candles_recent(indexer, base_market)
        series_2 = await get_candles_recent(indexer, quote_market)

        if not (len(series_1) > 0 and len(series_1) == len(series_2)):
            _skip_candles += 1
            continue

        spread = series_1 - (hedge_ratio * series_2)

        try:
            z_score = float(calculate_zscore(spread).values.tolist()[-1])
        except Exception:
            _skip_candles += 1
            continue

        if abs(z_score) > _max_z_seen:
            _max_z_seen = abs(z_score)
            _best_low_z_pair = f"{base_market}/{quote_market}"

        if abs(z_score) < ZSCORE_THRESH:
            _skip_low_z += 1
            continue

        spread_s = pd.Series(spread)
        if len(spread_s) < WINDOW + 1:
            _skip_candles += 1
            continue

        spread_last = float(spread_s.iloc[-1])
        spread_mean_prev_s = spread_s.rolling(WINDOW).mean().shift(1)
        spread_mean_prev = float(spread_mean_prev_s.iloc[-1])

        if not math.isfinite(spread_mean_prev):
            _skip_candles += 1
            continue

        spread_dev = abs(spread_last - spread_mean_prev)

        base_quantity = eff_usd / base_price
        quote_quantity = eff_usd / quote_price
        approx_spread_usd = spread_dev * min(base_quantity, quote_quantity)

        base_side = "BUY" if z_score < 0 else "SELL"
        quote_side = "BUY" if z_score > 0 else "SELL"

        base_size_fmt = format_number(base_quantity, base_step)
        quote_size_fmt = format_number(quote_quantity, quote_step)

        failsafe_p = base_price * (1.02 if base_side == "BUY" else 0.98)
        accept_failsafe_base_price = format_number(failsafe_p, tick_base)

        if float(base_size_fmt) < min_base or float(quote_size_fmt) < min_quote:
            _skip_min_size += 1
            log_event({"type": "signal_skip", "reason": "min_size",
                       "base": base_market, "quote": quote_market,
                       "base_size": float(base_size_fmt), "min_base": min_base},
                      print_terminal=False)
            continue

        # ── Spread bps for scoring (lightweight orderbook call) ───────────
        spread_bps = await get_market_spread_bps(indexer, base_market)
        if spread_bps is None:
            spread_bps = 100.0  # conservative default when unavailable

        # ── Opportunity score ─────────────────────────────────────────────
        # Higher = better:  big signal × fast reversion × tight spread
        hl_f = max(1.0, float(half_life or 20.0))
        score = abs(z_score) * (1.0 / hl_f) * (100.0 / max(1.0, spread_bps))

        candidates.append({
            "base_market": base_market,
            "quote_market": quote_market,
            "hedge_ratio": hedge_ratio,
            "half_life": half_life,
            "z_score": z_score,
            "spread_dev": spread_dev,
            "approx_spread_usd": approx_spread_usd,
            "base_side": base_side,
            "quote_side": quote_side,
            "base_price": base_price,
            "quote_price": quote_price,
            "base_size_fmt": base_size_fmt,
            "quote_size_fmt": quote_size_fmt,
            "accept_failsafe_base_price": accept_failsafe_base_price,
            "spread_bps": spread_bps,
            "score": score,
        })

        # Actualizar contadores de concentración por mercado
        _market_candidate_count[base_market]  = _market_candidate_count.get(base_market, 0) + 1
        _market_candidate_count[quote_market] = _market_candidate_count.get(quote_market, 0) + 1

    # ── Sort by score ──────────────────────────────────────────────────────
    if OPPORTUNITY_SCORING and candidates:
        candidates.sort(key=lambda c: c["score"], reverse=True)

    # ── Phase 1 summary (always printed to terminal) ───────────────────────
    _total_csv = len(df)
    _phase1_summary = (
        f"[ENTRY] Phase1: {_total_csv} CSV pairs → "
        f"invalid={_skip_invalid} live={_skip_live} concentration={_skip_concentration} "
        f"cooldown={_skip_cooldown} price_ratio={_skip_price_r} candles={_skip_candles} "
        f"low_z={_skip_low_z}(max|z|={_max_z_seen:.2f} @ {_best_low_z_pair}) "
        f"min_size={_skip_min_size} → candidates={len(candidates)}"
    )
    print(_phase1_summary, flush=True)

    log_event({
        "type": "entry_candidates_scored",
        "total_csv": _total_csv,
        "total_candidates": len(candidates),
        "skip_invalid": _skip_invalid,
        "skip_live": _skip_live,
        "skip_cooldown": _skip_cooldown,
        "skip_low_z": _skip_low_z,
        "skip_min_size": _skip_min_size,
        "max_z_seen": round(_max_z_seen, 3),
        "best_low_z_pair": _best_low_z_pair,
        "opportunity_scoring": OPPORTUNITY_SCORING,
        "fee_round_trip": round(fee_round_trip, 4),
        "eff_usd_per_trade": eff_usd,
        "top": [
            {
                "pair": f"{c['base_market']}/{c['quote_market']}",
                "score": round(c["score"], 4),
                "z": round(c["z_score"], 3),
                "hl": c["half_life"],
                "spread_bps": round(c["spread_bps"], 1),
                "approx_edge_usd": round(c["approx_spread_usd"], 2),
            }
            for c in candidates[:5]
        ],
    })

    # ══════════════════════════════════════════════════════════════════════
    # PHASE 2: Open top N candidates (with funding + min-edge gates)
    # ══════════════════════════════════════════════════════════════════════
    for cand in candidates:
        if opened_count >= max_new_trades:
            break
        if open_pairs >= eff_max:
            break

        base_market = cand["base_market"]
        quote_market = cand["quote_market"]
        z_score = cand["z_score"]
        half_life = cand["half_life"]
        base_side = cand["base_side"]
        quote_side = cand["quote_side"]
        approx_spread_usd = cand["approx_spread_usd"]

        # ── Gate A: Min edge ─────────────────────────────────────────────
        min_edge_required = MIN_EDGE_FEE_MULTIPLE * fee_round_trip
        if approx_spread_usd < min_edge_required:
            print(f"[ENTRY] SKIP {base_market}/{quote_market}: min_edge "
                  f"edge=${approx_spread_usd:.2f} < required=${min_edge_required:.2f} "
                  f"(fee_rt=${fee_round_trip:.2f}×{MIN_EDGE_FEE_MULTIPLE}x) "
                  f"z={z_score:.2f}", flush=True)
            log_event({
                "type": "signal_skip",
                "reason": "min_edge_gate",
                "base": base_market,
                "quote": quote_market,
                "approx_spread_usd": round(approx_spread_usd, 4),
                "min_edge_required": round(min_edge_required, 4),
                "fee_round_trip": round(fee_round_trip, 4),
                "score": round(cand["score"], 4),
            })
            continue

        # ── Gate B: Funding rate ─────────────────────────────────────────
        if FUNDING_GATE_ENABLED and funding_rates:
            fr_base = funding_rates.get(base_market, 0.0)
            fr_quote = funding_rates.get(quote_market, 0.0)

            # Expected hold ≈ one half_life. Funding periods = hold_hours / 8h.
            hold_hours = float(half_life or 8.0)
            funding_periods = max(1.0, hold_hours / 8.0)

            # Longs pay when rate > 0; shorts receive when rate > 0.
            base_funding = eff_usd * fr_base * funding_periods * (1.0 if base_side == "BUY" else -1.0)
            quote_funding = eff_usd * fr_quote * funding_periods * (1.0 if quote_side == "BUY" else -1.0)
            net_funding = base_funding + quote_funding  # positive = net cost to us

            log_event({
                "type": "funding_check",
                "base": base_market,
                "quote": quote_market,
                "fr_base": fr_base,
                "fr_quote": fr_quote,
                "base_funding_cost": round(base_funding, 4),
                "quote_funding_cost": round(quote_funding, 4),
                "net_funding_cost": round(net_funding, 4),
                "hold_hours": hold_hours,
                "expected_edge_usd": round(approx_spread_usd, 4),
                "ratio": round(net_funding / max(approx_spread_usd, 1e-9), 3),
            }, print_terminal=False)

            if net_funding > approx_spread_usd * FUNDING_MAX_COST_RATIO:
                print(f"[ENTRY] SKIP {base_market}/{quote_market}: funding_gate "
                      f"net_funding=${net_funding:.3f} > {FUNDING_MAX_COST_RATIO*100:.0f}% of "
                      f"edge=${approx_spread_usd:.2f} z={z_score:.2f}", flush=True)
                log_event({
                    "type": "signal_skip",
                    "reason": "funding_gate",
                    "base": base_market,
                    "quote": quote_market,
                    "net_funding_cost": round(net_funding, 4),
                    "expected_edge_usd": round(approx_spread_usd, 4),
                    "max_ratio": FUNDING_MAX_COST_RATIO,
                })
                continue

        # ── Open the trade ───────────────────────────────────────────────
        trace_id = uuid.uuid4().hex[:10]

        log_event({
            "type": "entry_signal",
            "trace_id": trace_id,
            "base": base_market,
            "quote": quote_market,
            "z": z_score,
            "score": round(cand["score"], 4),
            "spread_bps": round(cand["spread_bps"], 1),
            "spread_dev": round(cand["spread_dev"], 6),
            "approx_spread_usd": round(approx_spread_usd, 4),
            "min_edge_required": round(min_edge_required, 4),
            "base_side": base_side,
            "quote_side": quote_side,
            "base_price": cand["base_price"],
            "quote_price": cand["quote_price"],
            "base_size": float(cand["base_size_fmt"]),
            "quote_size": float(cand["quote_size_fmt"]),
            "half_life": half_life,
            "hedge_ratio": cand["hedge_ratio"],
            "usd_per_trade": eff_usd,
        })

        print(f"[ENTRY] Opening trade for {base_market} and {quote_market} "
              f"(score={cand['score']:.4f} z={z_score:.3f} hl={half_life})...")

        bot_agent = BotAgent(
            node,
            indexer,
            market_1=base_market,
            market_2=quote_market,
            base_side=base_side,
            base_size=cand["base_size_fmt"],
            base_price=cand["base_price"],
            quote_side=quote_side,
            quote_size=cand["quote_size_fmt"],
            quote_price=cand["quote_price"],
            accept_failsafe_base_price=cand["accept_failsafe_base_price"],
            z_score=z_score,
            half_life=half_life,
            hedge_ratio=cand["hedge_ratio"],
            trace_id=trace_id,
            intended_usd_per_trade=eff_usd,
        )

        bot_open_dict = await bot_agent.open_trades(wallet)

        # Always log result
        if isinstance(bot_open_dict, dict):
            log_event({
                "type": "entry_result",
                "trace_id": trace_id,
                "base": base_market,
                "quote": quote_market,
                "pair_status": bot_open_dict.get("pair_status"),
                "comments": bot_open_dict.get("comments"),
                "order_id_m1": bot_open_dict.get("order_id_m1"),
                "order_id_m2": bot_open_dict.get("order_id_m2"),
                "filled_usd_1": _sf(bot_open_dict.get("filled_usd_1")),
                "filled_usd_2": _sf(bot_open_dict.get("filled_usd_2")),
                "fee_1": _sf(bot_open_dict.get("fee_1")),
                "fee_2": _sf(bot_open_dict.get("fee_2")),
                "score": round(cand["score"], 4),
            })

        # Save LIVE
        if isinstance(bot_open_dict, dict) and bot_open_dict.get("pair_status") == "LIVE":
            now_iso = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            bot_open_dict["opened_at"] = bot_open_dict.get("opened_at") or now_iso
            bot_open_dict["side_1"] = bot_open_dict.get("side_1") or base_side
            bot_open_dict["side_2"] = bot_open_dict.get("side_2") or quote_side
            bot_open_dict["price_1"] = bot_open_dict.get("price_1") or cand["base_price"]
            bot_open_dict["price_2"] = bot_open_dict.get("price_2") or cand["quote_price"]
            bot_open_dict["size_1"] = bot_open_dict.get("size_1") or _sf(bot_open_dict.get("filled_size_1"), _sf(cand["base_size_fmt"]))
            bot_open_dict["size_2"] = bot_open_dict.get("size_2") or _sf(bot_open_dict.get("filled_size_2"), _sf(cand["quote_size_fmt"]))
            bot_open_dict["entry_score"] = round(cand["score"], 4)  # for future analysis

            bot_agents.append(bot_open_dict)
            with open(JSON_PATH, "w") as f:
                json.dump(bot_agents, f, indent=2)

            opened_count += 1
            open_pairs += 1
            live_markets.add(base_market)
            live_markets.add(quote_market)

            # Reset failure counter for this pair
            _record_pair_success(base_market, quote_market)

            print(f"[ENTRY] Trade LIVE: {base_market} {base_side} / {quote_market} {quote_side}")

            filled_usd_1 = _sf(bot_open_dict.get("filled_usd_1"))
            filled_usd_2 = _sf(bot_open_dict.get("filled_usd_2"))
            fee_1 = _sf(bot_open_dict.get("fee_1"))
            fee_2 = _sf(bot_open_dict.get("fee_2"))
            fee_estimated = bot_open_dict.get("fee_1_estimated") or bot_open_dict.get("fee_2_estimated")
            total_fees = fee_1 + fee_2
            residual = _sf(bot_open_dict.get("residual_usd"))
            z = _sf(bot_open_dict.get("z_score"))
            hl = bot_open_dict.get("half_life")
            notional = filled_usd_1 + filled_usd_2
            fee_label = "~est" if fee_estimated else "real"

            send_message(
                f"TRADE LIVE\n"
                f"{base_market} {base_side} / {quote_market} {quote_side}\n"
                f"Notional: ${notional:,.2f} (m1=${filled_usd_1:,.2f}, m2=${filled_usd_2:,.2f})\n"
                f"Fees ({fee_label}): ${total_fees:.4f}\n"
                f"Residual: ${residual:.2f}\n"
                f"Z-score: {z:.3f} | Half-life: {hl}h | Score: {cand['score']:.4f}"
            )
            continue

        # SKIPPED (spread gate, PREPARE fail before COMMIT, etc.) — no exposure created
        if isinstance(bot_open_dict, dict) and bot_open_dict.get("pair_status") == "SKIPPED":
            skip_reason = bot_open_dict.get("comments", "")
            log_event({
                "type": "entry_skipped",
                "trace_id": trace_id,
                "base": base_market,
                "quote": quote_market,
                "reason": skip_reason,
            })
            continue

        # Not LIVE: check for orphan exposure, gated on committed flag.
        # Only when MARKET orders were actually sent — otherwise we'd log false
        # orphans from pre-existing exposure in other sessions.
        committed = isinstance(bot_open_dict, dict) and bot_open_dict.get("committed", False)
        try:
            live_now = await _get_live_markets_with_position(indexer) if committed else set()
            orphan = committed and ((base_market in live_now) or (quote_market in live_now))
        except Exception:
            orphan = False

        if orphan and isinstance(bot_open_dict, dict):
            bot_open_dict["pair_status"] = "ORPHAN"
            bot_open_dict["needs_reconcile"] = True
            bot_open_dict["opened_at"] = bot_open_dict.get("opened_at") or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            bot_agents.append(bot_open_dict)
            with open(JSON_PATH, "w") as f:
                json.dump(bot_agents, f, indent=2)
            send_message(f"⚠️ ORPHAN: {base_market}/{quote_market}. Guardado para cleanup.")
            log_event({
                "type": "orphan_saved",
                "trace_id": trace_id,
                "base": base_market,
                "quote": quote_market,
            })
            fail_count = _record_pair_fail(base_market, quote_market)
            if fail_count >= PAIR_FAIL_COOLDOWN_THRESHOLD:
                log_event({"type": "pair_fail_cooldown_set", "base": base_market,
                           "quote": quote_market, "consecutive_fails": fail_count,
                           "cooldown_hours": PAIR_FAIL_COOLDOWN_HOURS})
        else:
            reason = bot_open_dict.get("comments", "Unknown") if isinstance(bot_open_dict, dict) else "Unknown"
            print(f"[ENTRY] Trade FAILED for {base_market}/{quote_market}. Reason: {reason}")

            if isinstance(bot_open_dict, dict):
                f1 = _sf(bot_open_dict.get("filled_usd_1"))
                f2 = _sf(bot_open_dict.get("filled_usd_2"))
                one_leg_filled = (f1 > 0) != (f2 > 0)
                if one_leg_filled:
                    fail_count = _record_pair_fail(base_market, quote_market)
                    if fail_count >= PAIR_FAIL_COOLDOWN_THRESHOLD:
                        log_event({"type": "pair_fail_cooldown_set", "base": base_market,
                                   "quote": quote_market, "consecutive_fails": fail_count,
                                   "cooldown_hours": PAIR_FAIL_COOLDOWN_HOURS})

    print("[ENTRY] Entry scan completed.")
    if opened_count > 0:
        print(f"[ENTRY] {opened_count} new pairs opened this batch.")

    return opened_count
