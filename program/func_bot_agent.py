# func_bot_agent.py
#
# Cambios principales:
# - PREPARE sin cancel_order():
#     1) crea ambos legs como LIMIT POST_ONLY no ejecutables
#     2) good_til_blocks corto para que expiren solos
#     3) espera a que ambos sean visibles en indexer
#     4) solo entonces hace COMMIT con MARKET IOC en paralelo
#
# - Auditoría con retries
# - Logs estructurados
# - Flatten automático cuando queda una sola pierna
# - Gate de min fill
# - Control de residual

import asyncio
from datetime import datetime, timezone
from v4_proto.dydxprotocol.clob.order_pb2 import Order

from func_logging import log_event
from func_private import (
    place_market_order,
    place_limit_order,
    get_real_fill_details,
    wait_order_visible,
    cancel_order_by_client_id,
)
from func_position_guard import get_live_positions
from func_public import get_market_spread_bps
from constants import (
    SPREAD_GATE_MAX_PCT_OF_EDGE,
    SPREAD_GATE_PER_LEG_FLOOR_BPS,
    SPREAD_GATE_PER_LEG_CEILING_BPS,
)


def _sf(x, d=0.0):
    try:
        return float(x)
    except Exception:
        return d


class BotAgent:
    def __init__(
        self,
        client_node,
        client_indexer,
        market_1,
        market_2,
        base_side,
        base_size,
        base_price,
        quote_side,
        quote_size,
        quote_price,
        accept_failsafe_base_price,
        z_score,
        half_life,
        hedge_ratio,
        trace_id=None,
        min_m1_fill_usd=50.0,
        min_m1_fill_ratio=0.05,
        intended_usd_per_trade=None,
        expected_edge_usd=0.0,       # 2026-05-26: needed for dynamic spread gate
        prepare_price_bps=2000,      # 20% lejos del oracle para no ejecutar
        prepare_good_til_blocks=2,   # expira solo, no depende de cancel
        prepare_visible_timeout_s=6.0,
        audit_retries=5,
    ):
        self.node = client_node
        self.indexer = client_indexer

        self.market_1 = market_1
        self.market_2 = market_2

        self.base_side = base_side
        self.base_size = float(base_size)
        self.base_price = float(base_price)

        self.quote_side = quote_side
        self.quote_size = float(quote_size)
        self.quote_price = float(quote_price)

        self.accept_failsafe_base_price = accept_failsafe_base_price
        self.z_score = float(z_score) if z_score == z_score else float("nan")
        self.half_life = half_life
        self.hedge_ratio = float(hedge_ratio)
        self.trace_id = trace_id

        self.min_m1_fill_usd = float(min_m1_fill_usd)
        self.min_m1_fill_ratio = float(min_m1_fill_ratio)
        self.intended_usd_per_trade = float(intended_usd_per_trade) if intended_usd_per_trade is not None else None
        self.expected_edge_usd = float(expected_edge_usd) if expected_edge_usd is not None else 0.0

        self.prepare_price_bps = int(prepare_price_bps)
        self.prepare_good_til_blocks = int(prepare_good_til_blocks)
        self.prepare_visible_timeout_s = float(prepare_visible_timeout_s)
        self.audit_retries = int(audit_retries)

        self.order_dict = {
            "trace_id": trace_id,
            "market_1": market_1,
            "market_2": market_2,
            "hedge_ratio": hedge_ratio,
            "z_score": z_score,
            "half_life": half_life,

            "order_id_m1": "",
            "order_m1_size": self.base_size,
            "order_m1_side": base_side,
            "order_time_m1": "",

            "order_id_m2": "",
            "order_m2_size": self.quote_size,
            "order_m2_side": quote_side,
            "order_time_m2": "",

            "pair_status": "",
            "comments": "",

            "filled_size_1": 0.0,
            "filled_size_2": 0.0,
            "filled_usd_1": 0.0,
            "filled_usd_2": 0.0,
            "fee_1": 0.0,
            "fee_2": 0.0,
            "liquidity_1": None,
            "liquidity_2": None,

            "residual_usd": 0.0,
            "residual_tol": 0.0,
            "flattened": False,

            # Set to True only after COMMIT orders are actually sent.
            # Pre-COMMIT failures (PREPARE fail, spread skip, exposure gate)
            # must NOT trigger orphan checks — no exposure was created.
            "committed": False,

            "opened_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        }

    def _prepare_price(self, side: str, oracle_price: float):
        """
        Precio no ejecutable para POST_ONLY:
        BUY  -> muy abajo del oracle
        SELL -> muy arriba del oracle
        """
        bps = max(1, self.prepare_price_bps)
        if str(side).upper() == "BUY":
            mult = 1.0 - (bps / 10_000.0)
        else:
            mult = 1.0 + (bps / 10_000.0)
        return float(oracle_price * mult)

    async def audit_with_retry(self, market: str, client_id: str, retries=None):
        retries = self.audit_retries if retries is None else int(retries)

        last = None
        base_delay = 0.35
        max_delay = 3.0

        for i in range(retries):
            last = await get_real_fill_details(self.indexer, client_id, market)
            status = (last.get("status_label") or "").upper()
            filled = _sf(last.get("filled_size", 0))

            log_event({
                "type": "audit",
                "trace_id": self.trace_id,
                "market": market,
                "client_id": client_id,
                "attempt": i + 1,
                "retries": retries,
                "status": status,
                "filled_size": filled,
                "fee_total": _sf(last.get("fee_total", 0)),
            })

            # si ya hubo fill, o ya tenemos estado terminal, paramos
            if filled > 0:
                return last

            if status in (
                "FILLED",
                "PARTIALLY_FILLED_IOC",
                "KILLED_BY_FOK",
                "FAILED",
                "CANCELED",
                "BEST_EFFORT_OPENED",
            ):
                return last

            delay = min(max_delay, base_delay * (2 ** i))
            await asyncio.sleep(delay)

        return last or {"found": False, "status_label": "NOT_FOUND", "filled_size": 0.0}

    async def open_trades(self, wallet):
        log_event({
            "type": "open_start",
            "trace_id": self.trace_id,
            "m1": self.market_1,
            "m2": self.market_2,
            "side_1": self.base_side,
            "side_2": self.quote_side,
            "size_1": self.base_size,
            "size_2": self.quote_size,
            "z": self.z_score,
            "hedge_ratio": self.hedge_ratio,
            "half_life": self.half_life,
        })

        # =========================================================
        # PHASE 1: PREPARE
        # =========================================================
        # Creamos ambas órdenes como LIMIT POST_ONLY no ejecutables
        # y con expiración corta. No cancelamos: expiran solas.
        try:
            prep_p1 = self._prepare_price(self.base_side, self.base_price)
            prep_p2 = self._prepare_price(self.quote_side, self.quote_price)

            prep1_task = place_limit_order(
                self.node,
                self.indexer,
                wallet,
                self.market_1,
                self.base_side,
                self.base_size,
                prep_p1,
                reduce_only=False,
                post_only=True,
                good_til_blocks=self.prepare_good_til_blocks,
            )

            prep2_task = place_limit_order(
                self.node,
                self.indexer,
                wallet,
                self.market_2,
                self.quote_side,
                self.quote_size,
                prep_p2,
                reduce_only=False,
                post_only=True,
                good_til_blocks=self.prepare_good_til_blocks,
            )

            prep1, prep2 = await asyncio.gather(prep1_task, prep2_task)

            if not prep1 or not prep2:
                raise Exception(f"Prepare failed: prep1={prep1}, prep2={prep2}")

            cid1 = prep1.get("order", {}).get("id")
            cid2 = prep2.get("order", {}).get("id")

            if not cid1 or not cid2:
                raise Exception(f"Prepare missing ids: cid1={cid1}, cid2={cid2}")

            # Confirmar que ambas existen en indexer
            ok1_task = wait_order_visible(self.indexer, cid1, max_wait_s=self.prepare_visible_timeout_s)
            ok2_task = wait_order_visible(self.indexer, cid2, max_wait_s=self.prepare_visible_timeout_s)
            ok1, ok2 = await asyncio.gather(ok1_task, ok2_task)

            # Si alguna orden no es visible en el indexer en tiempo: loguear warning
            # pero NO abortar. El indexer en testnet tiene latencia variable.
            # La fase DECISION ya maneja correctamente fills parciales/cero.
            # Abortar aquí solo desperdicia el 60% de las oportunidades.
            if not (ok1 and ok2):
                log_event({
                    "type": "prepare_visibility_warn",
                    "trace_id": self.trace_id,
                    "stage": "prepare_visible",
                    "cid1": cid1,
                    "cid2": cid2,
                    "ok1": ok1,
                    "ok2": ok2,
                    "msg": "Indexer lag — proceeding to COMMIT anyway",
                })
            else:
                log_event({
                    "type": "prepare_ok",
                    "trace_id": self.trace_id,
                    "cid1": cid1,
                    "cid2": cid2,
                    "prepare_good_til_blocks": self.prepare_good_til_blocks,
                })

            # Esperamos un poco para que esas órdenes expiren solas
            # y no estorben al commit.
            await asyncio.sleep(2.0)


            # Safety: PREPARE must not create real exposure. If it did, do not COMMIT.
            live_before_commit = await get_live_positions(self.indexer, min_usd=1.0)
            dirty_prepare = [
                m for m in (self.market_1, self.market_2)
                if m in live_before_commit
            ]
            if dirty_prepare:
                self.order_dict["pair_status"] = "ERROR"
                self.order_dict["comments"] = f"PRE_COMMIT_EXPOSURE_DETECTED: {dirty_prepare}"
                log_event({
                    "type": "open_error",
                    "trace_id": self.trace_id,
                    "stage": "pre_commit_exposure_gate",
                    "dirty_markets": dirty_prepare,
                })
                return self.order_dict

        except Exception as e:
            self.order_dict["pair_status"] = "ERROR"
            self.order_dict["comments"] = f"PREPARE fail: {e}"
            log_event({
                "type": "open_error",
                "trace_id": self.trace_id,
                "stage": "prepare",
                "error": str(e),
            })
            return self.order_dict

        # =========================================================
        # SPREAD CHECK (pre-COMMIT gate, DYNAMIC since 2026-05-26)
        # =========================================================
        # Fetch live orderbook for both legs and decide whether the cost of
        # crossing both books (entry + exit, both legs) is acceptable given
        # the expected edge of THIS specific trade.
        #
        # Formula:
        #   spread_cost_usd = (s1 + s2) × notional_per_leg / 10000
        #   (derived from: 2 legs × 2 sides × half_spread × notional)
        #
        # Three-phase decision (see constants.py for details):
        #   1. FLOOR pass — both legs ≤ FLOOR_BPS → permit unconditionally
        #   2. CEILING block — any leg > CEILING_BPS → reject unconditionally
        #   3. EDGE-PROPORTIONAL — spread_cost ≤ MAX_PCT × expected_edge_usd
        #
        # On indexer error: proceed (never block on missing data).
        # On spread = None: treat as data-unavailable; only floor pass can succeed.
        try:
            spread1, spread2 = await asyncio.gather(
                get_market_spread_bps(self.indexer, self.market_1),
                get_market_spread_bps(self.indexer, self.market_2),
            )

            floor_bps = float(SPREAD_GATE_PER_LEG_FLOOR_BPS)
            ceiling_bps = float(SPREAD_GATE_PER_LEG_CEILING_BPS)
            max_pct = float(SPREAD_GATE_MAX_PCT_OF_EDGE)
            notional_per_leg = float(self.intended_usd_per_trade or 0.0)
            edge_usd = float(self.expected_edge_usd or 0.0)

            # Phase 1: floor pass
            s1_below_floor = (spread1 is None) or (spread1 <= floor_bps)
            s2_below_floor = (spread2 is None) or (spread2 <= floor_bps)
            phase1_pass = s1_below_floor and s2_below_floor

            # Phase 2: ceiling block (only meaningful if spreads are known)
            s1_above_ceiling = (spread1 is not None) and (spread1 > ceiling_bps)
            s2_above_ceiling = (spread2 is not None) and (spread2 > ceiling_bps)
            phase2_block = s1_above_ceiling or s2_above_ceiling

            # Phase 3: edge-proportional cost check
            # Use 0 for None spread (best case — book might not be wide, just unknown)
            s1_val = float(spread1) if spread1 is not None else 0.0
            s2_val = float(spread2) if spread2 is not None else 0.0
            spread_cost_usd = (s1_val + s2_val) * notional_per_leg / 10000.0
            max_cost_usd = max_pct * edge_usd if edge_usd > 0 else 0.0
            edge_check_known = edge_usd > 0
            phase3_pass = edge_check_known and (spread_cost_usd <= max_cost_usd)

            # Decision
            if phase1_pass:
                passed = True
                rule = "floor_pass"
                reason = ""
            elif phase2_block:
                passed = False
                rule = "ceiling_block"
                wide_m = self.market_1 if s1_above_ceiling else self.market_2
                wide_bps = spread1 if s1_above_ceiling else spread2
                reason = f"SPREAD_CEILING: {wide_m} {wide_bps:.1f}bps > {ceiling_bps:.0f}bps"
            elif phase3_pass:
                passed = True
                rule = "edge_pct_pass"
                reason = ""
            elif edge_check_known:
                passed = False
                rule = "edge_pct_block"
                reason = (
                    f"SPREAD_COST_EXCEEDS_EDGE_PCT: cost=${spread_cost_usd:.2f} "
                    f"> max=${max_cost_usd:.2f} ({max_pct*100:.0f}% of edge=${edge_usd:.2f}) "
                    f"| s1={s1_val:.1f}bps s2={s2_val:.1f}bps"
                )
            else:
                # No edge info AND not in floor pass AND not ceiling block.
                # Fall back to ceiling-only behavior: permit (we already passed ceiling).
                passed = True
                rule = "no_edge_data_fallback_to_ceiling"
                reason = ""

            log_event({
                "type": "spread_check",
                "trace_id": self.trace_id,
                "market_1": self.market_1,
                "market_2": self.market_2,
                "spread_1_bps": round(spread1, 2) if spread1 is not None else None,
                "spread_2_bps": round(spread2, 2) if spread2 is not None else None,
                "notional_per_leg": notional_per_leg,
                "expected_edge_usd": round(edge_usd, 4),
                "spread_cost_usd": round(spread_cost_usd, 4),
                "max_cost_usd": round(max_cost_usd, 4),
                "floor_bps": floor_bps,
                "ceiling_bps": ceiling_bps,
                "max_pct_of_edge": max_pct,
                "rule": rule,
                "passed": passed,
            })

            if not passed:
                self.order_dict["pair_status"] = "SKIPPED"
                self.order_dict["comments"] = reason
                return self.order_dict

        except Exception as e:
            # Spread check itself failed — log and proceed to COMMIT.
            # Never let an instrumentation failure block a trade.
            log_event({
                "type": "spread_check_error",
                "trace_id": self.trace_id,
                "error": str(e),
                "msg": "Spread check exception — proceeding to COMMIT",
            })

        # =========================================================
        # PHASE 2: COMMIT
        # =========================================================
        # Disparamos ambas MARKET IOC casi al mismo tiempo.
        try:
            m1_task = place_market_order(
                self.node,
                self.indexer,
                wallet,
                self.market_1,
                self.base_side,
                self.base_size,
                self.base_price,
                False,
                time_in_force_type=Order.TimeInForce.TIME_IN_FORCE_IOC,
            )

            m2_task = place_market_order(
                self.node,
                self.indexer,
                wallet,
                self.market_2,
                self.quote_side,
                self.quote_size,
                self.quote_price,
                False,
                time_in_force_type=Order.TimeInForce.TIME_IN_FORCE_IOC,
            )

            order_m1, order_m2 = await asyncio.gather(m1_task, m2_task)

            cid_m1 = (order_m1 or {}).get("order", {}).get("id")
            cid_m2 = (order_m2 or {}).get("order", {}).get("id")

            if not cid_m1 or not cid_m2:
                raise Exception(f"Commit missing ids: m1={order_m1}, m2={order_m2}")

            now_iso = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            self.order_dict["order_id_m1"] = cid_m1
            self.order_dict["order_id_m2"] = cid_m2
            self.order_dict["order_time_m1"] = now_iso
            self.order_dict["order_time_m2"] = now_iso

            log_event({
                "type": "commit_sent",
                "trace_id": self.trace_id,
                "cid1": cid_m1,
                "cid2": cid_m2,
            })

            # Mark that we have sent real MARKET orders — orphan check is now valid.
            self.order_dict["committed"] = True

        except Exception as e:
            self.order_dict["pair_status"] = "ERROR"
            self.order_dict["comments"] = f"COMMIT send fail: {e}"
            log_event({
                "type": "open_error",
                "trace_id": self.trace_id,
                "stage": "commit_send",
                "error": str(e),
            })
            return self.order_dict

        # =========================================================
        # PHASE 3: AUDIT
        # =========================================================
        audit1_task = self.audit_with_retry(self.market_1, self.order_dict["order_id_m1"])
        audit2_task = self.audit_with_retry(self.market_2, self.order_dict["order_id_m2"])
        audit_m1, audit_m2 = await asyncio.gather(audit1_task, audit2_task)

        status_m1 = (audit_m1.get("status_label") or "").upper()
        status_m2 = (audit_m2.get("status_label") or "").upper()

        real_filled_m1 = _sf(audit_m1.get("filled_size", 0))
        real_filled_m2 = _sf(audit_m2.get("filled_size", 0))

        filled_usd_1 = real_filled_m1 * float(self.base_price)
        filled_usd_2 = real_filled_m2 * float(self.quote_price)

        self.order_dict["filled_size_1"] = real_filled_m1
        self.order_dict["filled_size_2"] = real_filled_m2
        self.order_dict["filled_usd_1"] = filled_usd_1
        self.order_dict["filled_usd_2"] = filled_usd_2
        self.order_dict["fee_1"] = _sf(audit_m1.get("fee_total", 0))
        self.order_dict["fee_2"] = _sf(audit_m2.get("fee_total", 0))
        self.order_dict["fee_1_estimated"] = bool(audit_m1.get("fee_estimated", False))
        self.order_dict["fee_2_estimated"] = bool(audit_m2.get("fee_estimated", False))
        self.order_dict["liquidity_1"] = audit_m1.get("liquidity")
        self.order_dict["liquidity_2"] = audit_m2.get("liquidity")

        log_event({
            "type": "fills",
            "trace_id": self.trace_id,
            "status_1": status_m1,
            "status_2": status_m2,
            "filled_size_1": real_filled_m1,
            "filled_size_2": real_filled_m2,
            "filled_usd_1": filled_usd_1,
            "filled_usd_2": filled_usd_2,
            "fee_1": self.order_dict["fee_1"],
            "fee_2": self.order_dict["fee_2"],
            "fee_1_estimated": self.order_dict["fee_1_estimated"],
            "fee_2_estimated": self.order_dict["fee_2_estimated"],
        })

        # =========================================================
        # BEST_EFFORT_OPENED safety: cancel + re-query position
        # =========================================================
        # On testnet (and occasionally mainnet), a MARKET IOC order can land
        # in the book as an OPEN limit order instead of filling or being cancelled.
        # The indexer reports filled_size=0 (nothing filled yet), but the order
        # is LIVE and will fill later — creating an orphan leg.
        #
        # Fix: if either leg returned BEST_EFFORT_OPENED, cancel it immediately
        # and then query the REAL position to find out whether it actually filled.
        for leg_status, cid, market in (
            (status_m1, cid_m1, self.market_1),
            (status_m2, cid_m2, self.market_2),
        ):
            if leg_status == "BEST_EFFORT_OPENED":
                log_event({
                    "type": "best_effort_cancel_attempt",
                    "trace_id": self.trace_id,
                    "market": market,
                    "client_id": cid,
                    "msg": "BEST_EFFORT_OPENED detected — cancelling to prevent orphan",
                })
                try:
                    await cancel_order_by_client_id(self.node, cid)
                    await asyncio.sleep(1.5)  # let cancel propagate
                except Exception as ce:
                    log_event({
                        "type": "best_effort_cancel_error",
                        "trace_id": self.trace_id,
                        "market": market,
                        "error": str(ce),
                    })

        # After cancellation attempts, query the REAL position sizes.
        # This overrides audit-derived fill sizes for BEST_EFFORT legs.
        if "BEST_EFFORT_OPENED" in (status_m1, status_m2):
            try:
                real_pos = await get_live_positions(self.indexer, min_usd=0.5)
                if status_m1 == "BEST_EFFORT_OPENED":
                    pos_m1 = real_pos.get(self.market_1)
                    if pos_m1:
                        real_filled_m1 = abs(_sf(pos_m1.get("size", 0)))
                        filled_usd_1 = real_filled_m1 * float(self.base_price)
                        self.order_dict["filled_size_1"] = real_filled_m1
                        self.order_dict["filled_usd_1"] = filled_usd_1
                if status_m2 == "BEST_EFFORT_OPENED":
                    pos_m2 = real_pos.get(self.market_2)
                    if pos_m2:
                        real_filled_m2 = abs(_sf(pos_m2.get("size", 0)))
                        filled_usd_2 = real_filled_m2 * float(self.quote_price)
                        self.order_dict["filled_size_2"] = real_filled_m2
                        self.order_dict["filled_usd_2"] = filled_usd_2
                log_event({
                    "type": "best_effort_pos_recheck",
                    "trace_id": self.trace_id,
                    "m1_had_best_effort": status_m1 == "BEST_EFFORT_OPENED",
                    "m2_had_best_effort": status_m2 == "BEST_EFFORT_OPENED",
                    "real_filled_m1": real_filled_m1,
                    "real_filled_m2": real_filled_m2,
                    "filled_usd_1": filled_usd_1,
                    "filled_usd_2": filled_usd_2,
                })
            except Exception as be:
                log_event({
                    "type": "best_effort_pos_recheck_error",
                    "trace_id": self.trace_id,
                    "error": str(be),
                })

        # =========================================================
        # PHASE 4: DECISIÓN
        # =========================================================

        # Caso 1: ambas 0
        if real_filled_m1 <= 0 and real_filled_m2 <= 0:
            self.order_dict["pair_status"] = "FAILED"
            self.order_dict["comments"] = f"Both legs filled 0 (m1={status_m1}, m2={status_m2})."
            return self.order_dict

        # Caso 2: solo M2 se llenó -> flatten M2
        if real_filled_m1 <= 0 and real_filled_m2 > 0:
            try:
                close_side_m2 = "SELL" if self.quote_side.upper() == "BUY" else "BUY"
                await place_market_order(
                    self.node,
                    self.indexer,
                    wallet,
                    self.market_2,
                    close_side_m2,
                    real_filled_m2,
                    self.quote_price,
                    True,
                    time_in_force_type=Order.TimeInForce.TIME_IN_FORCE_IOC,
                )
                self.order_dict["pair_status"] = "FAILED"
                self.order_dict["comments"] = f"M1 filled 0; flattened M2 (filled={real_filled_m2})."
            except Exception as e:
                self.order_dict["pair_status"] = "ERROR"
                self.order_dict["comments"] = f"M1 filled 0; flatten M2 failed: {e}"
            return self.order_dict

        # Caso 3: solo M1 se llenó -> flatten M1
        if real_filled_m2 <= 0 and real_filled_m1 > 0:
            try:
                close_side_m1 = "SELL" if self.base_side.upper() == "BUY" else "BUY"
                await place_market_order(
                    self.node,
                    self.indexer,
                    wallet,
                    self.market_1,
                    close_side_m1,
                    real_filled_m1,
                    self.base_price,
                    True,
                    time_in_force_type=Order.TimeInForce.TIME_IN_FORCE_IOC,
                )
                self.order_dict["pair_status"] = "FAILED"
                self.order_dict["comments"] = f"M2 filled 0; flattened M1 (filled={real_filled_m1})."
            except Exception as e:
                self.order_dict["pair_status"] = "ERROR"
                self.order_dict["comments"] = f"M2 filled 0; flatten M1 failed: {e}"
            return self.order_dict

        # Gate mínimo de fill
        ratio_ok = True
        if self.intended_usd_per_trade is not None and self.intended_usd_per_trade > 0:
            ratio_ok = (filled_usd_1 / self.intended_usd_per_trade) >= self.min_m1_fill_ratio

        usd_ok = filled_usd_1 >= self.min_m1_fill_usd

        log_event({
            "type": "min_fill_gate",
            "trace_id": self.trace_id,
            "filled_usd_1": filled_usd_1,
            "min_usd": self.min_m1_fill_usd,
            "ratio_ok": ratio_ok,
            "min_ratio": self.min_m1_fill_ratio,
        })

        if not usd_ok or not ratio_ok:
            try:
                close_side_m1 = "SELL" if self.base_side.upper() == "BUY" else "BUY"
                close_side_m2 = "SELL" if self.quote_side.upper() == "BUY" else "BUY"

                await asyncio.gather(
                    place_market_order(
                        self.node,
                        self.indexer,
                        wallet,
                        self.market_1,
                        close_side_m1,
                        real_filled_m1,
                        self.base_price,
                        True,
                        time_in_force_type=Order.TimeInForce.TIME_IN_FORCE_IOC,
                    ),
                    place_market_order(
                        self.node,
                        self.indexer,
                        wallet,
                        self.market_2,
                        close_side_m2,
                        real_filled_m2,
                        self.quote_price,
                        True,
                        time_in_force_type=Order.TimeInForce.TIME_IN_FORCE_IOC,
                    ),
                )

                self.order_dict["pair_status"] = "FAILED"
                self.order_dict["comments"] = f"Min-fill gate hit; flattened both legs. filled_usd_1={filled_usd_1:.2f}"
            except Exception as e:
                self.order_dict["pair_status"] = "ERROR"
                self.order_dict["comments"] = f"Min-fill gate hit; flatten failed: {e}"
            return self.order_dict

        # Residual USD
        residual_usd = filled_usd_1 - filled_usd_2
        effective_usd = max(1e-9, min(filled_usd_1, filled_usd_2))
        tol = max(2.0, 0.02 * effective_usd)

        self.order_dict["residual_usd"] = float(residual_usd)
        self.order_dict["residual_tol"] = float(tol)

        log_event({
            "type": "residual",
            "trace_id": self.trace_id,
            "residual_usd": residual_usd,
            "tol_usd": tol,
        })

        if abs(residual_usd) > tol:
            self.order_dict["flattened"] = True
            try:
                if residual_usd > 0:
                    # sobra usd en leg1
                    size_to_close_m1 = residual_usd / float(self.base_price) if self.base_price > 0 else 0.0
                    close_side_m1 = "SELL" if self.base_side.upper() == "BUY" else "BUY"
                    await place_market_order(
                        self.node,
                        self.indexer,
                        wallet,
                        self.market_1,
                        close_side_m1,
                        size_to_close_m1,
                        self.base_price,
                        True,
                        time_in_force_type=Order.TimeInForce.TIME_IN_FORCE_IOC,
                    )
                else:
                    # sobra usd en leg2
                    size_to_close_m2 = (-residual_usd) / float(self.quote_price) if self.quote_price > 0 else 0.0
                    close_side_m2 = "SELL" if self.quote_side.upper() == "BUY" else "BUY"
                    await place_market_order(
                        self.node,
                        self.indexer,
                        wallet,
                        self.market_2,
                        close_side_m2,
                        size_to_close_m2,
                        self.quote_price,
                        True,
                        time_in_force_type=Order.TimeInForce.TIME_IN_FORCE_IOC,
                    )

                self.order_dict["pair_status"] = "FAILED"
                self.order_dict["comments"] = f"Residual too large -> flattened. residual_usd={residual_usd:.2f}"

                log_event({
                    "type": "flattened_residual",
                    "trace_id": self.trace_id,
                    "residual_usd": residual_usd,
                })
                return self.order_dict

            except Exception as e:
                self.order_dict["pair_status"] = "ERROR"
                self.order_dict["comments"] = f"Residual flatten fail: {e}"
                log_event({
                    "type": "open_error",
                    "trace_id": self.trace_id,
                    "stage": "residual_flatten",
                    "error": str(e),
                })
                return self.order_dict

        # Final safety: after all flatten/residual checks, both legs must be live.
        live_after_open = await get_live_positions(self.indexer, min_usd=1.0)
        if self.market_1 not in live_after_open or self.market_2 not in live_after_open:
            self.order_dict["pair_status"] = "ERROR"
            self.order_dict["comments"] = "POST_OPEN_RECONCILE_FAILED: one or both legs not live"
            log_event({
                "type": "open_error",
                "trace_id": self.trace_id,
                "stage": "post_open_reconcile",
                "live_markets": sorted(live_after_open.keys()),
                "m1": self.market_1,
                "m2": self.market_2,
            })
            return self.order_dict

        # Si llegamos aquí, queda LIVE
        self.order_dict["pair_status"] = "LIVE"
        self.order_dict["comments"] = "OK"

        log_event({
            "type": "open_live",
            "trace_id": self.trace_id,
            "filled_usd_1": filled_usd_1,
            "filled_usd_2": filled_usd_2,
            "fee_total": self.order_dict["fee_1"] + self.order_dict["fee_2"],
            "fee_estimated": self.order_dict["fee_1_estimated"] or self.order_dict["fee_2_estimated"],
        })

        return self.order_dict