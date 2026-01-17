from func_private import place_market_order, check_order_status, get_real_fill_details
from datetime import datetime
import asyncio
from v4_proto.dydxprotocol.clob.order_pb2 import Order


class BotAgent:
    def __init__(self, client_node, client_indexer, market_1, market_2, base_side, base_size, base_price, quote_side,
                 quote_size, quote_price, accept_failsafe_base_price, z_score, half_life, hedge_ratio):
        self.node = client_node
        self.indexer = client_indexer
        self.market_1 = market_1
        self.market_2 = market_2
        self.base_side = base_side
        self.base_size = base_size
        self.base_price = base_price
        self.quote_side = quote_side
        self.quote_size = quote_size
        self.quote_price = quote_price
        self.accept_failsafe_base_price = accept_failsafe_base_price
        self.z_score = z_score
        self.half_life = half_life
        self.hedge_ratio = hedge_ratio

        self.order_dict = {
            "market_1": market_1,
            "market_2": market_2,
            "hedge_ratio": hedge_ratio,
            "z_score": z_score,
            "half_life": half_life,
            "order_id_m1": "",
            "order_m1_size": base_size,
            "order_m1_side": base_side,
            "order_time_m1": "",
            "order_id_m2": "",
            "order_m2_size": quote_size,
            "order_m2_side": quote_side,
            "order_time_m2": "",
            "pair_status": "",
            "comments": "",
        }

    # Check order status by id
    async def check_order_status_by_id(self, order_id):

        # Allow time to process
        await asyncio.sleep(2)

        # Check order status
        order_status = await check_order_status(self.indexer, order_id)

        # Guard: If order cancelled move onto next Pair
        if order_status == "CANCELED":
            print(f"{self.market_1} vs {self.market_2} - Order cancelled...")
            self.order_dict["pair_status"] = "FAILED"
            return "failed"

        # Guard: If order not filled wait until order expiration
        if order_status != "FAILED":
            await asyncio.sleep(10)
            order_status = check_order_status(self.indexer, order_id)

            # Guard: If order cancelled move onto next Pair
            if order_status == "CANCELED":
                print(f"{self.market_1} vs {self.market_2} - Order cancelled...")
                self.order_dict["pair_status"] = "FAILED"
                return "failed"

            # Guard: If not filled, cancel order
            if order_status != "FILLED":
                self.node.private.cancel_order(order_id=order_id)
                self.order_dict["pair_status"] = "ERROR"
                print(f"{self.market_1} vs {self.market_2} - Order error...")
                return "error"

        # Return live
        return "live"


    async def open_trades(self, wallet):

        # 1. ORDER M1
        try:
            order_m1 = await place_market_order(
                self.node, self.indexer, wallet,
                self.market_1, self.base_side, self.base_size, self.base_price, False, time_in_force_type=Order.TimeInForce.TIME_IN_FORCE_IOC
            )
            if "id" in order_m1.get("order", {}):
                self.order_dict["order_id_m1"] = order_m1["order"]["id"]
                self.order_dict["order_time_m1"] = datetime.now().isoformat()
            else:
                raise Exception(f"API Response missing ID: {order_m1}")
        except Exception as e:
            print(f" ERROR Executing M1 ({self.market_1}): {e}")
            self.order_dict["pair_status"] = "ERROR"
            self.order_dict["comments"] = f"M1 Fail: {e}"
            return self.order_dict

        # 1.5) AUDIT M1 (Gatekeeper)

        audit_m1 = await get_real_fill_details(self.indexer, self.order_dict["order_id_m1"], self.market_1)
        status_m1 = audit_m1["status_label"]
        real_filled_m1 = float(audit_m1["filled_size"])

        print(f" Report M1: {status_m1} | Real fill: {real_filled_m1}")

        if real_filled_m1 <= 0:
            print("  M1 fill was 0. Skipping M2.")
            self.order_dict["pair_status"] = "FAILED"
            self.order_dict["comments"] = "M1 Filled 0. Sequence stopped."
            return self.order_dict

        # 2) ORDER M2 (USD-neutral hedge)

        base_px = float(self.base_price)
        quote_px = float(self.quote_price)

        filled_usd_1 = real_filled_m1 * base_px

        if quote_px > 0:
            adjusted_quote_size = filled_usd_1 / quote_px
        else:
            adjusted_quote_size = 0.0

        print(f" M1 filled ~${filled_usd_1:.2f}. Setting M2 size to: {adjusted_quote_size} (USD-neutral).")

        if adjusted_quote_size <= 0:
            print("  adjusted_quote_size invalid. Skipping M2.")
            self.order_dict["pair_status"] = "FAILED"
            self.order_dict["comments"] = "Bad quote price or size. Sequence stopped."
            return self.order_dict

        try:
            order_m2 = await place_market_order(
                self.node, self.indexer, wallet,
                self.market_2, self.quote_side, adjusted_quote_size, self.quote_price, False, time_in_force_type=Order.TimeInForce.TIME_IN_FORCE_IOC
            )
            if "id" in order_m2.get("order", {}):
                self.order_dict["order_id_m2"] = order_m2["order"]["id"]
                self.order_dict["order_time_m2"] = datetime.now().isoformat()
            else:
                raise Exception(f"API Response missing ID: {order_m2}")

        except Exception as e:
            print(f" ERROR Executing M2 ({self.market_2}): {e}")
            self.order_dict["pair_status"] = "ERROR"
            self.order_dict["comments"] = f"M2 Fail: {e}"
            return self.order_dict

        # 2.5) AUDIT M2

        audit_m2 = await get_real_fill_details(self.indexer, self.order_dict["order_id_m2"], self.market_2)

        status_m2 = audit_m2["status_label"]
        real_filled_m2 = float(audit_m2["filled_size"])

        print(f" Report M2: {status_m2} | Real fill: {real_filled_m2}")

        # 3) FLATTEN (si M2 no alcanzó a cubrir el USD de M1)
        filled_usd_2 = real_filled_m2 * quote_px  # notional aproximado de M2

        residual_usd = filled_usd_1 - filled_usd_2
        effective_usd = min(filled_usd_1, filled_usd_2)
        tol = max(2.0, 0.02 * effective_usd)

        print(f" Residual after M2: ${residual_usd:.2f} (tol ${tol:.2f})")

        if abs(residual_usd) > tol:
            print(" Residual too large -> FLATTEN with reduce-only IOC")

            try:
                if residual_usd > 0:
                    # M1 quedó más grande que M2 -> recorta M1 (reduce-only)
                    size_to_close_m1 = residual_usd / base_px if base_px > 0 else 0.0
                    close_side_m1 = "SELL" if self.base_side == "BUY" else "BUY"

                    await place_market_order(
                        self.node, self.indexer, wallet,
                        self.market_1, close_side_m1, size_to_close_m1, self.base_price,
                        True,  # reduce_only=True
                        time_in_force_type=Order.TimeInForce.TIME_IN_FORCE_IOC
                    )

                    self.order_dict["comments"] = f"Flattened residual by reducing M1 ~${residual_usd:.2f}"

                else:
                    # M2 quedó más grande que M1 -> recorta M2 (reduce-only)
                    size_to_close_m2 = (-residual_usd) / quote_px if quote_px > 0 else 0.0
                    close_side_m2 = "SELL" if self.quote_side == "BUY" else "BUY"

                    await place_market_order(
                        self.node, self.indexer, wallet,
                        self.market_2, close_side_m2, size_to_close_m2, self.quote_price,
                        True,  # reduce_only=True
                        time_in_force_type=Order.TimeInForce.TIME_IN_FORCE_IOC
                    )

                    self.order_dict["comments"] = f"Flattened residual by reducing M2 ~${-residual_usd:.2f}"

            except Exception as e:
                # Si el flatten falla, prefiero marcarlo como ERROR porque quedas direccional
                print(f" ERROR Flattening residual: {e}")
                self.order_dict["pair_status"] = "ERROR"
                self.order_dict["comments"] = f"Flatten fail: {e}"
                return self.order_dict

        # =========================================================
        # DONE
        # =========================================================
        self.order_dict["pair_status"] = "LIVE"
        return self.order_dict

