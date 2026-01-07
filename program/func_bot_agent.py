from func_private import place_market_order, check_order_status
from datetime import datetime
import asyncio


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
        order_status = check_order_status(self.indexer, order_id)

        # Guard: If order cancelled move onto next Pair
        if order_status == "CANCELED":
            print(f"{self.market_1} vs {self.market_2} - Order cancelled...")
            self.order_dict["pair_status"] = "FAILED"
            return "failed"

        # Guard: If order not filled wait until order expiration
        if order_status != "FAILED":
            await asyncio.sleep(15)
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
        print("---")
        print(f"{self.market_1} & {self.market_2}: Executing Pair...")
        print("---")

        # 1. ORDER M1
        try:
            order_m1 = await place_market_order(
                self.node, self.indexer, wallet,
                self.market_1, self.base_side, self.base_size, self.base_price, False
            )
            if "id" in order_m1.get("order", {}):
                self.order_dict["order_id_m1"] = order_m1["order"]["id"]
                self.order_dict["order_time_m1"] = datetime.now().isoformat()
            else:
                # Si la API devuelve algo inesperado
                raise Exception(f"API Response missing ID: {order_m1}")
        except Exception as e:
            print(f"❌ ERROR Ejecutando M1 ({self.market_1}): {e}")
            self.order_dict["pair_status"] = "ERROR"
            self.order_dict["comments"] = f"M1 Fail: {e}"
            return self.order_dict

        # 2. ORDER M2 (Inmediata, porque wallet.sequence ya subió en place_market_order)
        try:
            order_m2 = await place_market_order(
                self.node, self.indexer, wallet,
                self.market_2, self.quote_side, self.quote_size, self.quote_price, False
            )
            if "id" in order_m2.get("order", {}):
                self.order_dict["order_id_m2"] = order_m2["order"]["id"]
                self.order_dict["order_time_m2"] = datetime.now().isoformat()
            else:
                raise Exception(f"API Response missing ID: {order_m2}")
        except Exception as e:
            print(f"❌ ERROR Ejecutando M2 ({self.market_2}): {e}")
            self.order_dict["pair_status"] = "ERROR"
            self.order_dict["comments"] = f"M2 Fail: {e}"
            return self.order_dict

        self.order_dict["pair_status"] = "LIVE"
        return self.order_dict