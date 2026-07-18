#!/usr/bin/env python3
import unittest

from func_fill_audit import summarize_order_fills


class FillAuditTests(unittest.TestCase):
    def test_joins_client_order_to_fill_order_id(self):
        orders = [{
            "id": "exchange-order-uuid", "clientId": "123", "ticker": "LTC-USD",
            "status": "FILLED", "size": "0.7", "totalFilled": "0.7",
            "price": "42.98",
        }]
        fills = [{
            "id": "fill-1", "orderId": "exchange-order-uuid", "market": "LTC-USD",
            "size": "0.7", "price": "43.86", "fee": "0.015351",
            "liquidity": "TAKER",
        }]
        result = summarize_order_fills("123", "LTC-USD", orders, fills)
        self.assertAlmostEqual(result["avg_price"], 43.86)
        self.assertAlmostEqual(result["filled_usd_est"], 30.702)
        self.assertFalse(result["price_estimated"])

    def test_deduplicates_fills_from_two_queries(self):
        order = {"id": "oid", "client_id": "7", "ticker": "APT-USD", "status": "FILLED"}
        fill = {"id": "fid", "order_id": "oid", "market": "APT-USD",
                "size": "25", "price": "0.6", "fee": "0.0075"}
        result = summarize_order_fills("7", "APT-USD", [order], [fill, dict(fill)])
        self.assertEqual(result["matched_fill_count"], 1)
        self.assertEqual(result["filled_size"], 25.0)
        self.assertEqual(result["fee_total"], 0.0075)

    def test_does_not_treat_limit_as_fill_price(self):
        orders = [{
            "id": "oid", "clientId": "99", "ticker": "INJ-USD",
            "status": "FILLED", "size": "6.1", "totalFilled": "6.1",
            "price": "4.885",
        }]
        result = summarize_order_fills("99", "INJ-USD", orders, [])
        self.assertEqual(result["status_label"], "FILL_DETAILS_PENDING")
        self.assertEqual(result["filled_size"], 0.0)
        self.assertEqual(result["avg_price"], 0.0)
        self.assertEqual(result["reported_filled_size"], 6.1)

    def test_estimates_fee_only_when_fill_omits_it(self):
        orders = [{"id": "oid", "clientId": "1", "ticker": "DOT-USD"}]
        fills = [{"id": "f", "orderId": "oid", "market": "DOT-USD",
                  "size": "10", "price": "1", "fee": "0"}]
        result = summarize_order_fills("1", "DOT-USD", orders, fills)
        self.assertAlmostEqual(result["fee_total"], 0.005)
        self.assertTrue(result["fee_estimated"])


if __name__ == "__main__":
    unittest.main()
