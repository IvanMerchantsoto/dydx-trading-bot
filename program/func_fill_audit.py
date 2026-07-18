"""Pure helpers for reconciling dYdX orders with their fills.

dYdX exposes ``clientId`` on order records, while fill records reference the
exchange order UUID through ``orderId``.  Keeping this join in a dependency-free
module makes the accounting logic testable without a node or Indexer connection.
"""


def _sf(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _first(record, *keys):
    for key in keys:
        value = record.get(key)
        if value is not None and value != "":
            return value
    return None


def _client_id(record):
    return str(_first(
        record, "clientId", "client_id", "orderClientId", "order_client_id"
    ) or "")


def _order_id(record):
    return str(_first(record, "orderId", "order_id", "id") or "")


def _market(record):
    return str(_first(record, "market", "ticker") or "")


def _dedupe_fills(fills):
    """Remove duplicates caused by combining ticker and account queries."""
    output = []
    seen = set()
    for fill in fills:
        key = str(fill.get("id") or "")
        if not key:
            key = "|".join(str(_first(fill, name) or "") for name in (
                "orderId", "createdAt", "market", "side", "price", "size"
            ))
        if key in seen:
            continue
        seen.add(key)
        output.append(fill)
    return output


def summarize_order_fills(client_id, market, orders, fills, taker_fee_rate=0.0005):
    """Return authoritative execution details for one client order.

    The function first resolves ``client_id -> order UUID`` using order records,
    then joins fills through their ``orderId``.  A legacy direct-client-id match
    is retained for older Indexer response shapes.
    """
    cid = str(client_id)
    matching_orders = [
        order for order in orders
        if _client_id(order) == cid and (not _market(order) or _market(order) == market)
    ]
    server_order_ids = {_order_id(order) for order in matching_orders if _order_id(order)}

    matching_fills = []
    for fill in _dedupe_fills(fills):
        if _market(fill) and _market(fill) != market:
            continue
        fill_order_id = str(_first(fill, "orderId", "order_id") or "")
        direct_client_id = _client_id(fill)
        if (fill_order_id and fill_order_id in server_order_ids) or direct_client_id == cid:
            matching_fills.append(fill)

    if matching_fills:
        total_size = 0.0
        total_usd = 0.0
        total_fee = 0.0
        liquidity = None
        for fill in matching_fills:
            size = _sf(_first(fill, "size", "filledSize", "amount"))
            price = _sf(_first(fill, "price", "fillPrice"))
            total_size += size
            total_usd += size * price
            total_fee += _sf(_first(fill, "fee", "feeAmount", "feeUsd"))
            liquidity = _first(fill, "liquidity", "liquiditySide") or liquidity

        fee_estimated = False
        if total_fee == 0.0 and total_usd > 0.0:
            total_fee = total_usd * float(taker_fee_rate)
            fee_estimated = True
        return {
            "found": True,
            "status_label": "FILLED",
            "raw_status": "FILLED",
            "filled_size": total_size,
            "filled_usd_est": total_usd,
            "fee_total": total_fee,
            "fee_estimated": fee_estimated,
            "liquidity": liquidity,
            "server_order_id": next(iter(server_order_ids), None),
            "avg_price": total_usd / total_size if total_size > 0 else 0.0,
            "price_estimated": False,
            "matched_fill_count": len(matching_fills),
        }

    if not matching_orders:
        return None

    # The order may be visible before its fills.  Preserve its state for the
    # retry loop, but never pass the order's limit price off as an actual fill.
    order = matching_orders[0]
    raw_status = str(order.get("status") or "UNKNOWN").upper()
    original_size = _sf(order.get("size"))
    remaining = _sf(_first(order, "remainingSize", "remaining_size"))
    reported_filled = _sf(_first(
        order, "totalFilled", "total_filled", "filledSize", "sizeFilled"
    ))
    if reported_filled <= 0 and original_size > 0 and remaining > 0:
        reported_filled = max(0.0, original_size - remaining)
    avg_price = _sf(_first(order, "averageFilledPrice", "avgFillPrice"))

    if reported_filled > 0 and avg_price <= 0:
        status_label = "FILL_DETAILS_PENDING"
        filled_size = 0.0
    elif reported_filled > 0:
        status_label = "FILLED" if raw_status == "FILLED" else "PARTIALLY_FILLED_IOC"
        filled_size = reported_filled
    elif raw_status == "CANCELED":
        status_label = "KILLED_BY_FOK"
        filled_size = 0.0
    elif raw_status in ("OPEN", "PENDING"):
        status_label = "BEST_EFFORT_OPENED"
        filled_size = 0.0
    else:
        status_label = raw_status
        filled_size = 0.0

    total_usd = filled_size * avg_price if avg_price > 0 else 0.0
    fee_total = _sf(_first(order, "fee", "feeAmount", "feeUsd"))
    fee_estimated = False
    if fee_total == 0.0 and total_usd > 0.0:
        fee_total = total_usd * float(taker_fee_rate)
        fee_estimated = True
    return {
        "found": True,
        "status_label": status_label,
        "raw_status": raw_status,
        "filled_size": filled_size,
        "reported_filled_size": reported_filled,
        "filled_usd_est": total_usd,
        "fee_total": fee_total,
        "fee_estimated": fee_estimated,
        "liquidity": _first(order, "liquidity", "liquiditySide"),
        "server_order_id": _order_id(order) or None,
        "avg_price": avg_price,
        "price_estimated": avg_price <= 0,
        "matched_fill_count": 0,
    }
