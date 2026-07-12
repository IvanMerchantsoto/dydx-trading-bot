import asyncio
import json
import os
import random

from dydx_v4_client import MAX_CLIENT_ID, OrderFlags
from dydx_v4_client.indexer.rest.constants import OrderType
from dydx_v4_client.node.market import Market
from dydx_v4_client.wallet import Wallet
from v4_proto.dydxprotocol.clob.order_pb2 import Order

from constants import (
    API_KEY,
    WALLET_ADDRESS,
    MARKET_MAX_SLIPPAGE_BPS_DEFAULT,
    MARKET_MAX_SLIPPAGE_BPS_EXIT,
    MARKET_MAX_SLIPPAGE_BPS_FLATTEN,
    MARKET_SLIPPAGE_ORACLE_CAP_BPS,
)
from func_logging import log_event


# ---------------------------------------------------------------------------
# Fee constants
# ---------------------------------------------------------------------------

# dYdX v4 taker fee rate (0.05%). Used as fallback when indexer doesn't
# return fee data (common on testnet). Both legs are taker (MARKET IOC).
TAKER_FEE_BPS = 0.0005


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sf(x, d=0.0):
    try:
        return float(x)
    except Exception:
        return d


async def _ensure_wallet(node, wallet):
    if wallet is not None:
        return wallet
    return await Wallet.from_mnemonic(node, API_KEY.strip(), WALLET_ADDRESS.strip())


def _to_dydx_side(side: str):
    if str(side).upper() == "BUY":
        return Order.Side.SIDE_BUY
    return Order.Side.SIDE_SELL


def _extract_tx_result(tx):
    """
    2026-06-02: Helper to extract chain rejection info from the tx response.

    dydx-v4-client's node.place_order returns an object whose shape varies
    by version. We try multiple attribute names because Cosmos SDK wraps
    the response as either tx, tx_response, or BroadcastTxResponse.

    Returns (tx_hash:str|None, tx_code:int|None, raw_log:str|None).
    code == 0 → broadcast accepted (still may fail at execution)
    code != 0 → broadcast rejected (sequence mismatch, fees, signature, etc.)
    """
    if tx is None:
        return None, None, "tx_is_None"

    # Direct attributes
    tx_hash = getattr(tx, "tx_hash", None) or getattr(tx, "txhash", None)
    tx_code = getattr(tx, "code", None)
    raw_log = getattr(tx, "raw_log", None)

    # Some clients wrap result in tx_response
    if hasattr(tx, "tx_response"):
        tr = tx.tx_response
        tx_hash = tx_hash or getattr(tr, "txhash", None) or getattr(tr, "tx_hash", None)
        if tx_code is None:
            tx_code = getattr(tr, "code", None)
        raw_log = raw_log or getattr(tr, "raw_log", None)

    return tx_hash, tx_code, raw_log


async def _get_market_obj_and_oracle(indexer, market: str):
    m_data = await indexer.markets.get_perpetual_markets(market)
    market_data = m_data["markets"][market]
    oracle_price = _sf(market_data.get("oraclePrice"))
    market_obj = Market(market_data)
    return market_obj, oracle_price, market_data


async def _resync_sequence(node, wallet):
    """
    E1 fix: re-sincroniza wallet.sequence con el estado committed de la cadena.
    Se llama SÓLO ante rechazo/excepción de un broadcast (self-heal). En el
    camino feliz el sequence lo incrementamos localmente tras cada OK.
    Con el SequenceManager del SDK desactivado (LOCAL_SEQUENCE_MANAGEMENT),
    esta es la única fuente de re-sincronización.
    """
    if wallet is None or node is None:
        return
    try:
        acct = await node.get_account(wallet.address)
        old = getattr(wallet, "sequence", None)
        wallet.sequence = acct.sequence
        log_event({
            "type": "sequence_resync",
            "old_sequence": old,
            "new_sequence": acct.sequence,
        }, print_terminal=False)
    except Exception as e:
        log_event({"type": "sequence_resync_error", "error": str(e)}, print_terminal=False)


def _bounded_taker_price(side: str, oracle_price: float, best_bid, best_ask,
                         max_slippage_bps) -> tuple:
    """
    E2 fix: precio límite para una orden MARKET/taker IOC, acotado.

    Reemplaza la vieja banda fija oracle±5% (=500bps). El límite = mejor precio
    del book (touch) + tolerancia, PERO nunca más lejos del oráculo que el tope
    duro MARKET_SLIPPAGE_ORACLE_CAP_BPS. Si el book está tan torcido que el
    touch queda fuera del tope, el límite queda del lado seguro y la IOC no
    llena (NOT_FOUND) — comportamiento deseado: no operar books rotos.

    Returns (execution_price, ref_source) donde ref_source ∈ {"book","oracle"}.
    """
    slip = (float(max_slippage_bps) if max_slippage_bps is not None
            else float(MARKET_MAX_SLIPPAGE_BPS_DEFAULT))
    tol = slip / 10_000.0
    cap = float(MARKET_SLIPPAGE_ORACLE_CAP_BPS) / 10_000.0
    is_buy = str(side).upper() == "BUY"

    if is_buy:
        ref = best_ask if (best_ask and best_ask > 0) else oracle_price
        src = "book" if (best_ask and best_ask > 0) else "oracle"
        px = ref * (1.0 + tol)
        if oracle_price and oracle_price > 0:
            px = min(px, oracle_price * (1.0 + cap))
    else:
        ref = best_bid if (best_bid and best_bid > 0) else oracle_price
        src = "book" if (best_bid and best_bid > 0) else "oracle"
        px = ref * (1.0 - tol)
        if oracle_price and oracle_price > 0:
            px = max(px, oracle_price * (1.0 - cap))
    return float(px), src


# ---------------------------------------------------------------------------
# Position reconciliation helpers
# ---------------------------------------------------------------------------

async def get_subaccount_positions(indexer):
    try:
        resp = await indexer.account.get_subaccount(WALLET_ADDRESS.strip(), 0)
        sub = resp.get("subaccount", {}) or {}
        positions = sub.get("openPerpetualPositions", {}) or sub.get("perpetualPositions", {}) or {}
        return positions
    except Exception:
        return {}


async def get_market_position_size(indexer, market: str) -> float:
    positions = await get_subaccount_positions(indexer)
    pos = positions.get(market, {}) or {}
    return _sf(pos.get("size", 0.0))


async def get_positions_snapshot(indexer, markets):
    snap = {}
    positions = await get_subaccount_positions(indexer)
    for m in markets:
        pos = positions.get(m, {}) or {}
        snap[m] = _sf(pos.get("size", 0.0))
    return snap


async def reconcile_positions(indexer, market_1: str, market_2: str):
    snap = await get_positions_snapshot(indexer, [market_1, market_2])
    return {
        "market_1": market_1,
        "market_2": market_2,
        "pos_1": _sf(snap.get(market_1, 0.0)),
        "pos_2": _sf(snap.get(market_2, 0.0)),
    }


# ---------------------------------------------------------------------------
# Orders / status
# ---------------------------------------------------------------------------

async def check_order_status(indexer, client_id):
    await asyncio.sleep(1.5)
    try:
        response = await indexer.account.get_subaccount_orders(
            address=WALLET_ADDRESS.strip(),
            subaccount_number=0,
        )

        if response is None:
            return "UNKNOWN"

        orders = response if isinstance(response, list) else response.get("orders", [])
        for order in orders:
            if str(order.get("clientId")) == str(client_id):
                return order.get("status")
        return "NOT_FOUND"
    except Exception as e:
        print(f"Error in Indexer: {e}")
        return "FAILED"


async def cancel_order_by_client_id(node, client_id: str):
    cid = str(client_id)

    try:
        if hasattr(node, "cancel_order"):
            res = node.cancel_order(order_id=cid)
            if asyncio.iscoroutine(res):
                await res
            return True
    except Exception as e:
        print(f"[CANCEL] node.cancel_order failed client_id={cid}: {e}")

    try:
        priv = getattr(node, "private", None)
        if priv is not None and hasattr(priv, "cancel_order"):
            res = priv.cancel_order(order_id=cid)
            if asyncio.iscoroutine(res):
                await res
            return True
    except Exception as e:
        print(f"[CANCEL] node.private.cancel_order failed client_id={cid}: {e}")

    for attr in ("clob", "orders", "order", "private_client"):
        try:
            obj = getattr(node, attr, None)
            if obj is not None and hasattr(obj, "cancel_order"):
                res = obj.cancel_order(order_id=cid)
                if asyncio.iscoroutine(res):
                    await res
                return True
        except Exception as e:
            print(f"[CANCEL] node.{attr}.cancel_order failed client_id={cid}: {e}")

    print(f"[CANCEL] No cancel method found for client_id={cid}.")
    return False


# ---------------------------------------------------------------------------
# Order placement
# ---------------------------------------------------------------------------

async def place_market_order(
    node,
    indexer,
    wallet,
    market,
    side,
    size,
    price,  # kept for backwards compatibility (NO longer used as the band)
    reduce_only,
    time_in_force_type,
    max_slippage_bps=None,
):
    try:
        wallet = await _ensure_wallet(node, wallet)
        market_obj, oracle_price, _mdata = await _get_market_obj_and_oracle(indexer, market)
        dydx_side = _to_dydx_side(side)

        # E2 fix: precio límite acotado desde el book, no banda fija ±5%.
        best_bid, best_ask = await get_orderbook_best(indexer, market)
        execution_price, _px_src = _bounded_taker_price(
            side, oracle_price, best_bid, best_ask, max_slippage_bps
        )

        client_id = random.randint(0, MAX_CLIENT_ID)
        order_id = market_obj.order_id(
            WALLET_ADDRESS.strip(), 0, client_id, OrderFlags.SHORT_TERM
        )

        current_block = await node.latest_block_height()

        placed_order = market_obj.order(
            order_id=order_id,
            order_type=OrderType.MARKET,
            post_only=False,
            side=dydx_side,
            size=float(size),
            price=execution_price,
            time_in_force=time_in_force_type,
            reduce_only=bool(reduce_only),
            # 2026-06-30: GoodTilBlock buffer subido de 20 → 40 (~56s a 1.4s/blk).
            # Log mostró 276 broadcasts rechazados code=10 "GoodTilBlock < current
            # blockHeight" — 30+ segundos pasan entre node.latest_block_height()
            # y la llegada al chain. Con buffer 20 (~28s), apenas no alcanzaba.
            # Con 40 (~56s), tenemos colchón para latencia de mempool / gRPC.
            # MAX permitido por dYdX para SHORT_TERM = 50.
            good_til_block=current_block + 40,
        )

        _slip_used = (float(max_slippage_bps) if max_slippage_bps is not None
                      else float(MARKET_MAX_SLIPPAGE_BPS_DEFAULT))
        log_event({
            "type": "tx_market",
            "side": str(side).upper(),
            "market": market,
            "size": float(size),
            "oracle": oracle_price,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "exec_price": execution_price,
            "px_source": _px_src,
            "max_slippage_bps": _slip_used,
            "seq": getattr(wallet, "sequence", None),
            "reduce_only": bool(reduce_only),
        }, print_terminal=False)

        tx = await node.place_order(wallet=wallet, order=placed_order)

        # 2026-06-02: extraer y loguear el resultado de chain (code + raw_log)
        tx_hash, tx_code, raw_log = _extract_tx_result(tx)
        broadcast_ok = (tx_code is None or tx_code == 0)
        log_event({
            "type": "tx_broadcast_result",
            "stage": "market",
            "market": market,
            "client_id": str(client_id),
            "tx_hash": str(tx_hash) if tx_hash else None,
            "tx_code": tx_code,
            "raw_log": (str(raw_log)[:300] if raw_log else None),
            "broadcast_ok": broadcast_ok,
        }, print_terminal=False)

        if not broadcast_ok:
            # Chain rechazó la tx. NO incrementar sequence — la tx no consumió
            # nonce. Re-sincronizamos desde la cadena por si el desajuste fue de
            # sequence (self-heal E1).
            print(f"[MARKET] ⚠️ Chain rechazó {market} cid={client_id} "
                  f"code={tx_code} log={str(raw_log)[:120]}", flush=True)
            log_event({
                "type": "order_chain_rejected",
                "stage": "market",
                "market": market,
                "client_id": str(client_id),
                "tx_code": tx_code,
                "raw_log": (str(raw_log)[:500] if raw_log else None),
            })
            await _resync_sequence(node, wallet)
            return {
                "order": {"id": str(client_id), "status": "REJECTED", "market": market},
                "tx_hash": str(tx_hash) if tx_hash else "unknown",
                "tx_code": tx_code,
                "rejected": True,
            }

        # Broadcast aceptado: consumimos el nonce localmente (E1).
        wallet.sequence += 1

        return {
            "order": {"id": str(client_id), "status": "PENDING", "market": market},
            "tx_hash": str(tx_hash) if tx_hash else "unknown",
            "tx_code": tx_code,
            "rejected": False,
        }

    except Exception as e:
        print(f"[MARKET] Error enviando orden: {e}")
        log_event({
            "type": "order_error",
            "stage": "market",
            "market": market,
            "side": str(side).upper(),
            "size": _sf(size),
            "reduce_only": bool(reduce_only),
            "error": str(e),
        })
        # Estado del sequence incierto tras la excepción → re-sincronizar.
        await _resync_sequence(node, wallet)
        return None


async def place_limit_order(
    node,
    indexer,
    wallet,
    market,
    side,
    size,
    limit_price,
    reduce_only=False,
    post_only=True,
    time_in_force_type=None,
    good_til_blocks=40,
):
    try:
        wallet = await _ensure_wallet(node, wallet)
        market_obj, oracle_price, _mdata = await _get_market_obj_and_oracle(indexer, market)
        dydx_side = _to_dydx_side(side)

        client_id = random.randint(0, MAX_CLIENT_ID)
        order_id = market_obj.order_id(
            WALLET_ADDRESS.strip(), 0, client_id, OrderFlags.SHORT_TERM
        )

        current_block = await node.latest_block_height()

        if time_in_force_type is None:
            time_in_force_type = getattr(Order.TimeInForce, "TIME_IN_FORCE_GTT", None) or getattr(
                Order.TimeInForce, "TIME_IN_FORCE_IOC"
            )

        placed_order = market_obj.order(
            order_id=order_id,
            order_type=OrderType.LIMIT,
            post_only=bool(post_only),
            side=dydx_side,
            size=float(size),
            price=float(limit_price),
            time_in_force=time_in_force_type,
            reduce_only=bool(reduce_only),
            good_til_block=current_block + int(good_til_blocks),
        )

        log_event({
            "type": "tx_limit",
            "side": str(side).upper(),
            "market": market,
            "size": float(size),
            "limit_price": float(limit_price),
            "oracle": oracle_price,
            "reduce_only": bool(reduce_only),
            "post_only": bool(post_only),
            "good_til_blocks": int(good_til_blocks),
        }, print_terminal=False)

        tx = await node.place_order(wallet=wallet, order=placed_order)

        # 2026-06-02: extraer y loguear chain rejection (igual que market)
        tx_hash, tx_code, raw_log = _extract_tx_result(tx)
        broadcast_ok = (tx_code is None or tx_code == 0)
        log_event({
            "type": "tx_broadcast_result",
            "stage": "limit",
            "market": market,
            "client_id": str(client_id),
            "tx_hash": str(tx_hash) if tx_hash else None,
            "tx_code": tx_code,
            "raw_log": (str(raw_log)[:300] if raw_log else None),
            "broadcast_ok": broadcast_ok,
        }, print_terminal=False)

        if not broadcast_ok:
            print(f"[LIMIT] ⚠️ Chain rechazó {market} cid={client_id} "
                  f"code={tx_code} log={str(raw_log)[:120]}", flush=True)
            log_event({
                "type": "order_chain_rejected",
                "stage": "limit",
                "market": market,
                "client_id": str(client_id),
                "tx_code": tx_code,
                "raw_log": (str(raw_log)[:500] if raw_log else None),
            })
            await _resync_sequence(node, wallet)
            return {
                "order": {"id": str(client_id), "status": "REJECTED", "market": market},
                "tx_hash": str(tx_hash) if tx_hash else "unknown",
                "tx_code": tx_code,
                "rejected": True,
            }

        wallet.sequence += 1

        return {
            "order": {"id": str(client_id), "status": "PENDING", "market": market},
            "tx_hash": str(tx_hash) if tx_hash else "unknown",
        }

    except Exception as e:
        print(f"[LIMIT] Error enviando orden: {e}")
        log_event({
            "type": "order_error",
            "stage": "limit",
            "market": market,
            "side": str(side).upper(),
            "size": _sf(size),
            "limit_price": _sf(limit_price),
            "reduce_only": bool(reduce_only),
            "post_only": bool(post_only),
            "error": str(e),
        })
        await _resync_sequence(node, wallet)
        return None


# ---------------------------------------------------------------------------
# Fill details with broad fallback
# ---------------------------------------------------------------------------

async def get_real_fill_details(indexer, client_id, market, max_fill_lookback=200):
    cid = str(client_id)

    def _extract_cid(obj):
        return str(
            obj.get("clientId")
            or obj.get("client_id")
            or obj.get("orderClientId")
            or obj.get("order_client_id")
            or ""
        )

    def _extract_fee(fill):
        return (
            _sf(fill.get("fee"))
            or _sf(fill.get("feeAmount"))
            or _sf(fill.get("feeUsd"))
            or 0.0
        )

    await asyncio.sleep(1.2)

    fills_candidates = []
    try:
        fills_resp = await indexer.account.get_subaccount_fills(
            address=WALLET_ADDRESS.strip(),
            subaccount_number=0,
            ticker=market,
            limit=max_fill_lookback,
        )
        fills = fills_resp if isinstance(fills_resp, list) else fills_resp.get("fills", [])
        fills_candidates.extend(fills)
    except Exception:
        pass

    try:
        fills_resp = await indexer.account.get_subaccount_fills(
            address=WALLET_ADDRESS.strip(),
            subaccount_number=0,
            limit=max_fill_lookback,
        )
        fills = fills_resp if isinstance(fills_resp, list) else fills_resp.get("fills", [])
        fills_candidates.extend(fills)
    except Exception:
        pass

    matched_fills = [f for f in fills_candidates if _extract_cid(f) == cid]

    if matched_fills:
        total_size = 0.0
        total_usd = 0.0
        total_fee = 0.0
        liquidity = None

        for fill in matched_fills:
            size = _sf(fill.get("size") or fill.get("filledSize") or fill.get("amount"))
            price = _sf(fill.get("price") or fill.get("fillPrice"))
            total_size += size
            total_usd += size * price
            total_fee += _extract_fee(fill)
            liquidity = fill.get("liquidity") or fill.get("liquiditySide") or liquidity

        avg_price = total_usd / total_size if total_size > 0 else 0.0

        # Fallback: indexer (especially testnet) often omits fee data.
        # If fee_total is zero but we have a real fill, estimate taker fee.
        fee_estimated = False
        if total_fee == 0.0 and total_usd > 0.0:
            total_fee = total_usd * TAKER_FEE_BPS
            fee_estimated = True

        return {
            "found": True,
            "status_label": "FILLED" if total_size > 0 else "BEST_EFFORT_OPENED",
            "raw_status": "FILLED" if total_size > 0 else "BEST_EFFORT_OPENED",
            "filled_size": total_size,
            "filled_usd_est": total_usd,
            "fee_total": total_fee,
            "fee_estimated": fee_estimated,
            "liquidity": liquidity,
            "server_order_id": None,
            "avg_price": avg_price,
        }

    orders_candidates = []
    try:
        orders_resp = await indexer.account.get_subaccount_orders(
            address=WALLET_ADDRESS.strip(),
            subaccount_number=0,
            ticker=market,
            limit=max_fill_lookback,
        )
        orders = orders_resp if isinstance(orders_resp, list) else orders_resp.get("orders", [])
        orders_candidates.extend(orders)
    except Exception:
        pass

    try:
        orders_resp = await indexer.account.get_subaccount_orders(
            address=WALLET_ADDRESS.strip(),
            subaccount_number=0,
            limit=max_fill_lookback,
        )
        orders = orders_resp if isinstance(orders_resp, list) else orders_resp.get("orders", [])
        orders_candidates.extend(orders)
    except Exception:
        pass

    seen = set()
    deduped_orders = []
    for order in orders_candidates:
        key = str(order.get("id") or "") + "|" + str(order.get("clientId") or "")
        if key not in seen:
            seen.add(key)
            deduped_orders.append(order)

    for order in deduped_orders:
        if _extract_cid(order) != cid:
            continue

        raw_status = str(order.get("status") or "UNKNOWN").upper()
        original_size = _sf(order.get("size", 0))
        remaining = _sf(order.get("remainingSize", 0))
        avg_price = _sf(
            order.get("averageFilledPrice")
            or order.get("avgFillPrice")
            or order.get("price")
            or 0.0
        )

        filled_size = _sf(
            order.get("totalFilled")
            or order.get("filledSize")
            or order.get("sizeFilled")
            or max(0.0, original_size - remaining)
        )

        fee_total = _sf(order.get("fee") or order.get("feeAmount") or order.get("feeUsd") or 0.0)
        filled_usd = filled_size * avg_price

        # Fallback: estimate taker fee when indexer omits fee data
        fee_estimated = False
        if fee_total == 0.0 and filled_usd > 0.0:
            fee_total = filled_usd * TAKER_FEE_BPS
            fee_estimated = True

        if raw_status == "FILLED":
            status_label = "FILLED"
        elif raw_status == "CANCELED" and filled_size > 0:
            status_label = "PARTIALLY_FILLED_IOC"
        elif raw_status == "CANCELED" and filled_size <= 0:
            status_label = "KILLED_BY_FOK"
        elif raw_status in ("OPEN", "PENDING") and filled_size > 0:
            status_label = "BEST_EFFORT_OPENED"
        else:
            status_label = raw_status

        return {
            "found": True,
            "status_label": status_label,
            "raw_status": raw_status,
            "filled_size": filled_size,
            "filled_usd_est": filled_usd,
            "fee_total": fee_total,
            "fee_estimated": fee_estimated,
            "liquidity": order.get("liquidity") or order.get("liquiditySide"),
            "server_order_id": order.get("id"),
            "avg_price": avg_price,
        }

    return {
        "found": False,
        "status_label": "NOT_FOUND",
        "raw_status": "NOT_FOUND",
        "filled_size": 0.0,
        "filled_usd_est": 0.0,
        "fee_total": 0.0,
        "liquidity": None,
        "server_order_id": None,
        "avg_price": 0.0,
    }


# ---------------------------------------------------------------------------
# Abort / Close everything
# ---------------------------------------------------------------------------

async def abort_all_positions(node, indexer, max_rounds=3, ignore_markets=None):
    """
    Cierra posiciones, pero NO se queda bucleado.
    Si después de max_rounds siguen abiertas, las reporta y sigue.
    """
    import json
    import asyncio
    from dydx_v4_client.wallet import Wallet
    from v4_proto.dydxprotocol.clob.order_pb2 import Order
    from constants import API_KEY, WALLET_ADDRESS

    ignore_markets = set(ignore_markets or [])

    wallet = await Wallet.from_mnemonic(node, API_KEY.strip(), WALLET_ADDRESS.strip())

    async def get_live_positions():
        account_resp = await indexer.account.get_subaccount(WALLET_ADDRESS.strip(), 0)
        sub = account_resp.get("subaccount", {}) or {}
        positions = sub.get("openPerpetualPositions", {}) or sub.get("perpetualPositions", {}) or {}

        live = {}
        for market, p in positions.items():
            size = _sf(p.get("size", 0))
            if abs(size) > 0:
                live[market] = size
        return live

    print("[ABORT] Starting bounded close...")

    # Fetch oracle prices once per round for limit-price calculation.
    async def get_oracle_prices():
        try:
            resp = await indexer.markets.get_perpetual_markets()
            mdata = resp.get("markets", {}) or {}
            return {m: _sf(v.get("oraclePrice")) for m, v in mdata.items()}
        except Exception:
            return {}

    for round_i in range(1, int(max_rounds) + 1):
        live = await get_live_positions()
        oracles = await get_oracle_prices()

        live_to_close = {
            m: s for m, s in live.items()
            if m not in ignore_markets
        }

        if not live_to_close:
            print("[ABORT] No closable open positions left.")
            break

        print(f"[ABORT] Round {round_i}/{max_rounds}: closing {len(live_to_close)} positions...")

        for market, size in live_to_close.items():
            side = "SELL" if size > 0 else "BUY"
            qty = abs(size)

            oracle = oracles.get(market, 0.0)
            if oracle <= 0:
                print(f"[ABORT] No oracle price for {market}, skipping.")
                continue

            # For IOC orders the "price" is the worst acceptable fill price.
            # BUY to close short → accept up to 10% above oracle.
            # SELL to close long → accept down to 10% below oracle.
            slippage = 0.10
            price = oracle * (1 + slippage) if side == "BUY" else oracle * (1 - slippage)

            try:
                await place_market_order(
                    node=node,
                    indexer=indexer,
                    wallet=wallet,
                    market=market,
                    side=side,
                    size=qty,
                    price=price,
                    reduce_only=True,
                    time_in_force_type=Order.TimeInForce.TIME_IN_FORCE_IOC,
                    max_slippage_bps=MARKET_MAX_SLIPPAGE_BPS_FLATTEN,
                )
                print(f"[ABORT] Sent close {market} size={qty} side={side} price≤{price:.4f}")
            except Exception as e:
                print(f"[ABORT] Close failed {market}: {e}")

            await asyncio.sleep(0.75)

        await asyncio.sleep(2.0)

    remaining = await get_live_positions()

    ignored = {
        m: s for m, s in remaining.items()
        if m in ignore_markets
    }

    stuck = {
        m: s for m, s in remaining.items()
        if m not in ignore_markets
    }

    if stuck:
        print(f"[ABORT] WARNING: still open after {max_rounds} rounds:")
        for m, s in stuck.items():
            print(f"  - {m}: {s}")

    if ignored:
        print("[ABORT] Ignored positions:")
        for m, s in ignored.items():
            print(f"  - {m}: {s}")

    try:
        _json_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bot_agents.json")
        with open(_json_path, "w") as f:
            json.dump([], f, indent=2)
        print("[ABORT] bot_agents.json cleared.")
    except Exception as e:
        print(f"[ABORT] Could not clear bot_agents.json: {e}")

    print("[ABORT] Done. Continuing program.")
    return {
        "remaining": remaining,
        "ignored": ignored,
        "stuck": stuck,
    }

# ---------------------------------------------------------------------------
# Orderbook helpers
# ---------------------------------------------------------------------------

async def get_orderbook_best(indexer, market: str):
    """
    Return (best_bid, best_ask) float prices for market.
    Returns (None, None) on any error — callers must handle gracefully.
    """
    try:
        ob = await indexer.markets.get_perpetual_market_orderbook(market=market)
        bids = ob.get("bids", []) if isinstance(ob, dict) else []
        asks = ob.get("asks", []) if isinstance(ob, dict) else []
        best_bid = float(bids[0].get("price", 0) if isinstance(bids[0], dict) else bids[0]) if bids else None
        best_ask = float(asks[0].get("price", 0) if isinstance(asks[0], dict) else asks[0]) if asks else None
        if best_bid and best_bid <= 0:
            best_bid = None
        if best_ask and best_ask <= 0:
            best_ask = None
        return best_bid, best_ask
    except Exception:
        return None, None


# ---------------------------------------------------------------------------
# Maker close with MARKET fallback
# ---------------------------------------------------------------------------

async def close_pair_maker_with_fallback(
    node,
    indexer,
    wallet,
    m1: str,
    side_m1: str,
    size_m1: float,
    m2: str,
    side_m2: str,
    size_m2: float,
    markets: dict,
    timeout_s: float = 45.0,
    trace_id: str = None,
) -> tuple:
    """
    Attempt to close both legs as POST_ONLY maker limit orders simultaneously.

    Strategy:
    1. Fetch bid/ask for both legs.
    2. Place POST_ONLY LIMIT at bid (for SELL) or ask (for BUY) — rests in book as maker.
    3. Poll fills every 4s until timeout.
    4. For any leg still unfilled: cancel maker order → MARKET IOC fallback.

    Returns (result_m1, result_m2) where each result is a dict:
        {"close_type": "maker"|"taker_fallback"|"no_orderbook"|"order_error",
         "filled": bool, "fee_est": float}

    Callers should treat any result with filled=True as a successful close.
    Errors are logged but never raised — worst case both legs fall back to MARKET.
    """
    from func_utils import format_number
    from v4_proto.dydxprotocol.clob.order_pb2 import Order as _Order

    results = {
        m1: {"close_type": "pending", "filled": False, "fee_est": 0.0},
        m2: {"close_type": "pending", "filled": False, "fee_est": 0.0},
    }

    # ── 1. Fetch orderbooks for both legs ─────────────────────────────────
    bid1, ask1 = await get_orderbook_best(indexer, m1)
    bid2, ask2 = await get_orderbook_best(indexer, m2)

    tick_m1 = markets.get(m1, {}).get("tickSize")
    tick_m2 = markets.get(m2, {}).get("tickSize")

    # For SELL close (closing a long): place at bid
    # For BUY close (closing a short): place at ask
    maker_price_m1 = (bid1 if side_m1.upper() == "SELL" else ask1)
    maker_price_m2 = (bid2 if side_m2.upper() == "SELL" else ask2)

    use_maker_m1 = maker_price_m1 is not None and maker_price_m1 > 0
    use_maker_m2 = maker_price_m2 is not None and maker_price_m2 > 0

    if tick_m1 and use_maker_m1:
        maker_price_m1 = float(format_number(maker_price_m1, tick_m1))
    if tick_m2 and use_maker_m2:
        maker_price_m2 = float(format_number(maker_price_m2, tick_m2))

    # good_til_blocks: ~2 blocks/s on dYdX → timeout×2 blocks covers the wait
    gtb = max(10, int(timeout_s * 2.2))

    log_event({
        "type": "maker_exit_attempt",
        "trace_id": trace_id,
        "m1": m1, "m2": m2,
        "maker_price_m1": maker_price_m1,
        "maker_price_m2": maker_price_m2,
        "use_maker_m1": use_maker_m1,
        "use_maker_m2": use_maker_m2,
        "timeout_s": timeout_s,
    }, print_terminal=False)

    # ── 2. Place POST_ONLY limit orders for both legs ─────────────────────
    cid_m1 = None
    cid_m2 = None

    if use_maker_m1:
        r1 = await place_limit_order(
            node, indexer, wallet,
            market=m1, side=side_m1, size=float(size_m1),
            limit_price=maker_price_m1,
            reduce_only=True, post_only=True, good_til_blocks=gtb,
        )
        if r1:
            cid_m1 = str(r1["order"]["id"])
        else:
            results[m1]["close_type"] = "order_error"
    else:
        results[m1]["close_type"] = "no_orderbook"

    if use_maker_m2:
        r2 = await place_limit_order(
            node, indexer, wallet,
            market=m2, side=side_m2, size=float(size_m2),
            limit_price=maker_price_m2,
            reduce_only=True, post_only=True, good_til_blocks=gtb,
        )
        if r2:
            cid_m2 = str(r2["order"]["id"])
        else:
            results[m2]["close_type"] = "order_error"
    else:
        results[m2]["close_type"] = "no_orderbook"

    # ── 3. Poll fills ─────────────────────────────────────────────────────
    loop = asyncio.get_event_loop()
    deadline = loop.time() + float(timeout_s)

    # Track which legs still need filling
    need_m1 = use_maker_m1 and cid_m1 is not None
    need_m2 = use_maker_m2 and cid_m2 is not None

    while loop.time() < deadline and (need_m1 or need_m2):
        await asyncio.sleep(4.0)

        if need_m1:
            det = await get_real_fill_details(indexer, cid_m1, m1, max_fill_lookback=50)
            if det.get("status_label") == "FILLED" and _sf(det.get("filled_size")) > 0:
                need_m1 = False
                results[m1] = {
                    "close_type": "maker",
                    "filled": True,
                    "fee_est": _sf(det.get("fee_total", 0.0)),
                    "avg_price": _sf(det.get("avg_price", 0.0)),
                }
                log_event({
                    "type": "maker_exit_leg_filled",
                    "market": m1, "cid": cid_m1,
                    "avg_price": results[m1]["avg_price"],
                    "fee": results[m1]["fee_est"],
                    "trace_id": trace_id,
                }, print_terminal=False)

        if need_m2:
            det = await get_real_fill_details(indexer, cid_m2, m2, max_fill_lookback=50)
            if det.get("status_label") == "FILLED" and _sf(det.get("filled_size")) > 0:
                need_m2 = False
                results[m2] = {
                    "close_type": "maker",
                    "filled": True,
                    "fee_est": _sf(det.get("fee_total", 0.0)),
                    "avg_price": _sf(det.get("avg_price", 0.0)),
                }
                log_event({
                    "type": "maker_exit_leg_filled",
                    "market": m2, "cid": cid_m2,
                    "avg_price": results[m2]["avg_price"],
                    "fee": results[m2]["fee_est"],
                    "trace_id": trace_id,
                }, print_terminal=False)

    # ── 4. MARKET fallback for any unfilled legs ──────────────────────────
    for mkt, side, size, cid_cancel, needs_close in [
        (m1, side_m1, size_m1, cid_m1, need_m1),
        (m2, side_m2, size_m2, cid_m2, need_m2),
    ]:
        if not needs_close:
            continue

        log_event({
            "type": "maker_exit_timeout_fallback",
            "market": mkt, "cid": cid_cancel,
            "timeout_s": timeout_s, "trace_id": trace_id,
        }, print_terminal=False)

        # Cancel the unfilled maker order
        if cid_cancel:
            try:
                await cancel_order_by_client_id(node, cid_cancel)
                await asyncio.sleep(0.5)
            except Exception as ce:
                log_event({
                    "type": "maker_exit_cancel_error",
                    "market": mkt, "cid": cid_cancel,
                    "error": str(ce), "trace_id": trace_id,
                }, print_terminal=False)

        # Market fallback
        oracle = _sf(markets.get(mkt, {}).get("oraclePrice", 0))
        try:
            await place_market_order(
                node, indexer, wallet,
                mkt, side, float(size), oracle,
                True,
                time_in_force_type=_Order.TimeInForce.TIME_IN_FORCE_IOC,
                max_slippage_bps=MARKET_MAX_SLIPPAGE_BPS_EXIT,
            )
            results[mkt] = {"close_type": "taker_fallback", "filled": True, "fee_est": 0.0}
        except Exception as me:
            log_event({
                "type": "maker_exit_market_fallback_error",
                "market": mkt, "error": str(me), "trace_id": trace_id,
            })
            results[mkt] = {"close_type": "error", "filled": False, "fee_est": 0.0}

    log_event({
        "type": "maker_exit_complete",
        "trace_id": trace_id,
        "m1": m1, "m2": m2,
        "close_type_m1": results[m1].get("close_type"),
        "close_type_m2": results[m2].get("close_type"),
        "fee_m1": results[m1].get("fee_est", 0.0),
        "fee_m2": results[m2].get("fee_est", 0.0),
    }, print_terminal=False)

    return results[m1], results[m2]


async def wait_order_visible(indexer, client_id: str, max_wait_s=6.0):
    cid = str(client_id)
    deadline = asyncio.get_event_loop().time() + float(max_wait_s)

    while asyncio.get_event_loop().time() < deadline:
        try:
            resp = await indexer.account.get_subaccount_orders(
                address=WALLET_ADDRESS.strip(),
                subaccount_number=0,
                limit=100
            )
            orders = resp if isinstance(resp, list) else resp.get("orders", [])
            for o in orders:
                if str(o.get("clientId")) == cid:
                    return True
        except Exception:
            pass

        await asyncio.sleep(0.5)

    return False