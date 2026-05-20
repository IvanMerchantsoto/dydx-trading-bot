import json
import os
import asyncio
from datetime import datetime, timezone

from constants import WALLET_ADDRESS, MAX_OPEN_TRADES, UNMANAGED_IGNORE_MARKETS
from func_logging import log_event
from func_messaging import send_message
from func_private import place_market_order, get_real_fill_details
from func_utils import format_number
from v4_proto.dydxprotocol.clob.order_pb2 import Order

JSON_PATH = os.path.join(os.path.dirname(__file__), "bot_agents.json")
COOLDOWN_PATH = os.path.join(os.path.dirname(__file__), "pair_cooldowns.json")

# Safety knobs. Keep conservative until the bot has a clean week.
BLOCK_ENTRIES_ON_UNMANAGED_EXPOSURE = True
BLOCK_ENTRIES_ON_ORPHAN_RECORDS = True
COOLDOWN_MINUTES_AFTER_BAD_OPEN = 180
MAX_REAL_OPEN_MARKETS = int(MAX_OPEN_TRADES) * 2
MIN_POSITION_USD_TO_CARE = 5.0

# Anti-spam: how often (seconds) to re-send "ENTRY BLOCKED" Telegram alerts.
# Without this, a blocked state with 30s loop = 2 messages/min constantly.
ENTRY_BLOCKED_ALERT_COOLDOWN_S = 300  # 5 minutes between repeat block alerts
_last_entry_blocked_alert: float = 0.0


def _sf(x, d=0.0):
    try:
        return float(x)
    except Exception:
        return d


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def pair_key(m1, m2):
    return "/".join(sorted([str(m1), str(m2)]))


def load_json_list(path=JSON_PATH):
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except Exception:
        return []


def save_json_list(data, path=JSON_PATH):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data or [], f, indent=2)
    os.replace(tmp, path)


def load_cooldowns():
    try:
        with open(COOLDOWN_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def save_cooldowns(data):
    tmp = COOLDOWN_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data or {}, f, indent=2)
    os.replace(tmp, COOLDOWN_PATH)


def set_pair_cooldown(m1, m2, reason):
    data = load_cooldowns()
    data[pair_key(m1, m2)] = {"ts": utc_now_iso(), "reason": str(reason)}
    save_cooldowns(data)


def is_pair_in_cooldown(m1, m2, minutes=COOLDOWN_MINUTES_AFTER_BAD_OPEN):
    data = load_cooldowns()
    rec = data.get(pair_key(m1, m2))
    if not rec:
        return False
    try:
        ts = rec.get("ts", "").replace("Z", "+00:00")
        dt = datetime.fromisoformat(ts)
        age_min = (datetime.now(timezone.utc) - dt).total_seconds() / 60.0
        return age_min < float(minutes)
    except Exception:
        return True


async def get_subaccount(indexer):
    resp = await indexer.account.get_subaccount(WALLET_ADDRESS, 0)
    return resp.get("subaccount", {}) or {}


async def get_markets(indexer):
    resp = await indexer.markets.get_perpetual_markets()
    return resp.get("markets", {}) or {}


async def get_live_positions(indexer, markets=None, min_usd=MIN_POSITION_USD_TO_CARE):
    sub = await get_subaccount(indexer)
    positions = sub.get("openPerpetualPositions", {}) or sub.get("perpetualPositions", {}) or {}
    if markets is None:
        markets = await get_markets(indexer)

    live = {}
    for market, pdata in (positions or {}).items():
        size = _sf(pdata.get("size"), 0.0)
        if abs(size) <= 0:
            continue
        px = _sf(markets.get(market, {}).get("oraclePrice"), 0.0)
        notional = abs(size) * px if px > 0 else 0.0
        if notional >= float(min_usd):
            live[market] = {
                "market": market,
                "size": size,
                "oraclePrice": px,
                "notional": notional,
                "raw": pdata,
            }
    return live


def json_expected_markets(records=None, include_orphan=True):
    records = load_json_list() if records is None else records
    expected = set()
    pair_records = []
    for r in records:
        status = str(r.get("pair_status", "")).upper()
        if status == "ORPHAN" and not include_orphan:
            continue
        m1 = r.get("market_1")
        m2 = r.get("market_2")
        if m1:
            expected.add(m1)
        if m2:
            expected.add(m2)
        if m1 and m2:
            pair_records.append((m1, m2, status))
    return expected, pair_records


async def reconcile_bot_vs_dydx(indexer, drop_stale=False):
    """
    Source of truth is dYdX positions, not bot_agents.json.
    Returns a dict that entry/main can use to block new trades.
    """
    markets = await get_markets(indexer)
    live = await get_live_positions(indexer, markets=markets)
    records = load_json_list()
    expected_markets, pair_records = json_expected_markets(records, include_orphan=True)

    live_markets = set(live.keys())
    # Exclude markets we've explicitly decided to ignore (testnet stuck positions).
    # These are in UNMANAGED_IGNORE_MARKETS because they can't be closed via the
    # normal MARKET IOC path and should not block new entries indefinitely.
    ignored_markets = set(UNMANAGED_IGNORE_MARKETS or [])
    unmanaged = sorted((live_markets - expected_markets) - ignored_markets)
    stale = sorted(expected_markets - live_markets)
    orphan_records = [r for r in records if str(r.get("pair_status", "")).upper() == "ORPHAN"]

    if drop_stale and stale:
        cleaned = []
        for r in records:
            m1 = r.get("market_1")
            m2 = r.get("market_2")
            if m1 in stale or m2 in stale:
                log_event({
                    "type": "stale_json_record_dropped",
                    "market_1": m1,
                    "market_2": m2,
                    "status": r.get("pair_status"),
                })
                continue
            cleaned.append(r)
        save_json_list(cleaned)
        records = cleaned

    real_pair_slots = int((len(live_markets) + 1) / 2)
    block_reason = None
    if BLOCK_ENTRIES_ON_UNMANAGED_EXPOSURE and unmanaged:
        block_reason = f"unmanaged dYdX exposure: {unmanaged}"
    elif BLOCK_ENTRIES_ON_ORPHAN_RECORDS and orphan_records:
        block_reason = f"ORPHAN records present: {len(orphan_records)}"
    elif len(live_markets) >= MAX_REAL_OPEN_MARKETS:
        block_reason = f"real live markets cap reached: {len(live_markets)}/{MAX_REAL_OPEN_MARKETS}"
    elif real_pair_slots >= int(MAX_OPEN_TRADES):
        block_reason = f"real pair slot cap reached: {real_pair_slots}/{MAX_OPEN_TRADES}"

    out = {
        "markets": markets,
        "live_positions": live,
        "live_markets": sorted(live_markets),
        "json_records": records,
        "expected_markets": sorted(expected_markets),
        "unmanaged_markets": unmanaged,
        "stale_markets": stale,
        "orphan_count": len(orphan_records),
        "real_pair_slots": real_pair_slots,
        "block_entries": block_reason is not None,
        "block_reason": block_reason,
    }

    log_event({
        "type": "reconcile_state",
        "live_markets": out["live_markets"],
        "expected_markets": out["expected_markets"],
        "unmanaged_markets": unmanaged,
        "stale_markets": stale,
        "orphan_count": len(orphan_records),
        "real_pair_slots": real_pair_slots,
        "block_entries": out["block_entries"],
        "block_reason": block_reason,
    })
    return out


async def close_market_actual(node, indexer, wallet, market, reason="cleanup"):
    markets = await get_markets(indexer)
    live = await get_live_positions(indexer, markets=markets, min_usd=0.0)
    pos = live.get(market)
    if not pos or abs(_sf(pos.get("size"))) <= 0:
        return {"market": market, "closed": False, "reason": "no_live_position"}

    size = _sf(pos["size"])
    close_side = "SELL" if size > 0 else "BUY"
    close_size = abs(size)
    step = markets.get(market, {}).get("stepSize")
    if step:
        close_size = float(format_number(close_size, step))

    px = _sf(markets.get(market, {}).get("oraclePrice"), 0.0)
    log_event({
        "type": "cleanup_close_sent",
        "market": market,
        "side": close_side,
        "size": close_size,
        "reason": reason,
    })

    res = await place_market_order(
        node=node,
        indexer=indexer,
        wallet=wallet,
        market=market,
        side=close_side,
        size=close_size,
        price=px,
        reduce_only=True,
        time_in_force_type=Order.TimeInForce.TIME_IN_FORCE_IOC,
    )
    cid = (res or {}).get("order", {}).get("id")
    audit = None
    if cid:
        # max_fill_lookback is the correct param name (not max_retries)
        audit = await get_real_fill_details(indexer, cid, market, max_fill_lookback=200)

    return {"market": market, "closed": True, "client_id": cid, "audit": audit}


async def close_markets_actual(node, indexer, wallet, markets_to_close, reason="cleanup"):
    results = []
    for market in sorted(set(markets_to_close or [])):
        try:
            results.append(await close_market_actual(node, indexer, wallet, market, reason=reason))
            await asyncio.sleep(0.5)
        except Exception as e:
            log_event({"type": "cleanup_close_error", "market": market, "reason": reason, "error": str(e)})
            results.append({"market": market, "closed": False, "error": str(e)})
    return results


async def assert_safe_to_open(indexer):
    global _last_entry_blocked_alert
    state = await reconcile_bot_vs_dydx(indexer, drop_stale=True)
    if state["block_entries"]:
        msg = f"[ENTRY BLOCKED] {state['block_reason']}"
        print(msg)
        # Only send Telegram alert once every ENTRY_BLOCKED_ALERT_COOLDOWN_S seconds
        # to avoid flooding when the loop runs every 30s and entries stay blocked.
        now = datetime.now(timezone.utc).timestamp()
        if now - _last_entry_blocked_alert >= ENTRY_BLOCKED_ALERT_COOLDOWN_S:
            send_message(msg)
            _last_entry_blocked_alert = now
        return False, state
    # Reset cooldown timer when entries are no longer blocked
    _last_entry_blocked_alert = 0.0
    return True, state
