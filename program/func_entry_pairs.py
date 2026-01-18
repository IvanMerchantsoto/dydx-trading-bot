from func_messaging import send_message
from constants import ZSCORE_THRESH, USD_PER_TRADE, USD_MIN_COLLATERAL, MAX_OPEN_TRADES
from func_utils import format_number
from func_public import get_candles_recent
from func_cointegration import calculate_zscore
from datetime import datetime, timezone
from func_private import is_open_positions
from constants import WALLET_ADDRESS
from func_bot_agent import BotAgent
import pandas as pd
import json
import os

from pprint import pprint

JSON_PATH = os.path.join(os.path.dirname(__file__), "bot_agents.json")

def _sf(x, d=0.0):
    try:
        return float(x)
    except Exception:
        return d
async def _get_subaccount(indexer):
    resp = await indexer.account.get_subaccount(WALLET_ADDRESS, 0)
    return resp.get("subaccount", {}) or {}

async def _get_live_markets_with_position(indexer):
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

# Open positions
async def open_positions(node, indexer, wallet, max_new_trades=1):
    """
      Manage finding triggers for trade entry
      Store trades for managing later on on exit function
    """
    opened_count = 0

    # 1) Hard cap from JSON (pairs)
    open_pairs = _count_open_pairs_from_json()
    if open_pairs >= MAX_OPEN_TRADES:
        print(f"⛔ MAX_OPEN_TRADES reached ({open_pairs}/{MAX_OPEN_TRADES}).")
        send_message(f"⛔ MAX_OPEN_TRADES reached ({open_pairs}/{MAX_OPEN_TRADES}).")
        return 0

        # 2) Optional collateral gate (best-effort)
    try:
        sub = await _get_subaccount(indexer)
        free_collateral = _sf(sub.get("freeCollateral"))
        if free_collateral and free_collateral < float(USD_MIN_COLLATERAL):
            print(
                f"⛔ Free collateral ${free_collateral:,.2f} < USD_MIN_COLLATERAL ${USD_MIN_COLLATERAL:,.2f}.")
            send_message(f"⛔ Low collateral. Free ${free_collateral:,.0f} < Min ${USD_MIN_COLLATERAL:,.0f}.")
            return 0
    except Exception:
        # If we can't read collateral, don't block trading (or you can choose to block)
        pass

    # 3) Live markets with a position -> avoid duplicating legs
    live_markets = await _get_live_markets_with_position(indexer)

    # 4) Load cointegrated pairs
    df = pd.read_csv("cointegrated_pairs.csv")

    # 5) Market metadata
    markets_response = await indexer.markets.get_perpetual_markets()
    markets = markets_response.get("markets", {}) or {}

    # 6) Load existing bot agents (open pairs state)
    bot_agents = _load_open_pairs_from_json()
    # pprint(bot_agents)

    def get_safe_min_size(m_name):
        m_data = markets.get(m_name, {})
        val = m_data.get("minOrderSize") or m_data.get("stepSize")
        return float(val) if val else 0.0

    for _, row in df.iterrows():

        # Stop if we already opened enough for this call (batch)
        if opened_count >= max_new_trades:
            break

        # Stop if we hit hard cap
        if open_pairs >= MAX_OPEN_TRADES:
            break

        base_market = row["base_market"]
        quote_market = row["quote_market"]
        hedge_ratio = float(row["hedge_ratio"])
        half_life = row.get("half_life")

        if base_market not in markets or quote_market not in markets:
            continue

        # Skip if either leg already has a live position
        if base_market in live_markets or quote_market in live_markets:
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
            continue

        # Get candles
        series_1 = await get_candles_recent(indexer, base_market)
        series_2 = await get_candles_recent(indexer, quote_market)

        if not (len(series_1) > 0 and len(series_1) == len(series_2)):
            continue

        spread = series_1 - (hedge_ratio * series_2)
        z_score = calculate_zscore(spread).values.tolist()[-1]

        # Trigger
        if abs(z_score) < ZSCORE_THRESH:
            continue

        # Determine side
        base_side = "BUY" if z_score < 0 else "SELL"
        quote_side = "BUY" if z_score > 0 else "SELL"

        # Sizes (USD-neutral)
        base_quantity = USD_PER_TRADE / base_price
        quote_quantity = USD_PER_TRADE / quote_price

        base_size_fmt = format_number(base_quantity, base_step)
        quote_size_fmt = format_number(quote_quantity, quote_step)

        # Failsafe base price
        failsafe_p = base_price * (1.02 if base_side == "BUY" else 0.98)
        accept_failsafe_base_price = format_number(failsafe_p, tick_base)

        # Min size guard
        if float(base_size_fmt) < min_base or float(quote_size_fmt) < min_quote:
            print(f"Skipping {base_market}/{quote_market}: Size too low for exchange.")
            continue

        print(f"\n\nOpening trade for {base_market} and {quote_market}...")

        bot_agent = BotAgent(
            node,
            indexer,
            market_1=base_market,
            market_2=quote_market,
            base_side=base_side,
            base_size=base_size_fmt,
            base_price=base_price,
            quote_side=quote_side,
            quote_size=quote_size_fmt,
            quote_price=quote_price,
            accept_failsafe_base_price=accept_failsafe_base_price,
            z_score=z_score,
            half_life=half_life,
            hedge_ratio=hedge_ratio,
        )

        bot_open_dict = await bot_agent.open_trades(wallet)

        if bot_open_dict != "failed" and isinstance(bot_open_dict, dict) and bot_open_dict.get("pair_status") == "LIVE":
            # Enrich trade record for exits + PnL + time stop
            now_iso = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            bot_open_dict["opened_at"] = bot_open_dict.get("opened_at") or now_iso

            # Best-effort normalize fields expected by manage_trade_exits for PnL
            # If BotAgent already returns these, we keep them.
            bot_open_dict["side_1"] = bot_open_dict.get("side_1") or base_side
            bot_open_dict["side_2"] = bot_open_dict.get("side_2") or quote_side
            bot_open_dict["price_1"] = bot_open_dict.get("price_1") or base_price
            bot_open_dict["price_2"] = bot_open_dict.get("price_2") or quote_price

            # Sizes: prefer real fills if BotAgent provides them, else intended sizes
            bot_open_dict["size_1"] = bot_open_dict.get("size_1") or _sf(bot_open_dict.get("filled_size_1"),
                                                                         _sf(base_size_fmt))
            bot_open_dict["size_2"] = bot_open_dict.get("size_2") or _sf(bot_open_dict.get("filled_size_2"),
                                                                         _sf(quote_size_fmt))

            bot_agents.append(bot_open_dict)

            with open(JSON_PATH, "w") as f:
                json.dump(bot_agents, f, indent=2)

            opened_count += 1
            open_pairs += 1

            # Mark legs as live to prevent reusing them in same call
            live_markets.add(base_market)
            live_markets.add(quote_market)

            print(f"Saved JSON -> {JSON_PATH} ({len(bot_agents)} items)")
            print(f"Trade LIVE: {base_market} {base_side} @ {base_price} & {quote_market} {quote_side} @ {quote_price}")
            send_message(f"Trade LIVE: {base_market} {base_side} / {quote_market} {quote_side}")

        else:
            reason = bot_open_dict.get("comments", "Unknown Error") if isinstance(bot_open_dict,
                                                                                  dict) else "Unknown Error"
            print(f"⚠️ Trade FAILED for {base_market}/{quote_market}. Reason: {reason}")

    print("Success: Manage open trades checked.")
    if opened_count > 0:
        print(f"Success: {opened_count} new pairs executed (this batch call).")

    return opened_count