from constants import ZSCORE_THRESH, USD_PER_TRADE, USD_MIN_COLLATERAL
from func_utils import format_number
from func_public import get_candles_recent
from func_cointegration import calculate_zscore
from func_private import is_open_positions
from constants import WALLET_ADDRESS
from func_bot_agent import BotAgent
import pandas as pd
import json

from pprint import pprint


# Open positions
async def open_positions(node, indexer, wallet):
    """
      Manage finding triggers for trade entry
      Store trades for managing later on on exit function
    """

    # Load cointegrated pairs
    df = pd.read_csv("cointegrated_pairs.csv")

    # Get markets from referencing of min order size, tick size etc
    markets_response = await indexer.markets.get_perpetual_markets()
    markets = markets_response.get("markets", {})

    # Initialize container for BotAgent results
    bot_agents = []

        # Opening JSON file
        #try:
            #open_positions_file = open("bot_agents.json")
            #open_positions_dict = json.load(open_positions_file)
            #for p in open_positions_dict:
                #bot_agents.append(p)
        #except:
            #bot_agents = []

    for index, row in df.iterrows():

        # Extract variables
        base_market = row["base_market"]
        quote_market = row["quote_market"]
        hedge_ratio = float(row["hedge_ratio"])
        half_life = row["half_life"]

        if base_market not in markets or quote_market not in markets:
            continue

        def get_safe_min_size(m_name):
            m_data = markets.get(m_name, {})
            # Intentamos minOrderSize, si no existe usamos stepSize
            val = m_data.get("minOrderSize") or m_data.get("stepSize")
            return float(val) if val else 0.0

        try:
            base_price = float(markets[base_market]["oraclePrice"])
            quote_price = float(markets[quote_market]["oraclePrice"])

            min_base = get_safe_min_size(base_market)
            min_quote = get_safe_min_size(quote_market)

            base_step = markets[base_market]["stepSize"]
            quote_step = markets[quote_market]["stepSize"]
        except Exception as e:
            continue

        # Get prices
        series_1 = await get_candles_recent(indexer, base_market)
        series_2 = await get_candles_recent(indexer, quote_market)

        # Get ZScore
        if len(series_1) > 0 and len(series_1) == len(series_2):
            spread = series_1 - (hedge_ratio * series_2)
            z_score = calculate_zscore(spread).values.tolist()[-1]

            # Establish if potential trade
            if abs(z_score) >= ZSCORE_THRESH:

                # Ensure like-for-like not already open (diversify trading)
                is_base_open = await is_open_positions(indexer, base_market)
                is_quote_open = await is_open_positions(indexer, quote_market)

                # Place trade
                if not is_base_open and not is_quote_open:

                    # Determine side
                    base_side = "BUY" if z_score < 0 else "SELL"
                    quote_side = "BUY" if z_score > 0 else "SELL"

                    # Get acceptable price in string format with correct number of decimals

                    """
                    base_price = series_1[-1]
                    quote_price = series_2[-1]
                    accept_base_price = float(base_price) * 1.01 if z_score < 0 else float(base_price) * 0.99
                    accept_quote_price = float(quote_price) * 1.01 if z_score > 0 else float(quote_price) * 0.99
                    failsafe_base_price = float(base_price) * 0.05 if z_score < 0 else float(base_price) * 1.7
                    base_tick_size = markets[base_market]["tickSize"]
                    quote_tick_size = markets[quote_market]["tickSize"]

                    # Format prices
                    accept_base_price = format_number(accept_base_price, base_tick_size)
                    accept_quote_price = format_number(accept_quote_price, quote_tick_size)
                    accept_failsafe_base_price = format_number(failsafe_base_price, base_tick_size)

                    # Get size
                    base_quantity = 1 / base_price * USD_PER_TRADE
                    quote_quantity = 1 / quote_price * USD_PER_TRADE
                    
                
                    base_step_size = markets[base_market]["stepSize"]
                    quote_step_size = markets[quote_market]["stepSize"]

                    # Format sizes
                    base_size = format_number(base_quantity, base_step_size)
                    quote_size = format_number(quote_quantity, quote_step_size)

                    # Ensure size
                    base_min_order_size = markets[base_market].get("minSize")
                    quote_min_order_size = markets[quote_market].get("minSize")

                    def get_min_size(market_data):
                        return (market_data.get("minOrderSize") or
                                market_data.get("minSize") or
                                market_data.get("clobPair", {}).get("minOrderSize") or
                                market_data.get("stepSize"))

                    base_min_order_size = get_min_size(markets[base_market])
                    quote_min_order_size = get_min_size(markets[quote_market])

                    if base_min_order_size is None or quote_min_order_size is None:
                        print(f"Skipping {base_market}/{quote_market}: No min size detected.")
                        continue

                    check_base = float(base_quantity) > float(base_min_order_size)
                    check_quote = float(quote_quantity) > float(quote_min_order_size)

                    # If checks pass, place trades
                    if check_base and check_quote:

                        # Check account balance
                        account_resp = await indexer.account.get_subaccount(WALLET_ADDRESS, 0)
                        subaccount = account_resp.get("subaccount")

                        if subaccount:
                            free_collateral = float(subaccount.get("freeCollateral", 0))
                            print(f"Balance: {free_collateral} | Mín required: {USD_MIN_COLLATERAL}")
                        else:
                            print("Error obtaining account balance.")
                            continue


                        # Guard: Ensure collateral
                        if free_collateral < USD_MIN_COLLATERAL:
                            break
                    """

                    base_quantity = USD_PER_TRADE / base_price
                    #quote_quantity = (USD_PER_TRADE * hedge_ratio) / quote_price
                    quote_quantity = base_quantity*hedge_ratio

                    base_size_fmt = format_number(base_quantity, base_step)
                    quote_size_fmt = format_number(quote_quantity, quote_step)

                    # --- DEBUG DE MATEMÁTICAS (Para ver por qué no cuadran los montos) ---
                    print(f"\n📐 MATH DEBUG [{base_market} / {quote_market}]")
                    print(f"   Precios: {base_market}=${base_price} | {quote_market}=${quote_price}")
                    print(f"   Hedge Ratio: {hedge_ratio}")
                    print(f"   Base Qty: {base_quantity} -> Formateado: {base_size_fmt} (Step: {base_step})")
                    print(f"   Quote Qty: {quote_quantity} -> Formateado: {quote_size_fmt} (Step: {quote_step})")
                    # -------------------------------------------------------------------

                    if base_side == "BUY":
                        failsafe_p = base_price * 1.02
                    else:
                        failsafe_p = base_price * 0.98

                    accept_failsafe_base_price = format_number(failsafe_p, markets[base_market]["tickSize"])

                    if float(base_size_fmt) < min_base or float(quote_size_fmt) < min_quote:
                        print(f"Saltando {base_market}/{quote_market}: Tamaño muy pequeño para el mínimo del exchange.")
                        continue

                    print(f"Opening trade for {base_market} and {quote_market}...")

                        # Create Bot Agent
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

                    # Open Trades
                    bot_open_dict = await bot_agent.open_trades(wallet)

                    if bot_open_dict != "failed" and bot_open_dict.get("pair_status") == "LIVE":
                        bot_agents.append(bot_open_dict)
                        print(f"Trade LIVE: {base_market} {base_side} @ {base_price} & {quote_market} {quote_side} @ {quote_price}")
                    else:
                        reason = bot_open_dict.get("comments", "Unknown Error")
                        print(f"⚠️ Trade FAILED for {base_market}/{quote_market}. Reason: {reason}")

        # Save agents
        #print(f"Success: Manage open trades checked.")
    if len(bot_agents) > 0:
        with open("bot_agents.json", "w") as f:
            json.dump(bot_agents, f)
        print(f"Success: {len(bot_agents)} new pairs executed.")
