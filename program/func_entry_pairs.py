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

                    base_quantity = USD_PER_TRADE / base_price
                    quote_quantity = USD_PER_TRADE / quote_price

                    base_size_fmt = format_number(base_quantity, base_step)
                    quote_size_fmt = format_number(quote_quantity, quote_step)


                    if base_side == "BUY":
                        failsafe_p = base_price * 1.02
                    else:
                        failsafe_p = base_price * 0.98

                    accept_failsafe_base_price = format_number(failsafe_p, markets[base_market]["tickSize"])

                    if float(base_size_fmt) < min_base or float(quote_size_fmt) < min_quote:
                        print(f"Saltando {base_market}/{quote_market}: Tamaño muy pequeño para el mínimo del exchange.")
                        continue

                    print(f"\n\nOpening trade for {base_market} and {quote_market}...")

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
