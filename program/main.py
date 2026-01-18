import asyncio

from func_connections import connect_dydx
from func_private import abort_all_positions
from func_public import construct_market_prices
from func_entry_pairs import open_positions
from func_cointegration import store_cointegration_results
from func_exit_pairs import manage_trade_exits
from func_kpis import send_account_kpis
from func_risk_off import risk_off_close_worst_pair
from func_messaging import send_message

from constants import (
    ABORT_ALL_POSITIONS,
    FIND_COINTEGRATED,
    PLACE_TRADES,
    MANAGE_EXITS,
    EXIT_CHECK_SECONDS,
    KPI_SECONDS,
    BATCH_OPEN_TRADES,
    MAX_OPEN_TRADES,
    RISK_OFF_ENABLED,
    RISK_OFF_FREE_COLLATERAL_TRIGGER,
    RISK_OFF_FORCE_IF_OPEN_TRADES_GE,
)

async def _count_open_pairs_from_json():
    import json
    try:
        with open("bot_agents.json", "r") as f:
            data = json.load(f)
        return len(data) if data else 0
    except Exception:
        return 0

async def main():

    send_message("Bot Launched Successfully")

    # Connect
    try:
        print("Connecting to client...")
        node, indexer, wallet = await connect_dydx()
    except Exception as e:
        print("Error connecting to client: ", e)
        send_message("Failed to connect to DYDX.")
        raise

    # Abort open positions
    if ABORT_ALL_POSITIONS:
        try:
            print("Closing all positions...")
            await abort_all_positions(node, indexer)
        except Exception as e:
            print("Error closing all positions: ", e)
            send_message("Failed to abort all positions.")
            raise

    # Find cointegrated
    if FIND_COINTEGRATED:
        try:
            print("Fetching market prices, please allow 3 minutes...")
            df_market_prices = await construct_market_prices(node, indexer)
            print("Storing cointegrated pairs...")
            stores_result = store_cointegration_results(df_market_prices)
            if stores_result != "saved":
                raise RuntimeError("Error saving cointegrated pairs.")
        except Exception as e:
            print("Error cointegrating pairs: ", e)
            send_message("Failed to cointegrate pairs.")
            raise

    opened_since_exit = 0
    last_kpi_ts = 0.0

    loop = asyncio.get_event_loop()

    while True:
        now = loop.time()

        # 1) Exits periódicos (siempre)
        if MANAGE_EXITS:
            await manage_trade_exits(node, indexer, wallet)

        # 2) KPIs cada KPI_SECONDS
        if now - last_kpi_ts >= KPI_SECONDS:
            snapshot = await send_account_kpis(indexer)
            last_kpi_ts = now

            # Trigger risk-off por collateral bajo
            if RISK_OFF_ENABLED and snapshot and snapshot.get("free", 0) < float(RISK_OFF_FREE_COLLATERAL_TRIGGER):
                send_message("⚠️ Free collateral low -> Risk-off")
                await risk_off_close_worst_pair(node, indexer, wallet)

        # 3) Batch logic: abre hasta BATCH_OPEN_TRADES, luego fuerza exits
        if PLACE_TRADES:
            open_pairs = await _count_open_pairs_from_json()

            # Hard cap global
            if open_pairs >= MAX_OPEN_TRADES:
                if RISK_OFF_ENABLED and open_pairs >= RISK_OFF_FORCE_IF_OPEN_TRADES_GE:
                    send_message("⚠️ Max trades reached -> Risk-off")
                    await risk_off_close_worst_pair(node, indexer, wallet)
            else:
                remaining_in_batch = max(0, BATCH_OPEN_TRADES - opened_since_exit)
                if remaining_in_batch > 0:
                    print("Finding trading opportunities...")
                    opened_now = await open_positions(node, indexer, wallet, max_new_trades=remaining_in_batch)
                    opened_since_exit += int(opened_now or 0)

                # Si ya completaste batch, fuerza un ciclo extra de exits + risk-off pro opcional
                if opened_since_exit >= BATCH_OPEN_TRADES:
                    send_message(f"🔁 Batch complete ({opened_since_exit}). Managing exits...")
                    if MANAGE_EXITS:
                        await manage_trade_exits(node, indexer, wallet)
                    opened_since_exit = 0

                    # Paso pro: si estás estresado (muchos trades), mata el peor 1 vez
                    open_pairs2 = await _count_open_pairs_from_json()
                    if RISK_OFF_ENABLED and open_pairs2 >= RISK_OFF_FORCE_IF_OPEN_TRADES_GE:
                        send_message("🧹 Portfolio uploaded after batch -> Risk-off worst pair")
                        await risk_off_close_worst_pair(node, indexer, wallet)

        await asyncio.sleep(EXIT_CHECK_SECONDS)

if __name__ == "__main__":
    asyncio.run(main())
