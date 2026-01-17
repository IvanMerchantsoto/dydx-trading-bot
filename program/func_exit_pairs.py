from constants import CLOSE_AT_ZSCORE_CROSS, WALLET_ADDRESS
from func_utils import format_number
from func_public import get_candles_recent
from func_cointegration import calculate_zscore
from func_private import place_market_order
from v4_proto.dydxprotocol.clob.order_pb2 import Order
import json
import asyncio

from pprint import pprint

# Manage trade exits
async def manage_trade_exits(node, indexer, wallet):

  """
    Manage exiting open positions
    Based upon criteria set in constants
  """

  # -----------------------------
  # Load saved open trades
  # -----------------------------
  try:
      with open("bot_agents.json", "r") as f:
          open_positions_dict = json.load(f)
  except Exception:
      return "complete"

  if not open_positions_dict:
      return "complete"

  # -----------------------------
  # Pull market metadata once (tickSize, oraclePrice)
  # -----------------------------
  markets_resp = await indexer.markets.get_perpetual_markets()
  markets = markets_resp.get("markets", {})

  # -----------------------------
  # Pull live open positions once
  # -----------------------------
  try:
      account_resp = await indexer.account.get_subaccount(WALLET_ADDRESS, 0)
      subaccount = account_resp.get("subaccount", {})
      positions = subaccount.get("openPerpetualPositions", {}) or subaccount.get("perpetualPositions", {})
  except Exception as e:
      print(f"Error reading subaccount positions: {e}")
      return "error"

  # Only markets with non-zero size
  live_pos = {}
  for m, pdata in (positions or {}).items():
      try:
          sz = float(pdata.get("size", 0))
      except Exception:
          sz = 0.0
      if abs(sz) > 0:
          live_pos[m] = sz

  save_output = []

  # -----------------------------
  # Loop saved pairs
  # -----------------------------

  for position in open_positions_dict:
      is_close = False

      m1 = position.get("market_1")
      m2 = position.get("market_2")

      if not m1 or not m2:
          save_output.append(position)
          continue

      # Guard: both markets still open on exchange
      if m1 not in live_pos or m2 not in live_pos:
          print(f"Warning: One leg not live anymore for {m1}/{m2}. Keeping record for review.")
          save_output.append(position)
          continue

      # -----------------------------
      # Z-score close logic
      # -----------------------------
      if CLOSE_AT_ZSCORE_CROSS:
          hedge_ratio = float(position.get("hedge_ratio", 0))
          z_score_traded = float(position.get("z_score", 0))

          series_1 = await get_candles_recent(indexer, m1)
          series_2 = await get_candles_recent(indexer, m2)

          if len(series_1) > 0 and len(series_1) == len(series_2):
              spread = series_1 - (hedge_ratio * series_2)
              z_score_current = float(calculate_zscore(spread).values.tolist()[-1])

              z_score_level_check = abs(z_score_current) >= abs(z_score_traded)
              z_score_cross_check = (
                      (z_score_current < 0 and z_score_traded > 0) or
                      (z_score_current > 0 and z_score_traded < 0)
              )

              if z_score_level_check and z_score_cross_check:
                  is_close = True

      # -----------------------------
      # If not closing, keep record
      # -----------------------------
      if not is_close:
          save_output.append(position)
          continue

      # -----------------------------
      # Close both legs (reduce-only)
      # We close using ACTUAL current position size from live_pos (more reliable than stored order size).
      # -----------------------------
      try:
          # Leg 1
          size_m1 = float(live_pos[m1])
          close_side_m1 = "SELL" if size_m1 > 0 else "BUY"  # long -> sell, short -> buy
          close_size_m1 = abs(size_m1)

          # Leg 2
          size_m2 = float(live_pos[m2])
          close_side_m2 = "SELL" if size_m2 > 0 else "BUY"
          close_size_m2 = abs(size_m2)

          # Optional: format sizes to stepSize (safer)
          step_m1 = markets.get(m1, {}).get("stepSize")
          step_m2 = markets.get(m2, {}).get("stepSize")
          if step_m1:
              close_size_m1 = float(format_number(close_size_m1, step_m1))
          if step_m2:
              close_size_m2 = float(format_number(close_size_m2, step_m2))

          print(f"\n>>> Closing pair: {m1} & {m2}")
          print(f"Leg1 close: {close_side_m1} {m1} size={close_size_m1}")
          print(f"Leg2 close: {close_side_m2} {m2} size={close_size_m2}")

          # NOTE: Your current place_market_order uses oraclePrice internally for execution price (failsafe),
          # and enforces IOC + reduce_only. :contentReference[oaicite:7]{index=7} :contentReference[oaicite:8]{index=8}
          await place_market_order(
              node, indexer, wallet,
              m1, close_side_m1, close_size_m1, markets[m1]["oraclePrice"],
              True,  # reduce_only
              time_in_force_type=Order.TimeInForce.TIME_IN_FORCE_IOC
          )

          # small spacing to avoid rate limits
          await asyncio.sleep(0.5)

          await place_market_order(
              node, indexer, wallet,
              m2, close_side_m2, close_size_m2, markets[m2]["oraclePrice"],
              True,  # reduce_only
              time_in_force_type=Order.TimeInForce.TIME_IN_FORCE_IOC
          )

          print(">>> Closed both legs (reduce-only).")

      except Exception as e:
          print(f"Exit failed for {m1} with {m2}: {e}")
          save_output.append(position)

      # -----------------------------
      # Save remaining items
      # -----------------------------
  print(f"{len(save_output)} Items remaining. Saving file...")
  with open("bot_agents.json", "w") as f:
      json.dump(save_output, f)

  return "complete"