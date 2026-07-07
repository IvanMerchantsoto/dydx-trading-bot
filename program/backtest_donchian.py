#!/usr/bin/env python3
"""
backtest_donchian.py — Backtest de estrategia Donchian Channel Breakout (Turtle-style)

Reglas:
  Entry LONG:  close > máximo(N_entry) [default 20 días]
  Entry SHORT: close < mínimo(N_entry)
  Exit LONG:   close < mínimo(N_exit)  [default 10 días, trailing stop]
  Exit SHORT:  close > máximo(N_exit)

Position sizing:
  ATR (Average True Range) determines position size.
  Risk per trade = 1% of equity.
  Position = risk / (2 × ATR)  (2 × ATR ≈ stop distance)

Universe: BTC-USD, ETH-USD, SOL-USD, LINK-USD, ARB-USD

Uso:
    cd ~/DYDX/program
    source ~/DYDX/.venv/bin/activate
    python3 backtest_donchian.py
"""

import asyncio
import sys
import os
from pathlib import Path
from datetime import datetime, timedelta, timezone

SCRIPT_DIR = Path(__file__).parent
sys.path.insert(0, str(SCRIPT_DIR))

import pandas as pd
import numpy as np
from func_connections import connect_dydx

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
UNIVERSE = ["BTC-USD", "ETH-USD", "SOL-USD", "LINK-USD", "ARB-USD"]
N_ENTRY = 20        # días para breakout de entrada
N_EXIT = 10         # días para trailing stop
ATR_PERIOD = 14     # días para ATR
RISK_PER_TRADE_PCT = 0.01   # 1% del equity
MAX_UNITS_PER_MARKET = 4    # pyramiding max
TAKER_FEE_BPS = 0.0005      # 0.05%
SLIPPAGE_BPS = 0.0010       # 10 bps por trade (conservador)
INITIAL_EQUITY = 300.0
BACKTEST_DAYS = 180         # ~6 meses
RESOLUTION = "1DAY"         # daily bars (Turtle usaba daily)


async def fetch_candles_range(indexer, market: str, days_back: int):
    """Fetch daily candles for the last N days. Handles pagination."""
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days_back)

    all_candles = []
    current_end = end

    while current_end > start:
        current_start = max(start, current_end - timedelta(days=100))
        try:
            resp = await indexer.markets.get_perpetual_market_candles(
                market=market,
                resolution=RESOLUTION,
                from_iso=current_start.isoformat().replace("+00:00", "Z"),
                to_iso=current_end.isoformat().replace("+00:00", "Z"),
                limit=100,
            )
            batch = resp.get("candles", []) if isinstance(resp, dict) else []
            if not batch:
                break
            all_candles.extend(batch)
            # Move backwards
            oldest = min(batch, key=lambda c: c.get("startedAt", ""))
            oldest_dt = datetime.fromisoformat(oldest["startedAt"].replace("Z", "+00:00"))
            if oldest_dt <= start:
                break
            current_end = oldest_dt - timedelta(seconds=1)
        except Exception as e:
            print(f"  ⚠️ {market}: fetch error at {current_end}: {e}")
            break

    if not all_candles:
        return None

    # Convert to DataFrame
    df = pd.DataFrame(all_candles)
    df["datetime"] = pd.to_datetime(df["startedAt"])
    df["high"] = df["high"].astype(float)
    df["low"] = df["low"].astype(float)
    df["close"] = df["close"].astype(float)
    df["open"] = df["open"].astype(float)
    df = df.sort_values("datetime").drop_duplicates("datetime").reset_index(drop=True)
    return df[["datetime", "open", "high", "low", "close"]]


def calculate_atr(df, period=14):
    """Calculate Average True Range."""
    high = df["high"]
    low = df["low"]
    close_prev = df["close"].shift(1)
    tr = pd.concat([
        high - low,
        (high - close_prev).abs(),
        (low - close_prev).abs(),
    ], axis=1).max(axis=1)
    return tr.rolling(period).mean()


def backtest_single_market(df, market, initial_equity):
    """Backtest Donchian on a single market. Returns trade list and final equity."""
    if len(df) < N_ENTRY + N_EXIT + ATR_PERIOD:
        return [], initial_equity, {}

    # Precompute indicators
    df = df.copy()
    df["high_n"] = df["high"].rolling(N_ENTRY).max().shift(1)   # previous day's N-day high
    df["low_n"] = df["low"].rolling(N_ENTRY).min().shift(1)
    df["high_exit"] = df["high"].rolling(N_EXIT).max().shift(1)
    df["low_exit"] = df["low"].rolling(N_EXIT).min().shift(1)
    df["atr"] = calculate_atr(df, ATR_PERIOD)

    trades = []
    equity = initial_equity
    position = None  # {"side": "LONG"|"SHORT", "entry_price": X, "entry_date": Y, "size": Z, "notional": W}

    for i in range(N_ENTRY + N_EXIT + ATR_PERIOD, len(df)):
        row = df.iloc[i]
        close = row["close"]
        atr = row["atr"]
        if pd.isna(atr) or atr <= 0:
            continue

        # Check EXIT first (if position open)
        if position is not None:
            exit_signal = False
            exit_reason = None
            if position["side"] == "LONG" and close < row["low_exit"]:
                exit_signal = True
                exit_reason = f"trailing_stop_low_{N_EXIT}d"
            elif position["side"] == "SHORT" and close > row["high_exit"]:
                exit_signal = True
                exit_reason = f"trailing_stop_high_{N_EXIT}d"

            if exit_signal:
                # Close the position
                exit_price = close * (1 - SLIPPAGE_BPS) if position["side"] == "LONG" else close * (1 + SLIPPAGE_BPS)
                gross_pnl = (exit_price - position["entry_price"]) * position["size"]
                if position["side"] == "SHORT":
                    gross_pnl = -gross_pnl
                fees = position["notional"] * TAKER_FEE_BPS * 2  # open + close
                net_pnl = gross_pnl - fees
                equity += net_pnl
                trades.append({
                    "market": market,
                    "side": position["side"],
                    "entry_date": position["entry_date"],
                    "exit_date": row["datetime"],
                    "entry_price": position["entry_price"],
                    "exit_price": exit_price,
                    "size": position["size"],
                    "notional": position["notional"],
                    "gross_pnl": gross_pnl,
                    "fees": fees,
                    "net_pnl": net_pnl,
                    "exit_reason": exit_reason,
                    "hold_days": (row["datetime"] - position["entry_date"]).days,
                    "atr_at_entry": position["atr_at_entry"],
                })
                position = None

        # Check ENTRY (only if no position open)
        if position is None:
            # LONG signal: close > 20-day high
            if close > row["high_n"] and not pd.isna(row["high_n"]):
                risk = equity * RISK_PER_TRADE_PCT
                stop_distance = 2.0 * atr
                size = risk / stop_distance
                notional = size * close
                # Cap notional at 25% of equity to avoid over-leverage
                if notional > equity * 0.25:
                    notional = equity * 0.25
                    size = notional / close
                entry_price = close * (1 + SLIPPAGE_BPS)
                position = {
                    "side": "LONG",
                    "entry_price": entry_price,
                    "entry_date": row["datetime"],
                    "size": size,
                    "notional": notional,
                    "atr_at_entry": atr,
                }
            # SHORT signal: close < 20-day low
            elif close < row["low_n"] and not pd.isna(row["low_n"]):
                risk = equity * RISK_PER_TRADE_PCT
                stop_distance = 2.0 * atr
                size = risk / stop_distance
                notional = size * close
                if notional > equity * 0.25:
                    notional = equity * 0.25
                    size = notional / close
                entry_price = close * (1 - SLIPPAGE_BPS)
                position = {
                    "side": "SHORT",
                    "entry_price": entry_price,
                    "entry_date": row["datetime"],
                    "size": size,
                    "notional": notional,
                    "atr_at_entry": atr,
                }

    # If position still open at end, close it at last price (mark-to-market)
    if position is not None:
        last_close = df.iloc[-1]["close"]
        exit_price = last_close * (1 - SLIPPAGE_BPS) if position["side"] == "LONG" else last_close * (1 + SLIPPAGE_BPS)
        gross_pnl = (exit_price - position["entry_price"]) * position["size"]
        if position["side"] == "SHORT":
            gross_pnl = -gross_pnl
        fees = position["notional"] * TAKER_FEE_BPS * 2
        net_pnl = gross_pnl - fees
        equity += net_pnl
        trades.append({
            "market": market,
            "side": position["side"],
            "entry_date": position["entry_date"],
            "exit_date": df.iloc[-1]["datetime"],
            "entry_price": position["entry_price"],
            "exit_price": exit_price,
            "size": position["size"],
            "notional": position["notional"],
            "gross_pnl": gross_pnl,
            "fees": fees,
            "net_pnl": net_pnl,
            "exit_reason": "end_of_backtest",
            "hold_days": (df.iloc[-1]["datetime"] - position["entry_date"]).days,
            "atr_at_entry": position["atr_at_entry"],
        })

    stats = {
        "n_trades": len(trades),
        "final_equity": equity,
        "return_pct": (equity - initial_equity) / initial_equity * 100,
    }
    return trades, equity, stats


def summarize_trades(all_trades, initial_equity_total):
    """Print summary statistics of all trades."""
    if not all_trades:
        print("No trades executed in backtest window.")
        return

    df = pd.DataFrame(all_trades)
    total_pnl = df["net_pnl"].sum()
    n = len(df)
    wins = df[df["net_pnl"] > 0]
    losses = df[df["net_pnl"] <= 0]
    winrate = len(wins) / n * 100 if n > 0 else 0
    avg_win = wins["net_pnl"].mean() if len(wins) > 0 else 0
    avg_loss = losses["net_pnl"].mean() if len(losses) > 0 else 0
    expectancy = df["net_pnl"].mean()
    total_fees = df["fees"].sum()

    print(f"\n{'='*70}")
    print(f"BACKTEST DONCHIAN — Summary ({BACKTEST_DAYS} days)")
    print(f"{'='*70}")
    print(f"  Initial equity:     ${initial_equity_total:.2f}")
    print(f"  Final equity:       ${initial_equity_total + total_pnl:.2f}")
    print(f"  Total net PnL:      ${total_pnl:+.2f}")
    print(f"  Return:             {total_pnl/initial_equity_total*100:+.2f}%")
    days = BACKTEST_DAYS
    annualized = ((initial_equity_total + total_pnl) / initial_equity_total) ** (365/days) - 1
    print(f"  Annualized return:  {annualized*100:+.2f}%")
    print()
    print(f"  Total trades:       {n}")
    print(f"  Winners:            {len(wins)} ({winrate:.1f}%)")
    print(f"  Losers:             {len(losses)}")
    print(f"  Avg winner:         ${avg_win:+.2f}")
    print(f"  Avg loser:          ${avg_loss:+.2f}")
    if avg_loss < 0:
        rr_ratio = abs(avg_win / avg_loss)
        print(f"  Win/Loss ratio:     {rr_ratio:.2f}x")
    print(f"  Expected per trade: ${expectancy:+.4f}")
    print(f"  Total fees:         ${total_fees:.2f}  ({total_fees/(abs(total_pnl)+1)*100:.1f}% of |PnL|)")

    # Per-market breakdown
    print(f"\n{'Market':<12} {'N':>5} {'WR%':>6} {'PnL':>10} {'Best':>10} {'Worst':>10}")
    for market in UNIVERSE:
        m_trades = df[df["market"] == market]
        if len(m_trades) == 0:
            continue
        m_wr = (m_trades["net_pnl"] > 0).mean() * 100
        m_pnl = m_trades["net_pnl"].sum()
        m_best = m_trades["net_pnl"].max()
        m_worst = m_trades["net_pnl"].min()
        print(f"  {market:<10} {len(m_trades):>5} {m_wr:>5.1f} ${m_pnl:>+8.2f} ${m_best:>+8.2f} ${m_worst:>+8.2f}")

    # Exit reason breakdown
    print(f"\nExit reasons:")
    for reason, group in df.groupby("exit_reason"):
        print(f"  {reason}: {len(group)} trades, avg pnl ${group['net_pnl'].mean():+.2f}")


async def main():
    print(f"Connecting to dYdX...")
    conn = await connect_dydx()
    if not conn or any(x is None for x in conn):
        print("❌ Connection failed. Check network / node URL.")
        return
    node, indexer, wallet = conn
    print(f"✓ Connected\n")

    print(f"Fetching {BACKTEST_DAYS} days of daily candles for {len(UNIVERSE)} markets...")
    market_data = {}
    for market in UNIVERSE:
        print(f"  {market}...", end=" ", flush=True)
        df = await fetch_candles_range(indexer, market, BACKTEST_DAYS)
        if df is not None and len(df) >= N_ENTRY + N_EXIT + ATR_PERIOD:
            market_data[market] = df
            print(f"✓ {len(df)} bars")
        else:
            print(f"⚠️ insufficient data")
        await asyncio.sleep(0.3)

    if not market_data:
        print("\n❌ No data collected. Backtest cannot run.")
        return

    print(f"\nRunning backtest with strategy:")
    print(f"  Entry: close > {N_ENTRY}d high (LONG) / close < {N_ENTRY}d low (SHORT)")
    print(f"  Exit:  close < {N_EXIT}d low (LONG) / close > {N_EXIT}d high (SHORT)")
    print(f"  ATR:   {ATR_PERIOD}d")
    print(f"  Risk per trade: {RISK_PER_TRADE_PCT*100:.1f}% of equity")
    print(f"  Slippage: {SLIPPAGE_BPS*10000:.0f} bps  |  Taker fee: {TAKER_FEE_BPS*10000:.0f} bps")
    print()

    # Run backtest on each market independently
    per_market_equity = INITIAL_EQUITY / len(market_data)
    all_trades = []
    for market, df in market_data.items():
        trades, final_eq, stats = backtest_single_market(df, market, per_market_equity)
        all_trades.extend(trades)
        print(f"  {market}: {stats['n_trades']} trades, "
              f"${per_market_equity:.0f} → ${final_eq:.0f} ({stats['return_pct']:+.1f}%)")

    summarize_trades(all_trades, INITIAL_EQUITY)


if __name__ == "__main__":
    asyncio.run(main())
