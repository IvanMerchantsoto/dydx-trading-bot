# func_pnl.py

def leg_pnl(side, entry_price, exit_price, size):
    """
    side: "BUY" (long) o "SELL" (short)
    """
    try:
        entry_price = float(entry_price)
        exit_price = float(exit_price)
        size = float(size)
    except Exception:
        return 0.0

    if side == "BUY":
        return (exit_price - entry_price) * size
    else:
        return (entry_price - exit_price) * size
