# dYdX Bot Audit Report — Overnight Run Analysis
**Log:** bot_run.log-2.jsonl (11,594 events) | **CSV:** trades (7).csv (1,000 fills)
**Sessions:** Apr 23 20:32–23:xx + May 19 02:07–14:44 | **Generated:** 2026-05-19

---

## 1. Realized P&L Analysis

### Top-Level Numbers (from dYdX CSV fills)
| Metric | Value |
|--------|-------|
| Total buys executed | $236,738.73 |
| Total sells executed | $231,043.57 |
| Total fees paid | $232.49 |
| **Net P&L (round-trip)** | **-$5,927.65** |

The large net loss is partly explained by open positions (unmatched buys/sells still live at export time) and partly by orphan losses.

### Per-Market FIFO Round-Trip P&L (top losers)
| Market | Realized | Fees | Net PnL | Unmatched |
|--------|----------|------|---------|-----------|
| PYTH-USD | -$390.45 | $4.12 | **-$394.57** | — |
| ALGO-USD | -$247.73 | $12.52 | **-$260.25** | 0/8,700 units unsold |
| WOO-USD | -$234.28 | $10.22 | **-$244.50** | 3,900 units unbought |
| CRO-USD | -$209.12 | $34.39 | **-$243.52** | 0/14,100 units unsold |
| MOVE-USD | -$215.23 | $3.94 | **-$219.17** | 0/8,130 units unsold |
| SUI-USD | -$177.95 | $15.48 | **-$193.43** | 1,030 units unbought |
| W-USD | -$173.19 | $12.70 | **-$185.89** | — |
| ARKM-USD | -$123.21 | $8.97 | **-$132.18** | 0/16,417 unsold |
| WIF-USD | -$118.24 | $8.95 | **-$127.19** | 239/0 |
| ADA-USD | -$81.62 | $14.17 | **-$95.79** | 0/3,930 unsold |

**Bright spots:** PENDLE-USD +$277.18, ETC-USD +$29.56, UNI-USD +$20.58

**Root causes of large losses:**
- Many ALGO/CRO/WOO positions show large unmatched quantities → positions likely still open at export (mark-to-market unrealized is not captured in realized CSV)
- Orphan legs were opened and closed at a loss when the pair failed mid-entry
- Fee churn: 162 commit attempts × average $0.75/commit in open fees alone

---

## 2. Fill Classification — Entry Pipeline Breakdown

Out of **375 entry_result events**:

| Status | Count | % | Meaning |
|--------|-------|---|---------|
| **ERROR** | 123 | 33% | PREPARE order not visible in indexer (pre-COMMIT abort). No fees paid. |
| **FAILED** | 104 | 28% | One or both legs filled 0 after COMMIT; flatten attempted |
| **SKIPPED** | 90 | 24% | Spread gate (bid-ask too wide). No fees paid. |
| **LIVE** | 44 | 12% | Both legs filled, passed all gates → position tracked |
| **ORPHAN** | 14 | 4% | Committed, but exposure detected post-failure |

Of the **162 COMMIT attempts**, fill outcome combos:
| M1 status / M2 status | Count | Result |
|-----------------------|-------|--------|
| FILLED / FILLED | 46 | → LIVE (if residual OK) |
| NOT_FOUND / FILLED | 26 | → flatten M1, FAILED |
| FILLED / NOT_FOUND | 24 | → flatten M2, FAILED |
| NOT_FOUND / NOT_FOUND | 21 | → FAILED, no exposure |
| BEST_EFFORT_OPENED (any) | 21 | → **Orphan risk** (see Bug #1) |
| PARTIALLY_FILLED_IOC | 24 | → residual check, usually FAILED |

**One-leg fill rate: 31%** — extremely high for testnet. Primary driver: illiquid pairs (POPCAT, ZRO, ARKM, ATH) where one leg exhausts the book and the other returns NOT_FOUND or BEST_EFFORT.

---

## 3. Orphan Ledger

**75 unique traces** had orphan activity (86 orphan_saved events).

### First Session (Apr 23, 20:42–21:00) — 14 orphans
These opened in rapid succession as the bot tried to reconcile old positions from previous sessions (COMP/LDO/STX/RUNE stuck since earlier runs). Root cause: `UNMANAGED_IGNORE_MARKETS` was not filtering these from the reconcile "unmanaged" set → entries were blocked, but orphan records still accumulated.

Key orphans from first session:
| Trace | Pair | M1 size | M2 size | Root cause |
|-------|------|---------|---------|------------|
| 0a0a8c5940 | ADA/DOT | -4,010 | 0 | Previous session residual |
| 4b29d7dc28 | ICP/ALGO | +404 | -9,650 | Previous session residual |
| 1a71a6aa80 | W/ZORA | -77,980 | 0 | Previous session residual |
| 1962ea2f16 | OP/WOO | -8,302 | +1,320 | Asymmetric fill |

### Second Session (May 19, 02:07–10:41) — 53 orphans
All from the same pattern: one leg returns NOT_FOUND after 5 audit retries (50 cases) or BEST_EFFORT_OPENED (21 cases).

**Most frequent pairs:**
- ALGO/ZK: 6 orphans (ZK-USD is extremely illiquid testnet)
- ZRO/POPCAT: 5 orphans (POPCAT bid-ask null repeatedly)
- POPCAT/ALGO: 5 orphans
- ATH/W: 4 orphans
- RENDER/ALGO: 3 orphans
- WOO/MET: 3 orphans

**Cleanup result:** 32 single_leg_cleanup attempts, 55 cleanup_close_errors (all from April 23 session, `max_retries` kwarg bug — already fixed). May 19 cleanups executed successfully.

### Classification
| Category | Count | Fee cost est. |
|----------|-------|---------------|
| NOT_FOUND after 5 retries (one leg) | 50 | ~$0.50/each |
| BEST_EFFORT_OPENED (order stayed in book) | 21 | ~$0.50/each |
| Pre-commit exposure (false save, no fees) | 3 | $0 |
| Residual too large (one leg fully unfilled) | 12 | ~$0.50/each |

**Estimated orphan fee cost:** ~$40–$55 in unnecessary taker fees from flatten attempts.

---

## 4. Logic Bugs Identified

### Bug #1 (CRITICAL): BEST_EFFORT_OPENED orders not cancelled
**File:** `func_bot_agent.py`  
**What happens:** On testnet, a MARKET IOC order sometimes lands as `OPEN` in the book (BEST_EFFORT_OPENED). The audit returns `filled_size=0`. Bot treats it as "not filled" → flattens the other leg → BUT the BEST_EFFORT order is still live and may fill later → orphan position.  
**Evidence:** 21 commits with BEST_EFFORT_OPENED in fills events. Traces like `5d9a9b689d` (ARB/ARKM): ARKM filled but ARB returned BEST_EFFORT.  
**Fix implemented:** After AUDIT, detect BEST_EFFORT_OPENED on any leg → call `cancel_order_by_client_id` → wait 1.5s → query real position size → use actual position as fill size.

### Bug #2 (CRITICAL): UNMANAGED_IGNORE_MARKETS not filtered in reconcile
**File:** `func_position_guard.py`  
**What happens:** `reconcile_bot_vs_dydx` computes `unmanaged = live_markets - expected_markets`. LDO-USD, COMP-USD, STX-USD are in `UNMANAGED_IGNORE_MARKETS` but NOT filtered out of `unmanaged`. Result: block_entries=True continuously → all entries blocked while stuck testnet positions exist (even though we can't close them).  
**Evidence:** 84 reconcile_state events with `block_entries=True` for `['COMP-USD', 'LDO-USD', 'STX-USD']`. Bot still opened trades via the `open_positions()` path which bypasses `assert_safe_to_open`.  
**Fix implemented:** Exclude `UNMANAGED_IGNORE_MARKETS` from the `unmanaged` set in `reconcile_bot_vs_dydx`.

### Bug #3 (HIGH): False `orphan_saved` for pre-COMMIT errors
**File:** `func_entry_pairs.py`  
**What happens:** When `open_trades()` returns `ERROR` with `PRE_COMMIT_EXPOSURE_DETECTED`, no COMMIT was made. But the orphan check runs anyway (`_get_live_markets_with_position`), finds the PRE-EXISTING position from a previous session, and logs `orphan_saved` + sends Telegram alert. This creates a false orphan record that later triggers `single_leg_cleanup` of old exposure.  
**Evidence:** Trace `4fa5a86f98` (W/ATH): `open_error: pre_commit_exposure_gate → ATH-USD` → immediately `orphan_saved` → `single_leg_cleanup: ATH-USD 16,000 units`.  
**Fix implemented:** Added `committed` flag (default `False`) to `order_dict`. Set to `True` only after `commit_sent`. Orphan check in `func_entry_pairs.py` now skipped when `committed=False`.

### Bug #4 (MEDIUM): `assert_safe_to_open` never called in main loop
**File:** `main.py`  
**What happens:** The PLACE_TRADES block in `main.py` only checks `len(bot_agents.json) >= MAX_OPEN_TRADES`. It never calls `assert_safe_to_open` (the real reconcile gate). The reconcile gate with dYdX as source of truth only ran via `func_position_guard.py` which was not imported by main.py.  
**Evidence:** `reconcile_state` events with `block_entries=True` coexist with `entry_signal`/`open_start` events from a separate code path.  
**Fix implemented:** `main.py` now imports `assert_safe_to_open` and calls it before `open_positions()`. If blocked, attempts to close unmanaged markets. Only proceeds to open new trades when gate passes.

### Bug #5 (MEDIUM): `open_fees_paid=0` in profit gate for legacy records
**File:** `func_exit_pairs.py`  
**What happens:** Positions opened before the fee-tracking fix have `fee_1=fee_2=0` in `bot_agents.json`. The profit gate uses `open_fees_paid=0`, making `min_gross_required` too lenient by ~$1.00 per pair (the missing open fee component). Trades close "profitably" when actual net (after open fees) is negative.  
**Evidence:** Sample `tp_blocked_fees` event: `open_fees=0.0, close_fees_est=$1.50, min_gross=$2.00` (should be $3.00 with real open fees).  
**Fix implemented:** When `open_fees_paid=0.0` and `total_notional > 0`, estimate open fees as `TAKER_FEE_BPS × total_notional`.

### Bug #6 (LOW): `cleanup_close_error: max_retries kwarg` (Apr 23 session)
**File:** `func_position_guard.py`  
**What happened:** 55 cleanup attempts for COMP/LDO/STX all failed with `get_real_fill_details() got an unexpected keyword argument 'max_retries'`. Correct param is `max_fill_lookback`.  
**Status:** Already fixed (Task #1, current code uses `max_fill_lookback=200`).

### Bug #7 (LOW): TP double-confirm count resets when z briefly exits zone
**File:** `func_exit_pairs.py`  
**What happens:** `tp_confirm` resets to 0 every time `tp_zone=False` (any check where z > 0.7). With EXIT_CHECK_SECONDS=30 and Z_TP=0.7, one slightly-off candle resets the 2-check accumulator. Result: 524 `tp_blocked_fees` events showing profitable trades (even gross > fees) that can't clear 2 consecutive checks because z oscillates just above 0.7.  
**Recommendation:** Consider storing confirm count per-trace and only resetting after confirmed deviation (e.g. abs(z) > 1.0), not the moment it exits TP zone.

---

## 5. Risk-Off Analysis

7 risk-off activations during the run, all of which closed real losing positions:

| Pair | Age | Unrealized | Score | Verdict |
|------|-----|-----------|-------|---------|
| WLD/APT | 1.43h | -$13.37 | 12.65 | ✅ Correct (real loss) |
| XMR/PAXG | 1.35h | -$9.13 | 10.82 | ✅ Correct |
| ARB/OP | 5.68h | -$1.24 | 10.99 | ⚠️ High z-score drove score (abs_z=2.16) |
| W/ATH | 1.51h | -$0.82 | 9.54 | ⚠️ Very small loss, mostly z-score |
| WIF/ARKM | 1.55h | -$2.50 | 6.31 | ✅ Reasonable |
| MET/ZORA | 1.51h | -$23.44 | 5.44 | ✅ Large real loss |
| XMR/PAXG | 4.37h | -$3.51 | 6.20 | ✅ Correct |

**41 risk_off_age_skip events** — the 0.5h age guard is working correctly, protecting fresh opens.
**1 risk_off_skipped_positive_unreal** — correctly skipped a pair with positive P&L.

Risk-off is behaving well. The W/ATH case (unreal=-$0.82, age=1.5h) is borderline but technically meets all thresholds.

---

## 6. Changes Implemented

### `func_position_guard.py`
- Import `UNMANAGED_IGNORE_MARKETS` from constants
- Filter `UNMANAGED_IGNORE_MARKETS` out of the `unmanaged` set in `reconcile_bot_vs_dydx`

### `func_bot_agent.py`
- Import `cancel_order_by_client_id` from func_private
- Added `"committed": False` field to initial `order_dict`
- After `commit_sent`: set `self.order_dict["committed"] = True`
- After AUDIT phase: if any leg has `BEST_EFFORT_OPENED` status → cancel the order via `cancel_order_by_client_id`, wait 1.5s, then query real position size via `get_live_positions` and update `filled_size`/`filled_usd` accordingly

### `func_entry_pairs.py`
- Orphan check now reads `committed` flag from `bot_open_dict`
- If `committed=False` (pre-COMMIT error), skip `_get_live_markets_with_position` call entirely → no false `orphan_saved` for old exposure

### `func_exit_pairs.py`
- When `open_fees_paid=0.0` and `total_notional > 0.0`, estimate `open_fees_paid = total_notional × TAKER_FEE_BPS` before calling `_profit_gate`
- This prevents the profit gate from being too lenient on legacy JSON records

### `main.py`
- Import `assert_safe_to_open`, `close_markets_actual` from `func_position_guard`
- In PLACE_TRADES block: call `assert_safe_to_open(indexer)` before `open_positions()`
- If blocked: log and attempt to close unmanaged markets via `close_markets_actual`
- If safe: proceed with normal batch open logic

### `test_simulations.py`
- T22: `committed` flag starts False, orphan check skipped when committed=False
- T23: `open_fees_paid` estimated correctly for legacy records, profit gate correctly tighter
- T24: `UNMANAGED_IGNORE_MARKETS` filtered from reconcile unmanaged set
- **64/64 tests passing**

---

## 7. Validation Checklist Before Live Trading

Before restarting the bot with real money, verify the following:

**Server config (`constants.py`):**
- [ ] `ABORT_ALL_POSITIONS = False`
- [ ] `FIND_COINTEGRATED = False`
- [ ] `UNMANAGED_IGNORE_MARKETS` contains all stuck testnet markets: `{"LDO-USD", "COMP-USD", "STX-USD", "ARKM-USD"}`
- [ ] `MODE = "DEVELOPMENT"` (testnet) or `"PRODUCTION"` (mainnet)
- [ ] `MAX_ENTRY_SPREAD_BPS = 25` when switching to mainnet

**Cointegration CSV:**
- [ ] `cointegrated_pairs.csv` is fresh (< 24h old)
- [ ] Remove any malformed market rows (FARTCOIN etc.)
- [ ] Verify pairs are still cointegrated with latest data

**bot_agents.json:**
- [ ] Empty array `[]` on fresh start (or contains only known LIVE positions)
- [ ] If restarting mid-session, verify every entry in JSON corresponds to a real dYdX position

**First 30 minutes of operation:**
- [ ] `reconcile_state` events show `block_entries: false`
- [ ] No `orphan_saved` events (if starting fresh)
- [ ] `commit_sent` followed by `fills` with at least some FILLED/FILLED combos
- [ ] At least one `open_live` event per 2-3 loops (signal quality check)
- [ ] No `cleanup_close_error` events

---

## 8. What to Monitor After Next Run

**Logs to watch:**
- `bot_run.log.jsonl`: filter for `"type": "orphan_saved"` — should decrease significantly
- `bot_run.log.jsonl`: filter for `"type": "best_effort_cancel_attempt"` — new event showing BEST_EFFORT fix activated
- `bot_run.log.jsonl`: filter for `"type": "reconcile_state"` with `"block_entries": true` — should only show for real unknowns, not LDO/COMP/STX
- `bot_run.log.jsonl`: filter for `"type": "open_live"` — target > 50% of commits

**KPIs to check:**
- LIVE count / COMMIT count ratio (target > 28%, currently 28%)
- Orphan count per hour (target < 2, currently ~8-10/hour)
- Fee cost per LIVE trade (target < $1.20 round-trip)

---

## 9. Open Questions / Assumptions to Confirm

1. **RUNE-USD**: Shows as unmanaged on May 19 (not in UNMANAGED_IGNORE_MARKETS). Was this position closed manually? If not, add it to the ignore list.

2. **ZORA-USD**: Present in some orphan traces. Currently not in UNMANAGED_IGNORE_MARKETS. Should it be? The account had ZORA exposure across multiple sessions.

3. **PENDLE-USD +$277**: This is a large profitable position (589 units unmatched). Is it still open? If so, monitor — it's a single-leg with no matching sell in the CSV.

4. **ETC-USD +$29**: 18 buys vs 1 sell — large long position still open.

5. **Testnet vs mainnet**: The 31% one-leg failure rate is testnet-specific (thin books, BEST_EFFORT_OPENED). On mainnet with $25 bps spread gate and liquid pairs, expect < 5% one-leg failures.

6. **`tp_confirm` persistence**: The `tp_confirm` field is written back into `bot_agents.json` on every loop. If the bot restarts, confirm counters reset to 0. This is acceptable but means a close may be delayed by 2 extra checks after restart.
