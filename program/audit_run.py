#!/usr/bin/env python3
"""
audit_run.py — Analizador comprensivo de bot_run.log.jsonl

Para cuando me pegas un log, me da DE UN TIRO toda la salud del bot:
  1. Overview: rango, restarts, equity tracking, num scans
  2. Funnel: CSV → universe → candidates → gates → commits → fills → LIVE
  3. Skip reasons distribution (top causes con muestras)
  4. Spread gate health: ok/blocked, por qué bloquea
  5. Chain broadcast health: ok/rejected, raw_logs únicos
  6. Trade economics (cuando LIVE): edge / fees / slippage / PnL
  7. Performance: scan duration distribution (si hay scan_timing events)
  8. Critical signals: 100% NOT_FOUND, indexer lag, etc.
  9. Recommended next actions

Uso:
    cd ~/DYDX/program
    python3 audit_run.py [ruta_al_log]

Si no pasas ruta, usa ./logs/bot_run.log.jsonl
"""

import json
import sys
import statistics
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path


DEFAULT_LOG = Path(__file__).parent / "logs" / "bot_run.log.jsonl"


def _iso_to_dt(ts):
    """Parse ISO timestamp safely."""
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None


def _percentile(arr, p):
    if not arr:
        return 0
    return statistics.quantiles(sorted(arr), n=100, method="inclusive")[min(98, max(0, p-1))]


def _fmt_dt(dt):
    if not dt:
        return "?"
    return dt.strftime("%Y-%m-%d %H:%M:%S UTC")


def _section(title):
    print()
    print("=" * 74)
    print(title)
    print("=" * 74)


def main(log_path):
    if not Path(log_path).exists():
        print(f"❌ Log no existe: {log_path}")
        sys.exit(1)

    print(f"\nAnalyzing: {log_path}\n")

    # Bucket all events
    types = Counter()
    first_ts = None
    last_ts = None

    # State trackers
    restarts = []        # [(ts, equity_seed)]
    equity_seeds = []    # equity at session start
    kpi_snapshots = []   # [(ts, equity, free, positions)]

    # Funnel
    csv_total_seen = 0
    universe_kept = 0
    csv_after_prefilter = 0
    prefilter_drops = Counter()
    candidates_total = 0
    candidates_scans = 0   # how many scans produced candidates
    entries_attempted = 0  # entry_signal count
    entry_status = Counter()
    entry_skip_reasons = Counter()
    entry_skip_samples = []
    signal_skip_reasons = Counter()
    skip_pair_top = defaultdict(Counter)  # reason → Counter(pair)

    # Gate diagnostics
    spread_ok = 0
    spread_blocked_by_reason = Counter()
    spread_block_samples = []
    funding_ok = 0
    funding_block = 0

    # Chain broadcast
    tx_broadcast_ok = 0
    tx_broadcast_rejected = 0
    tx_rejection_codes = Counter()
    tx_rejection_logs = []   # sample raw_logs

    # Audit
    audit_status = Counter()
    audit_attempts_when_found = []  # how many attempts to see fill
    audit_attempts_when_not_found = []
    audit_by_trace = defaultdict(list)

    # Fills
    fills_combos = Counter()
    live_fills = []   # [(trace_id, filled_usd_1, filled_usd_2, fee_1, fee_2, score)]
    failed_examples = []

    # Performance
    scan_durations = []  # if scan_timing events exist

    # Errors / warnings
    chain_rejections_samples = []
    prepare_warns = 0
    orphan_saved = 0
    open_errors = 0
    open_error_samples = []

    # PARSE
    with open(log_path, errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                j = json.loads(line)
            except Exception:
                continue
            t = j.get("type", "_")
            types[t] += 1
            ts = j.get("ts")
            if ts:
                if not first_ts or ts < first_ts:
                    first_ts = ts
                if not last_ts or ts > last_ts:
                    last_ts = ts

            if t == "session_equity_seeded":
                restarts.append((ts, j.get("equity_session_start")))
                if j.get("equity_session_start") is not None:
                    equity_seeds.append(float(j["equity_session_start"]))

            elif t == "kpi_snapshot":
                kpi_snapshots.append({
                    "ts": ts,
                    "equity": j.get("equity"),
                    "free": j.get("free_collateral"),
                    "positions": j.get("num_positions"),
                    "open_pairs": j.get("open_pairs"),
                })

            elif t == "universe_filtered":
                csv_total_seen = max(csv_total_seen, j.get("csv_total", 0))
                universe_kept = j.get("actual_kept", 0)

            elif t == "csv_prefiltered":
                csv_after_prefilter = j.get("csv_after_prefilter", 0)
                prefilter_drops["price_ratio"] += j.get("dropped_price_ratio", 0)
                prefilter_drops["nan_runtime"]  += j.get("dropped_nan_runtime", 0)
                prefilter_drops["no_market"]    += j.get("dropped_no_market", 0)

            elif t == "entry_candidates_scored":
                candidates_total += j.get("candidates", 0)
                candidates_scans += 1

            elif t == "entry_signal":
                entries_attempted += 1

            elif t == "entry_result":
                entry_status[j.get("pair_status", "?")] += 1
                if j.get("pair_status") == "FAILED" and len(failed_examples) < 5:
                    failed_examples.append(j)
                if j.get("pair_status") == "LIVE":
                    live_fills.append({
                        "trace_id": j.get("trace_id"),
                        "pair": f"{j.get('base','?')}/{j.get('quote','?')}",
                        "filled_usd_1": j.get("filled_usd_1", 0),
                        "filled_usd_2": j.get("filled_usd_2", 0),
                        "fee_1": j.get("fee_1", 0),
                        "fee_2": j.get("fee_2", 0),
                        "score": j.get("score", 0),
                    })

            elif t == "entry_skipped":
                # comment usually has the reason prefix
                cmt = j.get("comments") or j.get("reason") or ""
                reason = cmt.split(":")[0].strip()[:30] or "unknown"
                entry_skip_reasons[reason] += 1
                if len(entry_skip_samples) < 10:
                    entry_skip_samples.append(j)

            elif t == "signal_skip":
                r = j.get("reason", "?")
                signal_skip_reasons[r] += 1
                pair = f"{j.get('base','?')}/{j.get('quote','?')}"
                skip_pair_top[r][pair] += 1

            elif t == "spread_check":
                if j.get("ok"):
                    spread_ok += 1
                else:
                    reason = j.get("reason", "unknown_block")
                    spread_blocked_by_reason[reason] += 1
                    if len(spread_block_samples) < 5:
                        spread_block_samples.append(j)

            elif t == "funding_check":
                if j.get("ok"):
                    funding_ok += 1
                else:
                    funding_block += 1

            elif t == "tx_broadcast_result":
                if j.get("broadcast_ok"):
                    tx_broadcast_ok += 1
                else:
                    tx_broadcast_rejected += 1
                    code = j.get("tx_code")
                    tx_rejection_codes[code] += 1
                    if len(tx_rejection_logs) < 5:
                        tx_rejection_logs.append({
                            "market": j.get("market"),
                            "code": code,
                            "raw_log": j.get("raw_log"),
                        })

            elif t == "order_chain_rejected":
                if len(chain_rejections_samples) < 5:
                    chain_rejections_samples.append(j)

            elif t == "audit":
                s = j.get("status", "?")
                audit_status[s] += 1
                trace = j.get("trace_id")
                audit_by_trace[trace].append(j)

            elif t == "fills":
                key = (j.get("status_1", "?"), j.get("status_2", "?"))
                fills_combos[key] += 1

            elif t == "scan_timing":
                d = j.get("scan_total_s")
                if d is not None:
                    try:
                        scan_durations.append(float(d))
                    except Exception:
                        pass

            elif t == "prepare_visibility_warn":
                prepare_warns += 1

            elif t == "orphan_saved":
                orphan_saved += 1

            elif t == "open_error":
                open_errors += 1
                if len(open_error_samples) < 3:
                    open_error_samples.append(j)

    # Derived metrics from audit_by_trace: attempts per fill
    for trace, audits in audit_by_trace.items():
        # last audit per market gives the final state
        last_by_market = {}
        for a in audits:
            last_by_market[a.get("market")] = a
        for m, last in last_by_market.items():
            if last.get("filled_size", 0) > 0:
                audit_attempts_when_found.append(last.get("attempt", 0))
            elif last.get("status") == "NOT_FOUND":
                audit_attempts_when_not_found.append(last.get("attempt", 0))

    # ── REPORT ────────────────────────────────────────────────────────────

    _section("1. RUN OVERVIEW")
    print(f"  Time range:   {first_ts}  →  {last_ts}")
    dt_start = _iso_to_dt(first_ts)
    dt_end = _iso_to_dt(last_ts)
    if dt_start and dt_end:
        elapsed_h = (dt_end - dt_start).total_seconds() / 3600.0
        print(f"  Duration:     {elapsed_h:.1f} hours")
    print(f"  Total events: {sum(types.values()):,}")
    print(f"  Restarts:     {len(restarts)}")
    if equity_seeds:
        print(f"  Equity seeds: {equity_seeds[0]:.2f} → {equity_seeds[-1]:.2f} "
              f"({'+' if equity_seeds[-1] >= equity_seeds[0] else ''}{equity_seeds[-1] - equity_seeds[0]:.2f})")
    if kpi_snapshots:
        first_kpi = kpi_snapshots[0]
        last_kpi = kpi_snapshots[-1]
        print(f"  Equity (KPI): ${first_kpi.get('equity','?')} → ${last_kpi.get('equity','?')}")
        print(f"  Positions:    last snapshot = {last_kpi.get('positions','?')}")

    _section("2. FUNNEL — CSV → LIVE")
    print(f"  csv_total            (CSV file rows):   {csv_total_seen:>6}")
    print(f"  universe_kept        (post quality):    {universe_kept:>6}")
    print(f"  csv_after_prefilter  (post pre-filter): {csv_after_prefilter:>6}")
    if sum(prefilter_drops.values()):
        for r, n in prefilter_drops.most_common():
            print(f"     - dropped {r}: {n}")
    print(f"  candidates_total     (all scans):       {candidates_total:>6}")
    if candidates_scans > 0:
        print(f"  candidates_per_scan  (avg):            {candidates_total/candidates_scans:>6.1f}")
    print(f"  entries_attempted    (entry_signal):    {entries_attempted:>6}")
    for s, n in entry_status.most_common():
        pct = 100 * n / max(1, sum(entry_status.values()))
        print(f"     ↳ pair_status = {s:<8}: {n:>5}  ({pct:5.1f}%)")

    if entry_status.get("LIVE", 0) == 0:
        print(f"\n  🚨 0 trades LIVE en este run. Razones combinadas más abajo.")
    else:
        pct_live = 100 * entry_status['LIVE'] / max(1, sum(entry_status.values()))
        print(f"\n  ✅ {entry_status['LIVE']} trades LIVE ({pct_live:.1f}% conversion)")

    _section("3. SIGNAL_SKIP REASONS (top 10)")
    total_sig_skip = sum(signal_skip_reasons.values())
    print(f"  Total signal_skips: {total_sig_skip:,}")
    for r, n in signal_skip_reasons.most_common(10):
        pct = 100 * n / max(1, total_sig_skip)
        print(f"    {n:>6}  ({pct:5.1f}%)  {r}")
        # top pair for this reason
        if skip_pair_top[r]:
            top_pair, top_n = skip_pair_top[r].most_common(1)[0]
            print(f"            ↳ worst offender: {top_pair} ({top_n}x)")

    _section("4. ENTRY_SKIPPED REASONS (passed Phase 1, blocked at gate)")
    if not entry_skip_reasons:
        print("  None.")
    else:
        for r, n in entry_skip_reasons.most_common():
            print(f"  {n:>5}  {r}")
        if entry_skip_samples:
            print(f"\n  Sample (first 3):")
            for ex in entry_skip_samples[:3]:
                print(f"    - {ex.get('base','?')}/{ex.get('quote','?')}: {ex.get('comments','')[:100]}")

    _section("5. SPREAD GATE")
    total_sp = spread_ok + sum(spread_blocked_by_reason.values())
    if total_sp > 0:
        pct_ok = 100 * spread_ok / total_sp
        print(f"  Total spread_check events: {total_sp}")
        print(f"  OK:      {spread_ok}  ({pct_ok:.1f}%)")
        print(f"  Blocked: {sum(spread_blocked_by_reason.values())}")
        for r, n in spread_blocked_by_reason.most_common():
            print(f"     ↳ {r}: {n}")
    else:
        print("  No spread_check events.")

    _section("6. CHAIN BROADCAST HEALTH")
    total_bc = tx_broadcast_ok + tx_broadcast_rejected
    if total_bc == 0:
        print(f"  ⚠️ No tx_broadcast_result events found.")
        print(f"     Esto es esperado si el log es ANTES del fix 2026-06-02.")
        print(f"     Después del fix, todo place_market/place_limit emite uno.")
    else:
        pct_ok = 100 * tx_broadcast_ok / total_bc
        print(f"  Total broadcasts: {total_bc}")
        print(f"  Accepted:  {tx_broadcast_ok}  ({pct_ok:.1f}%)")
        print(f"  Rejected:  {tx_broadcast_rejected}")
        if tx_rejection_codes:
            print(f"  Rejection codes: {dict(tx_rejection_codes)}")
        for r in tx_rejection_logs[:3]:
            print(f"    - {r['market']}  code={r['code']}  raw_log={(r['raw_log'] or '')[:120]}")

    _section("7. AUDIT (fill detection)")
    if not audit_status:
        print("  No audit events.")
    else:
        total_audits = sum(audit_status.values())
        print(f"  Total audit events: {total_audits}")
        for s, n in audit_status.most_common():
            print(f"     ↳ {s}: {n}")
        if audit_attempts_when_found:
            print(f"\n  Attempts to see FILLED:")
            print(f"     p50={int(statistics.median(audit_attempts_when_found))}  "
                  f"p90={_percentile(audit_attempts_when_found,90):.0f}  "
                  f"max={max(audit_attempts_when_found)}")
        if audit_attempts_when_not_found:
            print(f"\n  NOT_FOUND traces: {len(audit_attempts_when_not_found)}")
            print(f"  (estos son fallos persistentes — exhaurieron retries)")

    _section("8. FILLS DISTRIBUTION")
    if not fills_combos:
        print("  No fills events.")
    else:
        total_f = sum(fills_combos.values())
        for combo, n in fills_combos.most_common(10):
            pct = 100 * n / total_f
            print(f"     ({combo[0]} , {combo[1]}): {n}  ({pct:.1f}%)")

    _section("9. TRADES LIVE (economics)")
    if not live_fills:
        print("  No LIVE trades en este log.")
    else:
        print(f"  Total LIVE: {len(live_fills)}")
        total_filled = sum((f['filled_usd_1'] or 0) + (f['filled_usd_2'] or 0) for f in live_fills)
        total_fees   = sum((f['fee_1'] or 0) + (f['fee_2'] or 0) for f in live_fills)
        print(f"  Total filled USD: ${total_filled:.2f}")
        print(f"  Total fees:       ${total_fees:.4f}")
        if total_filled > 0:
            print(f"  Fees / notional:  {100*total_fees/total_filled:.3f}%")
        print(f"\n  Samples (first 5):")
        for f in live_fills[:5]:
            print(f"    {f['pair']:<28} filled=${(f['filled_usd_1'] or 0)+(f['filled_usd_2'] or 0):>6.2f} "
                  f"fees=${(f['fee_1'] or 0)+(f['fee_2'] or 0):>5.4f}  score={f['score']:.3f}")

    _section("10. PERFORMANCE (scan timing)")
    if not scan_durations:
        print("  No scan_timing events.")
        print("  (esperado si el log es ANTES del fix 2026-06-02 de instrumentación)")
    else:
        print(f"  Total scans timed: {len(scan_durations)}")
        print(f"  p50: {statistics.median(scan_durations):>6.1f}s")
        print(f"  p90: {_percentile(scan_durations, 90):>6.1f}s")
        print(f"  p99: {_percentile(scan_durations, 99):>6.1f}s")
        print(f"  max: {max(scan_durations):>6.1f}s")

    _section("11. WARNINGS & ERRORS")
    print(f"  prepare_visibility_warn:  {prepare_warns}")
    print(f"  orphan_saved:             {orphan_saved}")
    print(f"  open_error:               {open_errors}")
    print(f"  chain rejections (new):   {len(chain_rejections_samples)}")
    if open_error_samples:
        print(f"\n  open_error sample:")
        ex = open_error_samples[0]
        print(f"    {json.dumps({k: ex[k] for k in ('error','market','market_1','market_2') if k in ex}, indent=2)}")

    _section("12. RECOMMENDED NEXT ACTIONS")

    # Heuristic recommendations
    actions = []

    if entry_status.get("LIVE", 0) == 0:
        if tx_broadcast_rejected > 0:
            actions.append("→ Chain está rechazando órdenes. Mira la sección 6 (codes & raw_logs).")
        elif tx_broadcast_ok == 0 and audit_status.get("NOT_FOUND", 0) > 0:
            actions.append("→ tx_broadcast_result events ausentes pero hay NOT_FOUND. "
                           "Estás corriendo código pre-2026-06-02 — actualiza func_private.py.")
        elif audit_status.get("NOT_FOUND", 0) > 0 and tx_broadcast_ok > 0:
            actions.append("→ Broadcast OK pero indexer no ve fills. Aumenta audit_retries "
                           "o investiga indexer lag.")
        else:
            actions.append("→ 0 LIVE pero también 0 commits. El gate de spread/edge o el "
                           "z-score threshold está bloqueando TODO. Revisa secciones 4 y 5.")

    if signal_skip_reasons.get("zscore_not_finite", 0) > 100:
        actions.append("→ Muchos NaN z-scores. Confirma que csv_prefiltered está activo "
                       "(sección 2 muestra dropped_nan_runtime > 0).")

    if signal_skip_reasons.get("price_ratio", 0) > 500:
        if prefilter_drops.get("price_ratio", 0) == 0:
            actions.append("→ Muchos skips por price_ratio. Asegúrate de que el pre-filter "
                           "esté pasando (sección 2 → dropped_price_ratio > 0).")

    if prepare_warns > 100 and entries_attempted < prepare_warns:
        actions.append("→ Indexer está laggy. Considera aumentar prepare_wait_s o "
                       "ignorar prepare_visibility_warn.")

    if not actions:
        actions.append("→ Métricas saludables. Mantén el monitoreo.")

    for a in actions:
        print(f"  {a}")

    print()


if __name__ == "__main__":
    log = sys.argv[1] if len(sys.argv) > 1 else str(DEFAULT_LOG)
    main(log)
