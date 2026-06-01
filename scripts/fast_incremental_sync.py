#!/usr/bin/env python3
"""Fast parallel incremental sync — target < 5 minutes for all levels.

Strategy:
- Daily/weekly: akshare with ThreadPoolExecutor (20 workers)
- 15m/60m: multiple baostock processes (N_BAO_WORKERS), each with own session
- 120m/240m: derived from 60m in-process
- All levels sync in parallel
"""
from __future__ import annotations

import argparse
import multiprocessing as mp
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
N_AK_WORKERS = 20       # concurrent threads for akshare daily/weekly
N_BAO_WORKERS = 6        # concurrent processes for baostock minute (fewer = more stable)
TIMEOUT_PER_SYMBOL = 30  # seconds
LOOKBACK_DAYS = 5        # fetch last N calendar days

BAO_FIELDS = "date,time,code,open,high,low,close,volume,amount"


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--db-path", default=str(ROOT_DIR / "data" / "market_data.sqlite"))
    p.add_argument("--universe-csv", default=str(ROOT_DIR / "data" / "universe_latest.csv"))
    p.add_argument("--lookback", type=int, default=LOOKBACK_DAYS)
    p.add_argument("--ak-workers", type=int, default=N_AK_WORKERS)
    p.add_argument("--bao-workers", type=int, default=N_BAO_WORKERS)
    p.add_argument("--levels", default="daily,weekly,15m,60m")
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args()


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------
def date_range(lookback: int) -> tuple[str, str]:
    end = datetime.now()
    start = end - timedelta(days=lookback)
    return start.strftime("%Y%m%d"), end.strftime("%Y%m%d")


def date_range_dash(lookback: int) -> tuple[str, str]:
    end = datetime.now()
    start = end - timedelta(days=lookback)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# Daily/Weekly via akshare (threaded)
# ---------------------------------------------------------------------------
def _fetch_ak_daily(symbol: str, start: str, end: str, period: str) -> tuple[str, str, list[dict]]:
    """Fetch one symbol's daily or weekly data via akshare. Returns (symbol, level, records)."""
    try:
        import akshare as ak
        df = ak.stock_zh_a_hist(
            symbol=symbol,
            period="daily" if period == "daily" else "weekly",
            start_date=start,
            end_date=end,
            adjust="qfq",
        )
        if df is None or df.empty:
            return symbol, period, []
        # Normalize columns
        col_map = {"日期": "date", "开盘": "open", "最高": "high", "最低": "low",
                    "收盘": "close", "成交量": "volume", "成交额": "amount", "换手率": "turnover"}
        df = df.rename(columns=col_map)
        if "date" not in df.columns:
            return symbol, period, []
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d %H:%M:%S")
        records = df[["date", "open", "high", "low", "close", "volume", "amount"]].to_dict("records")
        return symbol, period, records
    except Exception as e:
        return symbol, period, []


def sync_daily_weekly(symbols: list[str], levels: list[str], lookback: int, max_workers: int) -> dict[str, list[dict]]:
    """Sync daily/weekly in parallel using akshare threads. Returns {f'{symbol}_{level}': records}."""
    start, end = date_range(lookback)
    results: dict[str, list[dict]] = {}
    tasks = []

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        for sym in symbols:
            for level in levels:
                if level in ("daily", "weekly"):
                    tasks.append(pool.submit(_fetch_ak_daily, sym, start, end, level))

        ok = 0
        err = 0
        for future in as_completed(tasks):
            sym, level, records = future.result()
            key = f"{sym}_{level}"
            if records:
                results[key] = records
                ok += 1
            else:
                err += 1

    print(f"  daily/weekly: ok={ok} empty={err}")
    return results


# ---------------------------------------------------------------------------
# Minute via baostock (multiprocessing)
# ---------------------------------------------------------------------------
def _bao_query(bs_mod, bao_code: str, fields: str, start: str, end: str, freq: str) -> list[dict]:
    """Single baostock query with result parsing."""
    rs = bs_mod.query_history_k_data_plus(
        bao_code, fields,
        start_date=start, end_date=end,
        frequency=freq, adjustflag="2",
    )
    rows = []
    while rs.error_code == "0" and rs.next():
        rows.append(rs.get_row_data())
    if not rows:
        return []
    df = pd.DataFrame(rows, columns=fields.split(","))
    if "time" in df.columns and len(df) > 0 and df["time"].iloc[0]:
        df["date"] = pd.to_datetime(df["time"], format="%Y%m%d%H%M%S%f")
    else:
        df["date"] = pd.to_datetime(df["date"])
    df["date"] = df["date"].dt.strftime("%Y-%m-%d %H:%M:%S")
    for col in ["open", "high", "low", "close", "volume", "amount"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    return df[["date", "open", "high", "low", "close", "volume", "amount"]].to_dict("records")


BAO_DAILY_FIELDS = "date,code,open,high,low,close,volume,amount"


def _bao_worker(symbol_chunk: list[str], start_date: str, end_date: str,
                result_queue: mp.Queue, worker_id: int, levels: list[str]):
    """Worker process: login to baostock, fetch requested levels for each symbol."""
    import baostock as bs

    # Retry login up to 3 times
    for attempt in range(3):
        lg = bs.login()
        if lg.error_code == "0":
            break
        time.sleep(2)

    ok = 0
    err = 0
    consecutive_empty = 0

    for sym in symbol_chunk:
        bao_code = f"sh.{sym}" if sym.startswith("6") else f"sz.{sym}"
        sym_data: dict[str, list[dict]] = {}

        try:
            # Minute levels
            for freq, level in [("15", "15m"), ("60", "60m")]:
                if level in levels:
                    sym_data[level] = _bao_query(bs, bao_code, BAO_FIELDS, start_date, end_date, freq)

            # Daily/weekly levels
            for freq, level in [("d", "daily"), ("w", "weekly")]:
                if level in levels:
                    sym_data[level] = _bao_query(bs, bao_code, BAO_DAILY_FIELDS, start_date, end_date, freq)

        except Exception:
            for level in levels:
                sym_data.setdefault(level, [])

        has_data = any(len(v) > 0 for v in sym_data.values())
        if has_data:
            ok += 1
            consecutive_empty = 0
        else:
            err += 1
            consecutive_empty += 1

        # If too many consecutive empties, session might be dead — re-login
        if consecutive_empty >= 10:
            try:
                bs.logout()
            except Exception:
                pass
            time.sleep(1)
            bs.login()
            consecutive_empty = 0

        result_queue.put((sym, sym_data))

    try:
        bs.logout()
    except Exception:
        pass
    result_queue.put(("__DONE__", {"worker_id": worker_id, "ok": ok, "err": err}))


def sync_all_baostock(symbols: list[str], levels: list[str], lookback: int,
                      n_workers: int) -> dict[str, dict[str, list[dict]]]:
    """Sync all levels in parallel using baostock processes.
    Returns {symbol: {'15m': records, '60m': records, 'daily': records, ...}}
    """
    start, end = date_range_dash(lookback)
    results: dict[str, dict[str, list[dict]]] = {}

    # Split symbols into chunks
    chunk_size = max(1, len(symbols) // n_workers)
    chunks = [symbols[i:i + chunk_size] for i in range(0, len(symbols), chunk_size)]
    actual_workers = len(chunks)

    result_queue: mp.Queue = mp.Queue()
    processes = []

    for i, chunk in enumerate(chunks):
        p = mp.Process(target=_bao_worker, args=(chunk, start, end, result_queue, i, levels))
        p.start()
        processes.append(p)

    done_count = 0
    total_ok = 0
    total_err = 0
    timeout_deadline = time.time() + 280  # hard deadline: 4m40s

    while done_count < actual_workers and time.time() < timeout_deadline:
        try:
            sym, data = result_queue.get(timeout=5)
            if sym == "__DONE__":
                done_count += 1
                total_ok += data["ok"]
                total_err += data["err"]
            else:
                results[sym] = data
        except Exception:
            pass

    # Kill any remaining workers
    for p in processes:
        if p.is_alive():
            p.kill()
            p.join(timeout=2)

    print(f"  baostock: ok={total_ok} empty={total_err} workers={done_count}/{actual_workers}")
    return results


# ---------------------------------------------------------------------------
# Aggregate helpers
# ---------------------------------------------------------------------------
def aggregate_bars(records: list[dict], n: int) -> list[dict]:
    if not records:
        return []
    result = []
    for i in range(0, len(records), n):
        group = records[i:i + n]
        result.append({
            "date": group[-1]["date"],
            "open": group[0]["open"],
            "high": max(r["high"] for r in group),
            "low": min(r["low"] for r in group),
            "close": group[-1]["close"],
            "volume": sum(r["volume"] for r in group),
            "amount": sum(r["amount"] for r in group),
        })
    return result


# ---------------------------------------------------------------------------
# DB writer
# ---------------------------------------------------------------------------
def write_all_to_db(db_path: str, all_data: dict[str, dict[str, list[dict]]]) -> int:
    """Write all data {symbol: {level: records}} to sqlite."""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    total_rows = 0

    for symbol, level_data in all_data.items():
        # Derive 120m/240m from 60m
        recs_60m = level_data.get("60m", [])
        if recs_60m:
            level_data["120m"] = aggregate_bars(recs_60m, 2)
            level_data["240m"] = aggregate_bars(recs_60m, 4)

        for level, records in level_data.items():
            if not records:
                continue

            is_derived = 1 if level in ("120m", "240m") else 0
            derived_from = "60m" if level in ("120m", "240m") else None
            source = "baostock"

            rows = []
            for r in records:
                rows.append((
                    symbol, level, "qfq", source, r["date"],
                    float(r["open"]), float(r["high"]), float(r["low"]), float(r["close"]),
                    float(r["volume"]), float(r.get("amount", 0)), 0.0,
                    is_derived, derived_from, now,
                ))
            if rows:
                conn.executemany("""
                    INSERT OR REPLACE INTO kline
                    (symbol, level, adjust, source, date, open, high, low, close,
                     volume, amount, turnover, is_derived, derived_from, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, rows)
                total_rows += len(rows)

    conn.commit()
    conn.close()
    return total_rows


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    args = parse_args()
    levels = [l.strip() for l in args.levels.split(",")]

    df_uni = pd.read_csv(args.universe_csv, dtype={"symbol": str})
    symbols = df_uni["symbol"].tolist()
    print(f"fast_incremental_sync: {len(symbols)} symbols, levels={levels}, lookback={args.lookback}d")
    print(f"  ak_workers={args.ak_workers}, bao_workers={args.bao_workers}")

    t0 = time.time()

    # All data goes through baostock multiprocessing
    all_result = sync_all_baostock(symbols, levels, args.lookback, args.bao_workers)

    t_fetch = time.time() - t0
    print(f"  fetch done in {t_fetch:.1f}s")

    if args.dry_run:
        print("  dry-run: skipping DB write")
        return

    # Write to DB — all levels go through write_minute_to_db (handles daily/weekly too)
    t_write = time.time()
    n_rows = write_all_to_db(args.db_path, all_result)
    t_write = time.time() - t_write

    total = time.time() - t0
    print(f"  DB write: {n_rows} rows in {t_write:.1f}s")
    print(f"  TOTAL: {total:.1f}s ({total/60:.1f}m)")

    if total > 300:
        print(f"  WARNING: exceeded 5 minute target by {total - 300:.0f}s")
    else:
        print(f"  OK: within 5 minute target, {300 - total:.0f}s to spare")


if __name__ == "__main__":
    mp.set_start_method("fork", force=True)
    main()
