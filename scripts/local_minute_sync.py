#!/usr/bin/env python3
"""Local minute-level data sync using baostock. Uses subprocess for hard timeout."""
from __future__ import annotations

import multiprocessing as mp
import sqlite3
import sys
import time
from pathlib import Path

import pandas as pd

DB_PATH = Path(__file__).resolve().parents[1] / "data" / "market_data_minute.sqlite"
UNIVERSE_CSV = Path(__file__).resolve().parents[1] / "data" / "universe_latest.csv"

FIELDS = "date,time,code,open,high,low,close,volume,amount"
START_DATE = "2022-12-25"
END_DATE = "2026-04-09"
TIMEOUT_SEC = 90


def ensure_tables(conn: sqlite3.Connection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS kline (
            symbol TEXT NOT NULL,
            level TEXT NOT NULL,
            adjust TEXT NOT NULL DEFAULT 'qfq',
            source TEXT NOT NULL DEFAULT 'baostock',
            date TEXT NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume REAL,
            amount REAL,
            turnover REAL,
            is_derived INTEGER NOT NULL DEFAULT 0,
            derived_from TEXT,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (symbol, level, adjust, source, date)
        )
    """)
    conn.commit()


def _worker(symbol: str, result_dict: dict):
    """Run in a separate process — can be killed cleanly."""
    import baostock as bs
    bs.login()

    all_data = {}
    for freq, level in [("15", "15m"), ("60", "60m")]:
        bao_code = f"sh.{symbol}" if symbol.startswith("6") else f"sz.{symbol}"
        rs = bs.query_history_k_data_plus(
            bao_code, FIELDS,
            start_date=START_DATE, end_date=END_DATE,
            frequency=freq, adjustflag="2",
        )
        rows = []
        while (rs.error_code == "0") and rs.next():
            rows.append(rs.get_row_data())

        if not rows:
            all_data[level] = []
            continue

        df = pd.DataFrame(rows, columns=FIELDS.split(","))
        if "time" in df.columns and df["time"].iloc[0]:
            df["date"] = pd.to_datetime(df["time"], format="%Y%m%d%H%M%S%f")
        else:
            df["date"] = pd.to_datetime(df["date"])
        df["date"] = df["date"].dt.strftime("%Y-%m-%d %H:%M:%S")
        for col in ["open", "high", "low", "close", "volume", "amount"]:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
        df = df.drop(columns=["time", "code"], errors="ignore")
        all_data[level] = df.to_dict("records")

    try:
        bs.logout()
    except Exception:
        pass

    result_dict["data"] = all_data


def fetch_symbol(symbol: str) -> dict | None:
    """Fetch data for one symbol with hard timeout via subprocess."""
    manager = mp.Manager()
    result_dict = manager.dict()
    p = mp.Process(target=_worker, args=(symbol, result_dict))
    p.start()
    p.join(timeout=TIMEOUT_SEC)

    if p.is_alive():
        p.kill()
        p.join()
        return None

    if "data" not in result_dict:
        return None

    return dict(result_dict["data"])


def aggregate_bars(records: list[dict], n: int) -> list[dict]:
    if not records:
        return []
    result = []
    for i in range(0, len(records), n):
        group = records[i:i+n]
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


def upsert_rows(conn: sqlite3.Connection, symbol: str, level: str,
                records: list[dict], is_derived: int = 0, derived_from: str = "") -> int:
    if not records:
        return 0
    now = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = []
    for r in records:
        rows.append((
            symbol, level, "qfq", "baostock", r["date"],
            float(r["open"]), float(r["high"]), float(r["low"]), float(r["close"]),
            float(r["volume"]), float(r.get("amount", 0)), 0.0,
            is_derived, derived_from or None, now,
        ))
    conn.executemany("""
        INSERT OR REPLACE INTO kline
        (symbol, level, adjust, source, date, open, high, low, close,
         volume, amount, turnover, is_derived, derived_from, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)
    conn.commit()
    return len(rows)


def already_synced(conn: sqlite3.Connection, symbol: str) -> bool:
    row = conn.execute(
        "SELECT COUNT(*) FROM kline WHERE symbol=? AND level='15m'", (symbol,)
    ).fetchone()
    return row[0] > 100


def main():
    mp.set_start_method("fork", force=True)

    df_uni = pd.read_csv(UNIVERSE_CSV, dtype={"symbol": str})
    symbols = df_uni["symbol"].tolist()
    total = len(symbols)

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    ensure_tables(conn)

    print(f"local_minute_sync (baostock subprocess): {total} symbols")
    print(f"levels: 15m, 60m, 120m, 240m | timeout: {TIMEOUT_SEC}s")
    print(f"DB: {DB_PATH}")
    sys.stdout.flush()

    ok = 0
    skipped = 0
    errors = 0
    empty = 0
    t_start = time.time()

    for i, sym in enumerate(symbols, 1):
        if already_synced(conn, sym):
            skipped += 1
            if i % 200 == 0:
                print(f"[{i}/{total}] skip={skipped} ok={ok} empty={empty} err={errors}")
                sys.stdout.flush()
            continue

        data = fetch_symbol(sym)

        if data is None:
            errors += 1
            print(f"[{i}/{total}] {sym} TIMEOUT/ERROR")
            sys.stdout.flush()
            continue

        recs_15m = data.get("15m", [])
        recs_60m = data.get("60m", [])

        if not recs_15m and not recs_60m:
            empty += 1
            if empty <= 5 or empty % 100 == 0:
                print(f"[{i}/{total}] {sym} EMPTY")
                sys.stdout.flush()
            continue

        recs_120m = aggregate_bars(recs_60m, 2)
        recs_240m = aggregate_bars(recs_60m, 4)

        n15 = upsert_rows(conn, sym, "15m", recs_15m)
        n60 = upsert_rows(conn, sym, "60m", recs_60m)
        n120 = upsert_rows(conn, sym, "120m", recs_120m, is_derived=1, derived_from="60m")
        n240 = upsert_rows(conn, sym, "240m", recs_240m, is_derived=1, derived_from="60m")

        ok += 1

        if i % 20 == 0 or ok <= 3:
            elapsed = time.time() - t_start
            rate = ok / elapsed * 3600 if elapsed > 0 else 0
            remaining = total - skipped - ok - errors - empty
            eta_h = remaining / rate if rate > 0 else 0
            print(f"[{i}/{total}] {sym} 15m={n15} 60m={n60} 120m={n120} 240m={n240} "
                  f"(ok={ok} {rate:.0f}/hr ETA={eta_h:.1f}h)")
            sys.stdout.flush()

    conn.close()
    elapsed = time.time() - t_start
    print(f"\nDONE: ok={ok} skipped={skipped} empty={empty} errors={errors} total={total} elapsed={elapsed:.0f}s")


if __name__ == "__main__":
    main()
