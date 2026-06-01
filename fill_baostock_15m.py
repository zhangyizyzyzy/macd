#!/usr/bin/env python3
"""
fill_baostock_15m.py  ——  VPN off (国内 IP) 专用

读 outputs/data_foundation/shopping_list_15m_need_baostock.csv
用 BaoStock 把每个 symbol 的 15m 缺口补到今天, 落盘到 sqlite (source='baostock')

用法:
  # 1. 关 VPN, 切到国内 IP
  # 2. 运行:
  python3 fill_baostock_15m.py

  # 选项:
  python3 fill_baostock_15m.py --limit 10      # 只跑前 10 只
  python3 fill_baostock_15m.py --resume        # 跳过已经有数据的 (断点续跑)
  python3 fill_baostock_15m.py --dry-run       # 不写入 sqlite, 只打印会做什么

脚本特性:
- 自动断线重连 BaoStock
- 每 10 只 commit 一次, 中途 Ctrl+C 也不丢数据
- 失败列表输出到 outputs/data_foundation/baostock_fetch_errors.csv
- 幂等 (INSERT OR REPLACE by primary key)
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
import warnings
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

warnings.filterwarnings("ignore")

try:
    import baostock as bs
except ImportError:
    print("ERROR: baostock not installed. Run: pip3 install baostock")
    sys.exit(1)

# ==============================
# Config
# ==============================

PROJECT_ROOT = Path("/Users/zhangyi/Documents/code/macd股票版")
DB_PATH = PROJECT_ROOT / "data" / "market_data.sqlite"
SHOPPING_LIST_PATH = PROJECT_ROOT / "outputs" / "data_foundation" / "shopping_list_15m_need_baostock.csv"
OUTPUT_DIR = PROJECT_ROOT / "outputs" / "data_foundation"

SOURCE_TAG = "baostock"
COMMIT_EVERY = 10


# ==============================
# Symbol mapping
# ==============================


def to_baostock_code(symbol: str) -> str | None:
    """Convert 6-digit A-share code to BaoStock format.
    600/601/603/605/688 -> sh.xxxxxx
    000/001/002/003/300 -> sz.xxxxxx
    北交所 4/8 -> bj.xxxxxx (部分 BaoStock 不支持)
    """
    code = str(symbol).zfill(6)
    if code[0] == "6":
        return f"sh.{code}"
    if code[0] in {"0", "3"}:
        return f"sz.{code}"
    if code[0] in {"4", "8"}:
        return f"bj.{code}"
    return None


# ==============================
# BaoStock wrapper
# ==============================


class BaoStockSession:
    def __init__(self):
        self.logged_in = False

    def login(self):
        if self.logged_in:
            return
        lg = bs.login()
        if lg.error_code != "0":
            raise RuntimeError(f"BaoStock login failed: {lg.error_msg}")
        self.logged_in = True
        print(f"[BaoStock] logged in (code={lg.error_code})")

    def logout(self):
        if self.logged_in:
            bs.logout()
            self.logged_in = False

    def fetch_15m(self, bs_code: str, start_date: str, end_date: str) -> pd.DataFrame | None:
        """Fetch 15m bars from BaoStock. Returns DataFrame or None on empty/error."""
        self.login()
        fields = "date,time,code,open,high,low,close,volume,amount,adjustflag"
        rs = bs.query_history_k_data_plus(
            bs_code,
            fields,
            start_date=start_date,
            end_date=end_date,
            frequency="15",
            adjustflag="2",  # 前复权
        )
        if rs.error_code != "0":
            raise RuntimeError(f"query failed for {bs_code}: {rs.error_msg}")

        rows = []
        while rs.error_code == "0" and rs.next():
            rows.append(rs.get_row_data())
        if not rows:
            return None

        df = pd.DataFrame(rows, columns=rs.fields)
        # BaoStock 'time' field: 20260311093500000 (YYYYMMDDHHMMSSmsec)
        # Parse to Beijing datetime
        df["datetime"] = pd.to_datetime(df["time"].str[:14], format="%Y%m%d%H%M%S")
        df = df.set_index("datetime")
        for c in ["open", "high", "low", "close", "volume", "amount"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        return df[["open", "high", "low", "close", "volume", "amount"]]


# ==============================
# Upsert
# ==============================


def upsert_15m(con: sqlite3.Connection, symbol: str, df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = []
    for idx, r in df.iterrows():
        if pd.isna(r["close"]):
            continue
        date_str = idx.strftime("%Y-%m-%d %H:%M:%S")
        rows.append(
            (
                symbol,
                "15m",
                "qfq",
                SOURCE_TAG,
                date_str,
                float(r["open"]),
                float(r["high"]),
                float(r["low"]),
                float(r["close"]),
                float(r["volume"]) if pd.notna(r["volume"]) else 0.0,
                float(r["amount"]) if pd.notna(r["amount"]) else None,
                None,  # turnover
                0,
                None,
                now,
            )
        )
    if not rows:
        return 0
    con.executemany(
        """
        INSERT OR REPLACE INTO kline
        (symbol, level, adjust, source, date, open, high, low, close,
         volume, amount, turnover, is_derived, derived_from, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    return len(rows)


def last_date_in_sqlite(con: sqlite3.Connection, symbol: str) -> date | None:
    row = con.execute(
        "SELECT MAX(date) FROM kline WHERE symbol=? AND level='15m' AND adjust='qfq'",
        (symbol,),
    ).fetchone()[0]
    return pd.to_datetime(row).date() if row else None


# ==============================
# Main
# ==============================


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None, help="limit number of symbols")
    parser.add_argument("--resume", action="store_true", help="skip symbols already up-to-date")
    parser.add_argument("--dry-run", action="store_true", help="do not write to sqlite")
    parser.add_argument("--shopping-list", type=str, default=None, help="override shopping list path")
    args = parser.parse_args()

    shopping_path = Path(args.shopping_list) if args.shopping_list else SHOPPING_LIST_PATH
    if not shopping_path.exists():
        print(f"ERROR: shopping list not found: {shopping_path}")
        print("先跑 bootstrap_yfinance_fill.py 生成 shopping list")
        sys.exit(1)

    shopping = pd.read_csv(shopping_path, dtype={"symbol": str})
    shopping["symbol"] = shopping["symbol"].str.zfill(6)
    print(f"[Shop] Loaded {len(shopping)} rows from {shopping_path}")

    if args.limit:
        shopping = shopping.head(args.limit)
        print(f"[Shop] Limiting to first {len(shopping)} symbols")

    # Today as end date (Beijing), extend by 1 day to include today
    today = (datetime.now() + timedelta(hours=8)).date()  # approximate Beijing
    print(f"[Shop] Target end date: {today}")

    con = sqlite3.connect(str(DB_PATH))
    session = BaoStockSession()

    total = len(shopping)
    processed = 0
    total_bars = 0
    skipped = 0
    failures: list[dict] = []
    unmapped: list[str] = []

    try:
        for i, row in shopping.reset_index(drop=True).iterrows():
            sym = row["symbol"]
            start_date = row["start_date"]
            end_date = today.strftime("%Y-%m-%d")

            bs_code = to_baostock_code(sym)
            if bs_code is None:
                unmapped.append(sym)
                continue

            # Resume mode: skip if already up to today-1
            if args.resume:
                last = last_date_in_sqlite(con, sym)
                if last and last >= today - timedelta(days=1):
                    skipped += 1
                    continue
                if last:
                    start_date = (last + timedelta(days=1)).strftime("%Y-%m-%d")

            try:
                df = session.fetch_15m(bs_code, start_date, end_date)
                if df is None or df.empty:
                    failures.append({"symbol": sym, "bs_code": bs_code, "error": "empty"})
                    continue

                if args.dry_run:
                    n = len(df)
                    print(f"  [DRY] {sym} ({bs_code}): would insert {n} bars "
                          f"{df.index[0]} -> {df.index[-1]}")
                else:
                    n = upsert_15m(con, sym, df)
                    total_bars += n

                processed += 1
            except KeyboardInterrupt:
                print(f"\n[INTERRUPT] committing partial progress at {i+1}/{total}")
                con.commit()
                raise
            except Exception as exc:
                failures.append(
                    {"symbol": sym, "bs_code": bs_code, "error": str(exc)[:120]}
                )

            if (i + 1) % COMMIT_EVERY == 0:
                if not args.dry_run:
                    con.commit()
                print(
                    f"  [{i+1:3d}/{total}] bars={total_bars} "
                    f"ok={processed} fail={len(failures)} skip={skipped} last={sym}"
                )
    finally:
        if not args.dry_run:
            con.commit()
        session.logout()

    # Outputs
    if failures:
        err_path = OUTPUT_DIR / "baostock_fetch_errors.csv"
        pd.DataFrame(failures).to_csv(err_path, index=False)
        print(f"[Errors] {len(failures)} failures -> {err_path}")

    if unmapped:
        print(f"[Skip] {len(unmapped)} unmapped symbols (北交所等): {unmapped[:5]}...")

    print()
    print("=" * 70)
    print("BAOSTOCK FILL COMPLETE")
    print("=" * 70)
    print(f"  Processed: {processed}/{total}")
    print(f"  Total 15m bars inserted: {total_bars}")
    print(f"  Skipped (resume):        {skipped}")
    print(f"  Failures:                {len(failures)}")
    print(f"  Unmapped:                {len(unmapped)}")
    print()
    print("NEXT STEP:")
    print("  1. 恢复 VPN (切回美国 IP)")
    print("  2. 以后每天 yfinance 增量更新日线 (我稍后写那个脚本)")

    con.close()


if __name__ == "__main__":
    main()
