#!/usr/bin/env python3
"""
daily_yfinance_update.py  ——  每日增量更新脚本

用 yfinance 从美国 IP 拉最新的 daily + 15m 数据, 追加到 sqlite (source='yahoo').
设计为 cron 每日 17:00 北京时间 后跑一次.

用法:
  python3 daily_yfinance_update.py                           # HS300, daily+15m
  python3 daily_yfinance_update.py --symbols 600519,000001   # 指定股票
  python3 daily_yfinance_update.py --universe all            # 全 sqlite universe
  python3 daily_yfinance_update.py --levels daily            # 只更新 daily
  python3 daily_yfinance_update.py --dry-run                 # 预演不写入

策略:
- 每只股票, 查 sqlite 里该 level 的最后日期
- 拉 yfinance 从 (last+1) 到今天
- 15m 受 yfinance 60 天窗口限制, 若 gap > 58 天则跳过并告警 (需要 BaoStock 补)
- Upsert 到 sqlite 用 source='yahoo', 不覆盖 akshare/baostock 历史数据
- 失败不中断, 错误汇总写到 outputs/data_foundation/daily_update_errors.csv

cron 推荐:
  0 17 * * 1-5 /usr/bin/python3 /path/to/daily_yfinance_update.py >> /path/to/log 2>&1
"""
from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
import warnings
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

warnings.filterwarnings("ignore")
# Suppress yfinance internal noise (e.g. "possibly delisted" on weekends)
logging.getLogger("yfinance").setLevel(logging.CRITICAL)

try:
    import yfinance as yf
except ImportError:
    print("ERROR: yfinance not installed. Run: pip3 install yfinance")
    sys.exit(1)

# ==============================
# Config
# ==============================

PROJECT_ROOT = Path("/Users/zhangyi/Documents/code/macd股票版")
DB_PATH = PROJECT_ROOT / "data" / "market_data.sqlite"
HS300_PATH = PROJECT_ROOT / "data" / "hs300_current.csv"
OUTPUT_DIR = PROJECT_ROOT / "outputs" / "data_foundation"

BEIJING_TZ = timezone(timedelta(hours=8))
SOURCE_TAG = "yahoo"
COMMIT_EVERY = 30
YFINANCE_15M_WINDOW_DAYS = 58  # yfinance 限60天, 留2天安全边际


# ==============================
# Symbol mapping
# ==============================


def to_yahoo_ticker(code: str) -> str | None:
    code = str(code).zfill(6)
    if code[0] == "6":
        return f"{code}.SS"
    if code[0] in {"0", "3"}:
        return f"{code}.SZ"
    return None


# ==============================
# Universe loaders
# ==============================


def load_hs300_symbols() -> list[str]:
    df = pd.read_csv(HS300_PATH, dtype={"symbol": str})
    return [str(s).zfill(6) for s in df["symbol"]]


def load_all_symbols_from_sqlite(con: sqlite3.Connection) -> list[str]:
    rows = con.execute(
        "SELECT DISTINCT symbol FROM kline WHERE level='15m' ORDER BY symbol"
    ).fetchall()
    return [r[0] for r in rows]


# ==============================
# Sqlite helpers
# ==============================


def last_date(con: sqlite3.Connection, symbol: str, level: str) -> date | None:
    """Return last date across ALL sources for this symbol/level."""
    row = con.execute(
        "SELECT MAX(date) FROM kline WHERE symbol=? AND level=? AND adjust='qfq'",
        (symbol, level),
    ).fetchone()[0]
    return pd.to_datetime(row).date() if row else None


def upsert_bars(con: sqlite3.Connection, symbol: str, level: str, df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = []
    for idx, r in df.iterrows():
        if pd.isna(r["close"]):
            continue
        if level == "daily":
            date_str = pd.Timestamp(idx).strftime("%Y-%m-%d 00:00:00")
        else:
            date_str = pd.Timestamp(idx).strftime("%Y-%m-%d %H:%M:%S")
        rows.append(
            (
                symbol,
                level,
                "qfq",
                SOURCE_TAG,
                date_str,
                float(r["open"]),
                float(r["high"]),
                float(r["low"]),
                float(r["close"]),
                float(r["volume"]) if pd.notna(r["volume"]) else 0.0,
                None,
                None,
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


# ==============================
# yfinance fetcher
# ==============================


def fetch_yfinance(ticker: str, start: date, end: date, interval: str) -> pd.DataFrame | None:
    df = yf.download(
        ticker,
        start=start.strftime("%Y-%m-%d"),
        end=(end + timedelta(days=1)).strftime("%Y-%m-%d"),
        interval=interval,
        progress=False,
        auto_adjust=False,
        threads=False,
    )
    if df is None or df.empty:
        return None
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df = df.rename(
        columns={"Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume"}
    )
    # Intraday: convert UTC -> Beijing, strip tz
    if interval != "1d" and df.index.tz is not None:
        df.index = df.index.tz_convert(BEIJING_TZ).tz_localize(None)
    return df[["open", "high", "low", "close", "volume"]]


# ==============================
# Main
# ==============================


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--universe", type=str, default="hs300", choices=["hs300", "all"])
    parser.add_argument("--symbols", type=str, default=None, help="comma-separated symbols, overrides universe")
    parser.add_argument("--levels", type=str, default="daily,15m", help="comma-separated: daily,15m")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    levels = [lv.strip() for lv in args.levels.split(",") if lv.strip()]
    for lv in levels:
        if lv not in {"daily", "15m"}:
            print(f"ERROR: unsupported level '{lv}' (only daily/15m)")
            sys.exit(1)

    con = sqlite3.connect(str(DB_PATH))

    # Load universe
    if args.symbols:
        symbols = [s.strip().zfill(6) for s in args.symbols.split(",")]
    elif args.universe == "hs300":
        symbols = load_hs300_symbols()
    else:
        symbols = load_all_symbols_from_sqlite(con)

    if args.limit:
        symbols = symbols[: args.limit]

    today = datetime.now(BEIJING_TZ).date()
    ts_start = datetime.now()
    print(f"[Update] {ts_start.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[Update] universe={args.universe} symbols={len(symbols)} levels={levels}")
    print(f"[Update] Today (Beijing): {today}")

    # Stats
    total_bars = {lv: 0 for lv in levels}
    ok_syms = {lv: 0 for lv in levels}
    errors: list[dict] = []
    too_stale_15m: list[dict] = []
    unmapped: list[str] = []

    for i, sym in enumerate(symbols):
        ticker = to_yahoo_ticker(sym)
        if ticker is None:
            unmapped.append(sym)
            continue

        for lv in levels:
            last = last_date(con, sym, lv)
            if last is None:
                # No existing data - pull 4y for daily, 58d for 15m
                if lv == "daily":
                    start = today - timedelta(days=4 * 365)
                else:
                    start = today - timedelta(days=YFINANCE_15M_WINDOW_DAYS)
            elif last >= today:
                continue  # up to date
            else:
                start = last + timedelta(days=1)

            # Skip if start is in the future (weekend, holiday)
            if start > today:
                continue

            # Guard: 15m must be within yfinance window
            if lv == "15m":
                window_floor = today - timedelta(days=YFINANCE_15M_WINDOW_DAYS)
                if start < window_floor:
                    too_stale_15m.append({
                        "symbol": sym,
                        "last_date": str(last) if last else "none",
                        "gap_days": (today - (last or today)).days,
                    })
                    start = window_floor

            interval = "1d" if lv == "daily" else "15m"
            try:
                df = fetch_yfinance(ticker, start, today, interval)
                if df is None or df.empty:
                    continue

                # Filter out bars that are <= last date (yfinance sometimes returns
                # bars just before the requested start)
                if last is not None:
                    if lv == "daily":
                        df = df[df.index.date > last]
                    else:
                        df = df[df.index > pd.Timestamp(last) + pd.Timedelta(days=1)]
                if df.empty:
                    continue

                if args.dry_run:
                    n = len(df)
                    if not args.quiet:
                        print(f"  [DRY] {sym} {lv}: would insert {n} bars ({df.index[0]} -> {df.index[-1]})")
                else:
                    n = upsert_bars(con, sym, lv, df)
                total_bars[lv] += n
                ok_syms[lv] += 1
            except Exception as exc:
                errors.append({"symbol": sym, "level": lv, "error": str(exc)[:120]})

        if (i + 1) % COMMIT_EVERY == 0:
            if not args.dry_run:
                con.commit()
            if not args.quiet:
                progress = ", ".join(f"{lv}={total_bars[lv]}" for lv in levels)
                print(f"  [{i+1:4d}/{len(symbols)}] {progress} errors={len(errors)}")

    if not args.dry_run:
        con.commit()

    # Error output
    if errors:
        err_path = OUTPUT_DIR / "daily_update_errors.csv"
        pd.DataFrame(errors).to_csv(err_path, index=False)
        print(f"[Errors] {len(errors)} -> {err_path}")

    if too_stale_15m:
        stale_path = OUTPUT_DIR / "too_stale_15m.csv"
        pd.DataFrame(too_stale_15m).to_csv(stale_path, index=False)
        print(f"[Warn] {len(too_stale_15m)} symbols have 15m gap > 58 days -> {stale_path}")
        print(f"       用 BaoStock (关 VPN) 补这些: python3 fill_baostock_15m.py --shopping-list {stale_path}")

    ts_end = datetime.now()
    elapsed = (ts_end - ts_start).total_seconds()

    print()
    print("=" * 70)
    print("DAILY UPDATE COMPLETE")
    print("=" * 70)
    print(f"  Elapsed: {elapsed:.1f}s")
    for lv in levels:
        print(f"  {lv:5s}: {total_bars[lv]:>7d} bars across {ok_syms[lv]:>4d} symbols")
    if errors:
        print(f"  errors: {len(errors)}")
    if unmapped:
        print(f"  unmapped (北交所等): {len(unmapped)}")

    con.close()


if __name__ == "__main__":
    main()
