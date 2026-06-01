#!/usr/bin/env python3
"""
bootstrap_yfinance_fill.py  ——  一次性引导脚本

功能:
1. 审计 sqlite 里 HS300 每只股票 daily / 15m 的最后日期
2. 用 yfinance (source='yahoo') 把 daily 缺口补到今天 (美国 IP 友好)
3. 输出 15m 缺口 shopping list，让用户关 VPN 后用 BaoStock 补
4. 不触碰现有 akshare / baostock 数据，yahoo 作为新 source 独立存储

用法:
  python3 bootstrap_yfinance_fill.py              # 审计 + 填充
  python3 bootstrap_yfinance_fill.py --audit-only # 只审计不写入
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
import warnings
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

warnings.filterwarnings("ignore")

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
DAILY_FILL_BATCH_COMMIT = 30


# ==============================
# Symbol mapping
# ==============================


def to_yahoo_ticker(code: str) -> str | None:
    """Convert A-share code to yfinance ticker.

    600xxx / 601xxx / 603xxx / 605xxx / 688xxx -> .SS  (上交所主板 / 科创板)
    000xxx / 001xxx / 002xxx / 003xxx / 300xxx -> .SZ  (深交所主板 / 创业板)
    北交所 4xxxxx/8xxxxx/9xxxxx -> 先跳过 (yfinance 覆盖差)
    """
    code = str(code).zfill(6)
    if code[0] == "6":
        return f"{code}.SS"
    if code[0] in {"0", "3"}:
        return f"{code}.SZ"
    return None


# ==============================
# Audit
# ==============================


def audit_sqlite(con: sqlite3.Connection, symbols: list[str]) -> pd.DataFrame:
    """Return DataFrame with columns: symbol, daily_last, m15_last."""
    rows = []
    for sym in symbols:
        d_last = con.execute(
            "SELECT MAX(date) FROM kline WHERE symbol=? AND level='daily' AND adjust='qfq'",
            (sym,),
        ).fetchone()[0]
        m_last = con.execute(
            "SELECT MAX(date) FROM kline WHERE symbol=? AND level='15m' AND adjust='qfq'",
            (sym,),
        ).fetchone()[0]
        rows.append(
            {
                "symbol": sym,
                "daily_last": pd.to_datetime(d_last).date() if d_last else None,
                "m15_last": pd.to_datetime(m_last).date() if m_last else None,
            }
        )
    return pd.DataFrame(rows)


def print_audit_report(inventory: pd.DataFrame, today: date) -> None:
    print()
    print("=" * 70)
    print("AUDIT REPORT")
    print("=" * 70)
    total = len(inventory)

    daily_values = [d for d in inventory["daily_last"].tolist() if d is not None]
    if daily_values:
        d_oldest = min(daily_values)
        d_newest = max(daily_values)
        d_gap = (today - d_newest).days
        print(f"  daily: {len(daily_values)}/{total} symbols")
        print(f"    oldest last_date: {d_oldest}")
        print(f"    newest last_date: {d_newest}  (gap to today: {d_gap}d)")
    else:
        print(f"  daily: NO DATA")

    m15_values = [d for d in inventory["m15_last"].tolist() if d is not None]
    if m15_values:
        m_oldest = min(m15_values)
        m_newest = max(m15_values)
        m_gap = (today - m_newest).days
        print(f"  15m:   {len(m15_values)}/{total} symbols")
        print(f"    oldest last_date: {m_oldest}")
        print(f"    newest last_date: {m_newest}  (gap to today: {m_gap}d)")
    else:
        print(f"  15m:   NO DATA")

    missing = inventory[inventory["daily_last"].isna() & inventory["m15_last"].isna()]
    if len(missing):
        print(f"  completely missing: {len(missing)} symbols")
    print()


# ==============================
# yfinance fetchers
# ==============================


def fetch_daily(ticker: str, start: date, end: date) -> pd.DataFrame | None:
    df = yf.download(
        ticker,
        start=start.strftime("%Y-%m-%d"),
        end=(end + timedelta(days=1)).strftime("%Y-%m-%d"),  # yfinance end exclusive
        interval="1d",
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
    return df[["open", "high", "low", "close", "volume"]]


# ==============================
# Upsert into sqlite
# ==============================


def upsert_daily(con: sqlite3.Connection, symbol: str, df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = []
    for idx, r in df.iterrows():
        if pd.isna(r["close"]):
            continue
        date_str = pd.Timestamp(idx).strftime("%Y-%m-%d 00:00:00")
        rows.append(
            (
                symbol,
                "daily",
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
# Main runner
# ==============================


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--audit-only", action="store_true", help="Only print audit, no fetch")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of symbols")
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    hs300 = pd.read_csv(HS300_PATH, dtype={"symbol": str})
    symbols = [str(s).zfill(6) for s in hs300["symbol"]]
    if args.limit:
        symbols = symbols[: args.limit]
    print(f"[Bootstrap] HS300 universe: {len(symbols)} symbols")

    con = sqlite3.connect(str(DB_PATH))
    print(f"[Audit] Scanning sqlite for last dates...")
    inventory = audit_sqlite(con, symbols)

    today = datetime.now(BEIJING_TZ).date()
    print(f"[Audit] Today (Beijing): {today}")
    print_audit_report(inventory, today)

    # ---- Compute gaps ----
    gaps_daily: list[tuple[str, str, date, date]] = []
    gaps_15m_shopping: list[dict] = []
    missing_ticker: list[str] = []

    for _, row in inventory.iterrows():
        sym = row["symbol"]
        ticker = to_yahoo_ticker(sym)
        if ticker is None:
            missing_ticker.append(sym)
            continue

        # daily gap
        if row["daily_last"] is None:
            gaps_daily.append((sym, ticker, today - timedelta(days=4 * 365), today))
        elif row["daily_last"] < today:
            gaps_daily.append((sym, ticker, row["daily_last"] + timedelta(days=1), today))

        # 15m gap (yfinance can't backfill > 60d, so report as shopping list)
        if row["m15_last"] is None:
            gap_start = today - timedelta(days=4 * 365)
            gaps_15m_shopping.append(
                {
                    "symbol": sym,
                    "start_date": gap_start.strftime("%Y-%m-%d"),
                    "end_date": today.strftime("%Y-%m-%d"),
                    "note": "completely missing",
                }
            )
        elif row["m15_last"] < today:
            gaps_15m_shopping.append(
                {
                    "symbol": sym,
                    "start_date": (row["m15_last"] + timedelta(days=1)).strftime("%Y-%m-%d"),
                    "end_date": today.strftime("%Y-%m-%d"),
                    "note": f"incremental since {row['m15_last']}",
                }
            )

    print(f"[Plan] Daily gaps to fill via yfinance: {len(gaps_daily)} symbols")
    print(f"[Plan] 15m gaps for shopping list (needs BaoStock/VPN): {len(gaps_15m_shopping)} symbols")
    if missing_ticker:
        print(f"[Plan] Unknown ticker mapping (北交所等): {len(missing_ticker)} symbols -> skipped")

    # ---- Write shopping list ----
    if gaps_15m_shopping:
        sl_path = OUTPUT_DIR / "shopping_list_15m_need_baostock.csv"
        pd.DataFrame(gaps_15m_shopping).to_csv(sl_path, index=False)
        print(f"[Shop] 15m shopping list saved: {sl_path}")

    if missing_ticker:
        mt_path = OUTPUT_DIR / "unmapped_symbols.csv"
        pd.DataFrame({"symbol": missing_ticker}).to_csv(mt_path, index=False)
        print(f"[Shop] Unmapped symbols saved: {mt_path}")

    if args.audit_only:
        print("\n[Audit-only mode] No writes performed.")
        con.close()
        return

    # ---- Fill daily via yfinance ----
    print()
    print("=" * 70)
    print("FILLING DAILY VIA YFINANCE (source='yahoo')")
    print("=" * 70)
    print(f"Processing {len(gaps_daily)} symbols...")

    daily_bars_filled = 0
    daily_fails: list[tuple[str, str]] = []

    for i, (sym, ticker, start, end) in enumerate(gaps_daily):
        try:
            df = fetch_daily(ticker, start, end)
            if df is None:
                daily_fails.append((sym, "empty"))
            else:
                n = upsert_daily(con, sym, df)
                daily_bars_filled += n
        except KeyboardInterrupt:
            print("\nInterrupted by user, committing partial progress...")
            con.commit()
            raise
        except Exception as e:
            daily_fails.append((sym, str(e)[:80]))

        if (i + 1) % DAILY_FILL_BATCH_COMMIT == 0:
            con.commit()
            print(
                f"  [{i+1:3d}/{len(gaps_daily)}] filled={daily_bars_filled} "
                f"fails={len(daily_fails)} last={sym}"
            )

    con.commit()
    print(
        f"  DONE: {daily_bars_filled} bars across "
        f"{len(gaps_daily) - len(daily_fails)} symbols ({len(daily_fails)} failed)"
    )

    # ---- Error log ----
    if daily_fails:
        err_path = OUTPUT_DIR / "bootstrap_fetch_errors.csv"
        pd.DataFrame(daily_fails, columns=["symbol", "error"]).to_csv(err_path, index=False)
        print(f"[Errors] Saved: {err_path}")

    # ---- Summary ----
    print()
    print("=" * 70)
    print("BOOTSTRAP COMPLETE")
    print("=" * 70)
    print(f"  Daily filled from yfinance: {daily_bars_filled} bars")
    print(f"  Symbols updated:            {len(gaps_daily) - len(daily_fails)}")
    print(f"  Symbols failed:             {len(daily_fails)}")
    print(f"  15m shopping list (BaoStock side): {len(gaps_15m_shopping)} symbols")
    print()
    print("NEXT STEP:")
    print("  1. 关闭 VPN (切到国内 IP)")
    print(f"  2. 根据 {OUTPUT_DIR}/shopping_list_15m_need_baostock.csv 用 BaoStock 补 15m 数据")
    print(f"  3. 恢复 VPN, 以后每天 17:00 cron 跑 yfinance 增量更新 daily")

    con.close()


if __name__ == "__main__":
    main()
