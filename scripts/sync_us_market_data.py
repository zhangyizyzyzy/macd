#!/usr/bin/env python3
"""Download US stock daily/weekly data via yfinance into local SQLite."""
from __future__ import annotations

import argparse
import sqlite3
import time
from pathlib import Path

import pandas as pd
import yfinance as yf

# Extra symbols not in S&P 500 but worth tracking
EXTRA_SYMBOLS = [
    # Chinese ADR
    "BABA", "JD", "PDD", "BIDU", "NIO", "XPEV", "LI", "TME", "BILI", "IQ",
    "FUTU", "TIGR", "TAL", "EDU", "ZH", "MNSO",
    # Meme / Retail favorites
    "GME", "AMC", "RIVN", "LCID", "HOOD", "SOFI", "PLTR", "IONQ", "RKLB",
    # Crypto-related
    "COIN", "MSTR", "MARA", "RIOT", "CLSK",
    # Popular mid-cap tech
    "SMCI", "ARM", "CRWD", "DDOG", "ZS", "NET", "SNOW", "MDB", "CFLT", "S",
    "RBLX", "U", "DOCS", "GTLB", "ESTC", "PATH",
    # ETFs
    "SPY", "QQQ", "IWM", "DIA", "ARKK", "ARKG", "ARKF",
    "XLF", "XLE", "XLK", "XLV", "XLI", "XLP", "XLU", "XLB", "XLRE",
    "SOXX", "SMH", "TAN", "LIT", "KWEB", "FXI", "EEM", "VWO",
    "GLD", "SLV", "TLT", "HYG", "VIX",
]


def fetch_sp500_symbols() -> list[str]:
    """Fetch S&P 500 component list from Wikipedia."""
    try:
        tables = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")
        df = tables[0]
        symbols = df["Symbol"].str.replace(".", "-", regex=False).tolist()
        print(f"fetched S&P 500: {len(symbols)} symbols")
        return symbols
    except Exception as e:
        print(f"failed to fetch S&P 500 list: {e}, using fallback")
        return []


FALLBACK_SP500_TOP100 = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "BRK-B", "UNH", "JNJ",
    "V", "XOM", "JPM", "PG", "MA", "HD", "CVX", "MRK", "ABBV", "LLY",
    "PEP", "KO", "COST", "AVGO", "MCD", "WMT", "CSCO", "TMO", "ABT", "CRM",
    "ACN", "DHR", "ADBE", "NKE", "TXN", "NEE", "PM", "BMY", "UNP", "RTX",
    "ORCL", "HON", "QCOM", "LOW", "COP", "UPS", "MS", "GS", "INTC", "BA",
    "CAT", "AMD", "AMAT", "AMGN", "DE", "SBUX", "MDLZ", "GE", "ADI", "GILD",
    "LMT", "BKNG", "ADP", "SYK", "PLD", "MMC", "ISRG", "CB", "TMUS", "VRTX",
    "BLK", "CI", "SCHW", "AMT", "MO", "DUK", "SO", "ZTS", "CME", "PYPL",
    "REGN", "NOC", "PGR", "SLB", "BDX", "ICE", "BSX", "AON", "CSX", "LRCX",
    "FISV", "WM", "CL", "KLAC", "MU", "EQIX", "MCK", "NSC", "SNPS", "CDNS",
]


def get_default_symbols() -> list[str]:
    """Get full symbol list: S&P 500 + extra."""
    sp500 = fetch_sp500_symbols()
    if not sp500:
        sp500 = FALLBACK_SP500_TOP100
    all_symbols = list(dict.fromkeys(sp500 + EXTRA_SYMBOLS))  # deduplicate, preserve order
    return all_symbols


def ensure_us_tables(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS us_kline (
            symbol TEXT NOT NULL,
            level TEXT NOT NULL,
            date TEXT NOT NULL,
            open REAL, high REAL, low REAL, close REAL,
            volume REAL, amount REAL, turnover REAL,
            PRIMARY KEY (symbol, level, date)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS us_sync_log (
            symbol TEXT NOT NULL,
            level TEXT NOT NULL,
            rows_written INTEGER,
            min_date TEXT, max_date TEXT,
            synced_at TEXT,
            PRIMARY KEY (symbol, level)
        )
    """)
    conn.commit()


def sync_symbol(conn: sqlite3.Connection, symbol: str, level: str, start: str, end: str) -> int:
    interval_map = {"daily": "1d", "weekly": "1wk", "60m": "1h", "15m": "15m"}
    interval = interval_map.get(level, "1d")

    # yfinance intraday limits: 15m=60 days, 60m=730 days
    if level == "15m":
        start = max(start, (pd.Timestamp.now() - pd.Timedelta(days=59)).strftime("%Y-%m-%d"))
    elif level == "60m":
        start = max(start, (pd.Timestamp.now() - pd.Timedelta(days=729)).strftime("%Y-%m-%d"))

    ticker = yf.Ticker(symbol)
    df = ticker.history(start=start, end=end, interval=interval, auto_adjust=True)

    if df.empty:
        return 0

    df = df.reset_index()
    df.columns = [c.lower() for c in df.columns]
    df = df.rename(columns={"date": "date"})
    if "datetime" in df.columns:
        df = df.rename(columns={"datetime": "date"})
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d %H:%M:%S")

    rows = []
    for _, r in df.iterrows():
        rows.append((
            symbol, level, r["date"],
            float(r.get("open", 0)), float(r.get("high", 0)),
            float(r.get("low", 0)), float(r.get("close", 0)),
            float(r.get("volume", 0)), 0.0, 0.0,
        ))

    conn.executemany("""
        INSERT OR REPLACE INTO us_kline
        (symbol, level, date, open, high, low, close, volume, amount, turnover)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)

    conn.execute("""
        INSERT OR REPLACE INTO us_sync_log (symbol, level, rows_written, min_date, max_date, synced_at)
        VALUES (?, ?, ?, ?, ?, datetime('now'))
    """, (symbol, level, len(rows), rows[0][2] if rows else None, rows[-1][2] if rows else None))

    conn.commit()
    return len(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync US stock data via yfinance")
    parser.add_argument("--symbols", default="")
    parser.add_argument("--start-date", default="2022-01-01")
    parser.add_argument("--end-date", default="2030-01-01")
    parser.add_argument("--levels", default="daily,weekly")
    parser.add_argument("--db-path", default="data/market_data.sqlite")
    parser.add_argument("--sleep", type=float, default=0.5)
    args = parser.parse_args()

    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()] if args.symbols else get_default_symbols()
    levels = [l.strip() for l in args.levels.split(",")]
    db_path = Path(args.db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path))
    ensure_us_tables(conn)

    end_date = min(pd.Timestamp(args.end_date), pd.Timestamp.now()).strftime("%Y-%m-%d")
    total = len(symbols)

    print(f"us_market_sync symbols={total} levels={levels} start={args.start_date} end={end_date}")

    for i, sym in enumerate(symbols, 1):
        print(f"[{i}/{total}] syncing {sym}")
        for level in levels:
            try:
                n = sync_symbol(conn, sym, level, args.start_date, end_date)
                print(f"  {sym} {level} ok rows={n}")
            except Exception as e:
                print(f"  {sym} {level} ERROR {e}")
        time.sleep(args.sleep)

    conn.close()
    print("us_market_sync finished")


if __name__ == "__main__":
    main()
