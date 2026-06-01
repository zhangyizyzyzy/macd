#!/usr/bin/env python3
"""
watchlist_loader.py — 用户尽调题材清单加载器

约定:
  - 目录: watchlist/
  - 文件名: YYYY-MM-DD.csv (对应该日尽调结论)
  - 最小格式: 一列 symbol
  - 扩展格式: symbol, theme, note

用法:
  from watchlist_loader import load_active_watchlist
  pool = load_active_watchlist(as_of_date='2026-04-11', lookback_days=10)
  # pool = {'symbol': {'theme': '白酒', 'first_date': ..., 'notes': [...]}}

设计思想:
  - 不是"只看今天", 而是"最近 N 天任何一天出现过都算热"
    因为题材通常持续 5-15 天
  - 如果同一只股票多天出现, 合并为最早出现日期 (first_date)
"""
from __future__ import annotations

import csv
from datetime import date, datetime, timedelta
from pathlib import Path

WATCHLIST_DIR = Path("/Users/zhangyi/Documents/code/macd股票版/watchlist")


def parse_watchlist_csv(path: Path) -> list[dict]:
    """Parse one CSV file, return list of {symbol, theme, note}."""
    rows = []
    with path.open("r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for r in reader:
            sym = str(r.get("symbol", "")).strip().zfill(6)
            if not sym or not sym.isdigit():
                continue
            rows.append({
                "symbol": sym,
                "theme": str(r.get("theme", "")).strip(),
                "note": str(r.get("note", "")).strip(),
            })
    return rows


def load_active_watchlist(
    as_of_date: str | date | None = None,
    lookback_days: int = 10,
    watchlist_dir: Path = WATCHLIST_DIR,
) -> dict[str, dict]:
    """Load all stocks appearing in the last `lookback_days` watchlist files.

    Returns:
        {symbol: {
            'theme': <first non-empty theme>,
            'first_date': <earliest date appearing>,
            'last_date': <latest date appearing>,
            'notes': [list of notes]
        }}
    """
    if as_of_date is None:
        as_of = date.today()
    elif isinstance(as_of_date, str):
        as_of = datetime.strptime(as_of_date, "%Y-%m-%d").date()
    else:
        as_of = as_of_date

    earliest = as_of - timedelta(days=lookback_days - 1)

    merged: dict[str, dict] = {}
    files_found = 0
    for file in sorted(watchlist_dir.glob("*.csv")):
        if file.name.startswith("EXAMPLE") or file.name.startswith("_"):
            continue
        try:
            file_date = datetime.strptime(file.stem, "%Y-%m-%d").date()
        except ValueError:
            continue
        if file_date < earliest or file_date > as_of:
            continue
        files_found += 1
        for row in parse_watchlist_csv(file):
            sym = row["symbol"]
            if sym not in merged:
                merged[sym] = {
                    "theme": row["theme"],
                    "first_date": file_date,
                    "last_date": file_date,
                    "notes": [row["note"]] if row["note"] else [],
                }
            else:
                e = merged[sym]
                if file_date < e["first_date"]:
                    e["first_date"] = file_date
                if file_date > e["last_date"]:
                    e["last_date"] = file_date
                if not e["theme"] and row["theme"]:
                    e["theme"] = row["theme"]
                if row["note"]:
                    e["notes"].append(row["note"])

    print(f"[watchlist] as_of={as_of} lookback={lookback_days}d files={files_found} symbols={len(merged)}")
    return merged


def load_symbols_set(
    as_of_date: str | date | None = None,
    lookback_days: int = 10,
) -> set[str]:
    """Shortcut: return just the set of symbols."""
    return set(load_active_watchlist(as_of_date, lookback_days).keys())


if __name__ == "__main__":
    import sys
    as_of = sys.argv[1] if len(sys.argv) > 1 else None
    lookback = int(sys.argv[2]) if len(sys.argv) > 2 else 10
    pool = load_active_watchlist(as_of_date=as_of, lookback_days=lookback)
    if not pool:
        print("(空) 请在 watchlist/ 目录下放 YYYY-MM-DD.csv 文件")
        print("文件至少包含一列 symbol, 可选 theme, note")
    else:
        print(f"\n当前活跃股票池 ({len(pool)} 只):")
        for sym, info in sorted(pool.items()):
            theme = info['theme'] or '(未分类)'
            span = f"{info['first_date']} ~ {info['last_date']}"
            print(f"  {sym}  {theme:12s}  出现区间 {span}")
