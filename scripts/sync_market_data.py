#!/usr/bin/env python3
from __future__ import annotations

import argparse
from dataclasses import replace
from pathlib import Path
import sys
import time

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from market_data_store import MarketDataStore
from macd_time_signal_scanner import AKShareProvider, PROJECT_VERSION, ScanConfig
from macd_timeframe_backtest import (
    AGGREGATED_BASE_LEVEL,
    MINUTE_LEVEL_MAP,
    aggregate_intraday,
    minute_to_daily_window,
    normalize_minute_history,
    query_baostock_history,
)


LEVEL_CHOICES = ["15m", "30m", "60m", "120m", "240m", "daily", "weekly", "monthly"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync A-share market data into a local SQLite database")
    parser.add_argument("--start-date", default="20220101")
    parser.add_argument("--end-date", default="20300101")
    parser.add_argument("--adjust", default="qfq", choices=["", "qfq", "hfq"])
    parser.add_argument("--levels", default="15m,60m,120m,240m,daily,weekly,monthly")
    parser.add_argument("--symbols", default="")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--batch-size", type=int, default=50)
    parser.add_argument("--batch-index", type=int, default=1)
    parser.add_argument("--min-amount", type=float, default=1.0e8)
    parser.add_argument("--min-price", type=float, default=2.0)
    parser.add_argument("--include-st", action="store_true")
    parser.add_argument("--include-delisting", action="store_true")
    parser.add_argument("--minute-source", default="baostock", choices=["baostock", "eastmoney"])
    parser.add_argument("--db-path", default=str(Path("data") / "market_data.sqlite"))
    parser.add_argument("--universe-cache", default=str(Path("data") / "universe_latest.csv"))
    parser.add_argument("--universe-retries", type=int, default=3)
    parser.add_argument("--prefer-cached-universe", action="store_true")
    parser.add_argument("--bootstrap-backtest-db", default=str(Path("outputs") / "backtest_store.sqlite"))
    parser.add_argument("--skip-bootstrap", action="store_true")
    return parser.parse_args()


def build_config(args: argparse.Namespace) -> ScanConfig:
    return ScanConfig(
        start_date=args.start_date,
        end_date=args.end_date,
        adjust=args.adjust,
        period="daily",
        workers=1,
        min_amount=args.min_amount,
        min_price=args.min_price,
        limit=args.limit,
        recent_bars=9999,
        latest_only=False,
        exclude_st=not args.include_st,
        exclude_delisting=not args.include_delisting,
    )


def sync_bounds(level: str, cfg: ScanConfig) -> tuple[str, str]:
    start = pd.Timestamp(cfg.start_date).normalize()
    requested_end = pd.Timestamp(cfg.end_date)
    now = pd.Timestamp.now()
    if level in MINUTE_LEVEL_MAP:
        effective_end = min(requested_end + pd.Timedelta(hours=23, minutes=59, seconds=59), now.floor("min"))
    else:
        effective_end = min(requested_end.normalize(), now.normalize())
    return start.strftime("%Y-%m-%d %H:%M:%S"), effective_end.strftime("%Y-%m-%d %H:%M:%S")


def fetch_raw_15m(provider: AKShareProvider, symbol: str, cfg: ScanConfig, minute_source: str) -> pd.DataFrame:
    if minute_source == "baostock":
        return query_baostock_history(
            symbol=symbol,
            start_date=pd.Timestamp(cfg.start_date).strftime("%Y-%m-%d"),
            end_date=pd.Timestamp(cfg.end_date).strftime("%Y-%m-%d"),
            frequency="15",
            adjust=cfg.adjust,
        )

    minute_start, minute_end = minute_to_daily_window(cfg.start_date, cfg.end_date)
    raw = provider.ak.stock_zh_a_hist_min_em(
        symbol=symbol,
        start_date=minute_start,
        end_date=minute_end,
        period="15",
        adjust=cfg.adjust,
    )
    if raw is None or raw.empty:
        return pd.DataFrame()
    return normalize_minute_history(raw)


def resolve_universe(args: argparse.Namespace, cfg: ScanConfig, provider: AKShareProvider) -> pd.DataFrame:
    if args.symbols:
        items = [item.strip() for item in args.symbols.split(",") if item.strip()]
        return pd.DataFrame([{"symbol": item, "name": ""} for item in items])

    cache_path = Path(args.universe_cache).expanduser().resolve()
    if args.prefer_cached_universe and cache_path.exists():
        print(f"using cached universe -> {cache_path}")
        return pd.read_csv(cache_path, dtype={"symbol": str})

    last_error: Exception | None = None
    for attempt in range(1, max(args.universe_retries, 1) + 1):
        try:
            universe = provider.get_universe(cfg)
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            universe.to_csv(cache_path, index=False, encoding="utf-8-sig")
            return universe
        except Exception as exc:
            last_error = exc
            print(f"universe fetch attempt {attempt}/{args.universe_retries} failed: {exc}")
            if attempt < args.universe_retries:
                time.sleep(3)

    if cache_path.exists():
        print(f"using cached universe -> {cache_path}")
        cached = pd.read_csv(cache_path, dtype={"symbol": str})
        return cached
    if last_error is not None:
        raise last_error
    return pd.DataFrame()


def slice_batch(universe: pd.DataFrame, limit: int | None, batch_size: int, batch_index: int) -> tuple[pd.DataFrame, int]:
    work = universe.copy()
    if limit is not None:
        work = work.head(limit).copy()
    if batch_size <= 0:
        return work.reset_index(drop=True), 1
    total_batches = max((len(work) + batch_size - 1) // batch_size, 1)
    safe_batch_index = min(max(batch_index, 1), total_batches)
    start = (safe_batch_index - 1) * batch_size
    end = min(start + batch_size, len(work))
    return work.iloc[start:end].reset_index(drop=True), total_batches


def sync_symbol(
    store: MarketDataStore,
    provider: AKShareProvider,
    run_id: int,
    symbol: str,
    name: str,
    levels: list[str],
    cfg: ScanConfig,
    minute_source: str,
) -> None:
    minute_base: pd.DataFrame | None = None
    minute_base_saved = False
    minute_source_name = minute_source
    daily_source_name = "akshare"

    for level in levels:
        started_at = pd.Timestamp.now().isoformat()
        row_count = 0
        min_date: str | None = None
        max_date: str | None = None
        status = "ok"
        error_message: str | None = None
        try:
            source_name = minute_source_name if level in MINUTE_LEVEL_MAP else daily_source_name
            start_bound, end_bound = sync_bounds(level, cfg)
            if store.has_kline_coverage(symbol, level, cfg.adjust, source_name, start_bound, end_bound):
                status = "skipped"
            elif level in {"daily", "weekly", "monthly"}:
                hist = provider.get_history(symbol, replace(cfg, period=level))
                row_count = store.save_kline(symbol, level, cfg.adjust, daily_source_name, hist)
                if not hist.empty:
                    min_date = pd.Timestamp(hist["date"].iloc[0]).isoformat(sep=" ")
                    max_date = pd.Timestamp(hist["date"].iloc[-1]).isoformat(sep=" ")
            else:
                if minute_base is None:
                    base_start, base_end = sync_bounds("15m", cfg)
                    if store.has_kline_coverage(symbol, "15m", cfg.adjust, minute_source_name, base_start, base_end):
                        minute_base = store.load_kline(symbol, "15m", cfg.adjust, minute_source_name, base_start, base_end)
                    else:
                        minute_base = fetch_raw_15m(provider, symbol, cfg, minute_source_name)
                        if not minute_base.empty:
                            store.save_kline(symbol, "15m", cfg.adjust, minute_source_name, minute_base)
                            minute_base_saved = True

                if minute_base is None or minute_base.empty:
                    status = "empty"
                elif level == "15m":
                    if status != "skipped":
                        row_count = len(minute_base) if minute_base_saved else 0
                        min_date = pd.Timestamp(minute_base["date"].iloc[0]).isoformat(sep=" ")
                        max_date = pd.Timestamp(minute_base["date"].iloc[-1]).isoformat(sep=" ")
                else:
                    bars_per_group = MINUTE_LEVEL_MAP[level][1]
                    derived = aggregate_intraday(minute_base, bars_per_group)
                    row_count = store.save_kline(
                        symbol,
                        level,
                        cfg.adjust,
                        minute_source_name,
                        derived,
                        is_derived=True,
                        derived_from=AGGREGATED_BASE_LEVEL.get(level, "15m"),
                    )
                    if not derived.empty:
                        min_date = pd.Timestamp(derived["date"].iloc[0]).isoformat(sep=" ")
                        max_date = pd.Timestamp(derived["date"].iloc[-1]).isoformat(sep=" ")
        except Exception as exc:
            status = "error"
            error_message = str(exc)

        store.record_sync_item(
            run_id=run_id,
            symbol=symbol,
            level=level,
            status=status,
            rows_written=row_count,
            min_date=min_date,
            max_date=max_date,
            started_at=started_at,
            finished_at=pd.Timestamp.now().isoformat(),
            error=error_message,
        )
        if status == "error":
            print(f"{symbol} {level} ERROR {error_message}")
        else:
            print(f"{symbol} {level} {status} rows={row_count}")


def main() -> None:
    args = parse_args()
    levels = [item.strip() for item in args.levels.split(",") if item.strip()]
    unknown = [level for level in levels if level not in LEVEL_CHOICES]
    if unknown:
        raise SystemExit(f"unsupported levels: {unknown}")

    cfg = build_config(args)
    provider = AKShareProvider()
    store = MarketDataStore(args.db_path)
    run_id = 0
    try:
        if not args.skip_bootstrap:
            imported_rows = store.import_backtest_price_history(args.bootstrap_backtest_db)
            if imported_rows:
                print(f"bootstrapped {imported_rows} rows from {Path(args.bootstrap_backtest_db).expanduser().resolve()}")

        universe = resolve_universe(args, cfg, provider)
        if universe.empty:
            raise SystemExit("universe is empty")
        if not args.symbols and args.limit is not None:
            universe = universe.head(args.limit).copy()

        store.upsert_symbols(universe, source="akshare_spot_em" if not args.symbols else "manual")
        snapshot_id = store.create_universe_snapshot(
            universe,
            source="akshare_spot_em" if not args.symbols else "manual",
            filters={
                "start_date": args.start_date,
                "end_date": args.end_date,
                "levels": levels,
                "adjust": args.adjust,
                "minute_source": args.minute_source,
                "limit": args.limit,
                "min_amount": args.min_amount,
                "min_price": args.min_price,
                "include_st": args.include_st,
                "include_delisting": args.include_delisting,
            },
        )

        batch_universe = universe
        total_batches = 1
        if not args.symbols:
            batch_universe, total_batches = slice_batch(universe, args.limit, args.batch_size, args.batch_index)
            if batch_universe.empty:
                raise SystemExit("selected batch is empty")

        run_id = store.start_sync_run(
            start_date=args.start_date,
            end_date=args.end_date,
            levels=levels,
            symbols=[str(item) for item in batch_universe["symbol"].tolist()],
            source=args.minute_source,
            mode="market_data_sync",
            notes=f"snapshot_id={snapshot_id},batch={args.batch_index}/{total_batches},batch_size={args.batch_size}",
        )
        print(f"market_data_sync v{PROJECT_VERSION}")
        print(f"database -> {Path(args.db_path).expanduser().resolve()}")
        print(f"snapshot_id -> {snapshot_id}")
        print(f"run_id -> {run_id}")
        if not args.symbols:
            print(f"batch -> {args.batch_index}/{total_batches} size={len(batch_universe)} total_universe={len(universe)}")

        total = len(batch_universe)
        for idx, (_, row) in enumerate(batch_universe.iterrows(), start=1):
            symbol = str(row["symbol"])
            name = str(row.get("name", ""))
            print(f"[{idx}/{total}] syncing {symbol} {name}".strip())
            sync_symbol(store, provider, run_id, symbol, name, levels, cfg, args.minute_source)

        store.finish_sync_run(run_id, "success", notes=f"snapshot_id={snapshot_id},batch={args.batch_index}/{total_batches},batch_size={args.batch_size}")
        coverage = pd.read_sql_query(
            """
            SELECT level, COUNT(*) AS symbols, SUM(rows_count) AS rows_count
            FROM data_coverage
            GROUP BY level
            ORDER BY CASE level
                WHEN '15m' THEN 1
                WHEN '30m' THEN 2
                WHEN '60m' THEN 3
                WHEN '120m' THEN 4
                WHEN '240m' THEN 5
                WHEN 'daily' THEN 6
                WHEN 'weekly' THEN 7
                WHEN 'monthly' THEN 8
                ELSE 99
            END
            """,
            store.conn,
        )
        if not coverage.empty:
            with pd.option_context("display.max_columns", 20, "display.width", 180):
                print(coverage.to_string(index=False))
    except Exception as exc:
        if run_id:
            store.finish_sync_run(run_id, "error", notes=str(exc))
        raise
    finally:
        store.close()


if __name__ == "__main__":
    main()
