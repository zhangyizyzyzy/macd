#!/usr/bin/env python3
from __future__ import annotations

import argparse
import concurrent.futures as futures
from dataclasses import asdict
from pathlib import Path
import sys

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from backtest_store import BacktestStore
from macd_time_signal_scanner import AKShareProvider, PROJECT_VERSION, ScanConfig
from macd_timeframe_backtest import backtest_symbol, elapsed_days, summarize_results


class NullStore:
    def has_price_coverage(self, *args, **kwargs) -> bool:
        return False

    def load_price_history(self, *args, **kwargs) -> pd.DataFrame:
        return pd.DataFrame()

    def save_price_history(self, *args, **kwargs) -> None:
        return None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Parallel MACD timeframe backtest")
    parser.add_argument("--start-date", default="20240101")
    parser.add_argument("--end-date", default="20300101")
    parser.add_argument("--adjust", default="qfq", choices=["", "qfq", "hfq"])
    parser.add_argument("--levels", default="15m,60m,120m,240m")
    parser.add_argument("--symbols", default="")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--min-amount", type=float, default=1.0e8)
    parser.add_argument("--min-price", type=float, default=2.0)
    parser.add_argument("--include-st", action="store_true")
    parser.add_argument("--include-delisting", action="store_true")
    parser.add_argument("--minute-source", default="baostock", choices=["baostock", "eastmoney"])
    parser.add_argument("--cache-dir", default=str(Path("outputs") / "cache" / "timeframes"))
    parser.add_argument("--db-path", default=str(Path("outputs") / "backtest_store.sqlite"))
    parser.add_argument("--output", default=str(Path("outputs") / "timeframe_backtest_reverse_limit100_focus_bao.csv"))
    parser.add_argument("--summary-output", default=str(Path("outputs") / "timeframe_backtest_reverse_limit100_focus_bao_summary.csv"))
    parser.add_argument("--universe-output", default=str(Path("outputs") / "universe_limit100_snapshot.csv"))
    return parser.parse_args()


def worker(symbol: str, name: str, cfg_kwargs: dict, levels: list[str], cache_dir: str, minute_source: str) -> list[dict]:
    provider = AKShareProvider()
    store = NullStore()
    cfg = ScanConfig(**cfg_kwargs)
    trades = backtest_symbol(
        provider=provider,
        store=store,
        symbol=symbol,
        name=name,
        levels=levels,
        cfg=cfg,
        horizons=[1, 3, 5],
        exit_mode="reverse",
        minute_source=minute_source,
        cache_dir=cache_dir,
    )
    if trades.empty:
        return []
    return trades.to_dict("records")


def main() -> None:
    args = parse_args()
    levels = [item.strip() for item in args.levels.split(",") if item.strip()]
    cfg = ScanConfig(
        start_date=args.start_date,
        end_date=args.end_date,
        adjust=args.adjust,
        period="daily",
        workers=args.workers,
        min_amount=args.min_amount,
        min_price=args.min_price,
        limit=args.limit,
        recent_bars=9999,
        latest_only=False,
        exclude_st=not args.include_st,
        exclude_delisting=not args.include_delisting,
    )

    if args.symbols:
        items = [item.strip() for item in args.symbols.split(",") if item.strip()]
        universe = pd.DataFrame([{"symbol": item, "name": ""} for item in items])
    else:
        provider = AKShareProvider()
        universe = provider.get_universe(cfg)
        if universe.empty:
            raise SystemExit("universe is empty")
        universe = universe.head(args.limit).copy()
    universe_output = Path(args.universe_output).expanduser().resolve()
    universe_output.parent.mkdir(parents=True, exist_ok=True)
    universe[["symbol", "name"]].to_csv(universe_output, index=False, encoding="utf-8-sig")

    cfg_kwargs = asdict(cfg)
    rows = universe.to_dict("records")
    trade_frames: list[pd.DataFrame] = []
    with futures.ProcessPoolExecutor(max_workers=max(args.workers, 1)) as ex:
        task_map = {
            ex.submit(worker, str(row["symbol"]), str(row.get("name", "")), cfg_kwargs, levels, args.cache_dir, args.minute_source): str(row["symbol"])
            for row in rows
        }
        done = 0
        total = len(task_map)
        for fut in futures.as_completed(task_map):
            symbol = task_map[fut]
            done += 1
            try:
                records = fut.result()
                trade_count = len(records)
                print(f"[{done}/{total}] {symbol} trades={trade_count}")
                if records:
                    trade_frames.append(pd.DataFrame(records))
            except Exception as exc:
                print(f"[{done}/{total}] {symbol} ERROR {exc}")

    all_trades = pd.concat(trade_frames, ignore_index=True) if trade_frames else pd.DataFrame()
    effective_end = min(pd.Timestamp(args.end_date).normalize(), pd.Timestamp.now().normalize())
    sample_years = max(elapsed_days(pd.Timestamp(args.start_date).normalize(), effective_end) / 365.25, 1e-9)
    summary = summarize_results(all_trades, [1, 3, 5], "reverse", sample_years, len(universe))

    output_path = Path(args.output).expanduser().resolve()
    summary_path = Path(args.summary_output).expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    all_trades.to_csv(output_path, index=False, encoding="utf-8-sig")
    summary.to_csv(summary_path, index=False, encoding="utf-8-sig")

    store = BacktestStore(args.db_path)
    run_id = store.save_backtest_run(
        created_at=pd.Timestamp.now().isoformat(),
        start_date=args.start_date,
        end_date=args.end_date,
        levels=levels,
        symbols=[str(item) for item in universe["symbol"].tolist()],
        exit_mode="reverse",
        minute_source=args.minute_source,
        output_path=str(output_path),
        summary_path=str(summary_path),
        trades_df=all_trades,
        summary_df=summary,
    )
    store.close()

    print(f"parallel_backtest v{PROJECT_VERSION}")
    print(f"saved universe -> {universe_output}")
    print(f"saved trades -> {output_path}")
    print(f"saved summary -> {summary_path}")
    print(f"saved run_id -> {run_id}")
    if not summary.empty:
        with pd.option_context("display.max_columns", 50, "display.width", 220):
            print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
