#!/usr/bin/env python3
from __future__ import annotations

import argparse
import atexit
import signal
import time
from dataclasses import replace
from pathlib import Path
from typing import Any

import pandas as pd

from backtest_store import BacktestStore
from macd_time_signal_scanner import (
    AKShareProvider,
    MACDTimeSignalEngine,
    PROJECT_VERSION,
    ScanConfig,
    normalize_akshare_history,
)

LEVEL_CHOICES = ["15m", "30m", "60m", "120m", "240m", "daily", "weekly", "monthly"]
LONG_SIGNALS = {"LEFT_BOTTOM", "BUY1", "BUY2", "BUY3"}
SHORT_SIGNALS = {"LEFT_TOP", "SELL1", "SELL2", "SELL3"}
MINUTE_LEVEL_MAP = {"15m": ("15", 1), "30m": ("15", 2), "60m": ("15", 4), "120m": ("15", 8), "240m": ("15", 16)}
AGGREGATED_BASE_LEVEL = {"30m": "15m", "60m": "15m", "120m": "15m", "240m": "15m"}
_BAO_SESSION_READY = False
_BAO_TIMEOUT_SECONDS = 60


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare MACD signal performance across timeframes")
    parser.add_argument("--start-date", default="20240101")
    parser.add_argument("--end-date", default="20300101")
    parser.add_argument("--adjust", default="qfq", choices=["", "qfq", "hfq"])
    parser.add_argument("--levels", default="daily,weekly,60m,120m,240m")
    parser.add_argument("--symbol", default="")
    parser.add_argument("--name", default="")
    parser.add_argument("--symbols", default="")
    parser.add_argument("--limit", type=int, default=30)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--min-amount", type=float, default=1.0e8)
    parser.add_argument("--min-price", type=float, default=2.0)
    parser.add_argument("--include-st", action="store_true")
    parser.add_argument("--include-delisting", action="store_true")
    parser.add_argument("--exit-mode", default="reverse", choices=["reverse", "fixed"])
    parser.add_argument("--minute-source", default="baostock", choices=["baostock", "eastmoney"])
    parser.add_argument("--cache-dir", default=str(Path("outputs") / "cache" / "timeframes"))
    parser.add_argument("--db-path", default=str(Path("outputs") / "backtest_store.sqlite"))
    parser.add_argument("--horizons", default="1,3,5")
    parser.add_argument("--output", default=str(Path("outputs") / "timeframe_backtest.csv"))
    parser.add_argument("--summary-output", default=str(Path("outputs") / "timeframe_backtest_summary.csv"))
    return parser.parse_args()


def build_base_config(args: argparse.Namespace) -> ScanConfig:
    return ScanConfig(
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


def minute_to_daily_window(start_date: str, end_date: str) -> tuple[str, str]:
    start = pd.Timestamp(start_date).strftime("%Y-%m-%d 09:30:00")
    end = pd.Timestamp(end_date).strftime("%Y-%m-%d 15:00:00")
    return start, end


def normalize_minute_history(df: pd.DataFrame) -> pd.DataFrame:
    mapping = {
        "时间": "date",
        "开盘": "open",
        "收盘": "close",
        "最高": "high",
        "最低": "low",
        "成交量": "volume",
        "成交额": "amount",
        "换手率": "turnover",
    }
    out = df.rename(columns=mapping).copy()
    for col in ["open", "high", "low", "close", "volume", "amount", "turnover"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    out["date"] = pd.to_datetime(out["date"])
    out = out.dropna(subset=["date", "open", "high", "low", "close"]).sort_values("date").reset_index(drop=True)
    return out


def normalize_baostock_minute_history(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"] + " " + out["time"].str.slice(8, 14), format="%Y-%m-%d %H%M%S")
    for col in ["open", "high", "low", "close", "volume", "amount"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    out["turnover"] = 0.0
    out = out.dropna(subset=["date", "open", "high", "low", "close"]).sort_values("date").reset_index(drop=True)
    return out[["date", "open", "high", "low", "close", "volume", "amount", "turnover"]]


def to_baostock_symbol(symbol: str) -> str:
    if symbol.startswith(("6", "9", "5")):
        return f"sh.{symbol}"
    return f"sz.{symbol}"


def ensure_baostock_session() -> Any:
    global _BAO_SESSION_READY
    import baostock as bs

    if not _BAO_SESSION_READY:
        login_result = bs.login()
        if login_result.error_code != "0":
            raise RuntimeError(f"baostock login failed: {login_result.error_code} {login_result.error_msg}")
        _BAO_SESSION_READY = True

        def _logout() -> None:
            try:
                bs.logout()
            except Exception:
                pass

        atexit.register(_logout)
    return bs


class BaoStockTimeoutError(TimeoutError):
    pass


class _BaoTimeout:
    def __init__(self, seconds: int) -> None:
        self.seconds = max(int(seconds), 1)
        self._prev_handler: Any = None
        self._enabled = hasattr(signal, "SIGALRM")

    def _handle(self, signum: int, frame: Any) -> None:
        raise BaoStockTimeoutError(f"baostock request timed out after {self.seconds}s")

    def __enter__(self) -> "_BaoTimeout":
        if self._enabled:
            self._prev_handler = signal.signal(signal.SIGALRM, self._handle)
            signal.alarm(self.seconds)
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if self._enabled:
            signal.alarm(0)
            signal.signal(signal.SIGALRM, self._prev_handler)


def query_baostock_history(symbol: str, start_date: str, end_date: str, frequency: str, adjust: str) -> pd.DataFrame:
    bs = ensure_baostock_session()
    errors: list[str] = []
    for attempt in range(3):
        try:
            with _BaoTimeout(_BAO_TIMEOUT_SECONDS):
                rs = bs.query_history_k_data_plus(
                    to_baostock_symbol(symbol),
                    "date,time,open,high,low,close,volume,amount",
                    start_date=start_date,
                    end_date=end_date,
                    frequency=frequency,
                    adjustflag={"": "3", "qfq": "2", "hfq": "1"}[adjust],
                )
                if rs.error_code == "0":
                    rows: list[list[str]] = []
                    while rs.next():
                        rows.append(rs.get_row_data())
                    if not rows:
                        return pd.DataFrame()
                    return normalize_baostock_minute_history(pd.DataFrame(rows, columns=rs.fields))
        except BaoStockTimeoutError as exc:
            errors.append(f"attempt {attempt + 1}: {exc}")
        else:
            if rs.error_code == "10001001":
                errors.append(f"attempt {attempt + 1}: baostock session expired")
            else:
                raise RuntimeError(f"baostock query failed: {rs.error_code} {rs.error_msg}")

        global _BAO_SESSION_READY
        _BAO_SESSION_READY = False
        try:
            bs.logout()
        except Exception:
            pass
        bs = ensure_baostock_session()
    raise RuntimeError(f"baostock query failed after retries: {'; '.join(errors)}")


def cache_path(cache_dir: str, symbol: str, level: str, cfg: ScanConfig, minute_source: str) -> Path:
    safe = f"{symbol}_{level}_{cfg.start_date}_{cfg.end_date}_{cfg.adjust or 'none'}_{minute_source}.csv"
    return Path(cache_dir).expanduser().resolve() / safe


def coverage_bounds(level: str, cfg: ScanConfig) -> tuple[str, str]:
    start = pd.Timestamp(cfg.start_date).normalize()
    requested_end = pd.Timestamp(cfg.end_date)
    now = pd.Timestamp.now()
    if level in MINUTE_LEVEL_MAP:
        effective_end = min(requested_end + pd.Timedelta(hours=23, minutes=59, seconds=59), now.floor("min"))
    else:
        effective_end = min(requested_end.normalize(), now.normalize())
    return start.strftime("%Y-%m-%d %H:%M:%S"), effective_end.strftime("%Y-%m-%d %H:%M:%S")


def load_cached_history(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path)
    df["date"] = pd.to_datetime(df["date"])
    return df


def save_cached_history(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")


def aggregate_intraday(df: pd.DataFrame, bars_per_group: int) -> pd.DataFrame:
    if df.empty:
        return df
    work = df.copy()
    work["trade_date"] = work["date"].dt.strftime("%Y-%m-%d")
    work["seq"] = work.groupby("trade_date").cumcount()
    work["bucket"] = work["seq"] // bars_per_group
    grouped = (
        work.groupby(["trade_date", "bucket"], as_index=False)
        .agg(
            date=("date", "last"),
            open=("open", "first"),
            high=("high", "max"),
            low=("low", "min"),
            close=("close", "last"),
            volume=("volume", "sum"),
            amount=("amount", "sum"),
            turnover=("turnover", "sum"),
        )
        .sort_values("date")
        .reset_index(drop=True)
    )
    return grouped[["date", "open", "high", "low", "close", "volume", "amount", "turnover"]]


def elapsed_days(start_dt: pd.Timestamp, end_dt: pd.Timestamp) -> float:
    return max((pd.Timestamp(end_dt) - pd.Timestamp(start_dt)).total_seconds() / 86400.0, 0.0)


def fetch_history(
    provider: AKShareProvider,
    store: BacktestStore,
    symbol: str,
    level: str,
    cfg: ScanConfig,
    minute_source: str,
    cache_dir: str,
) -> pd.DataFrame:
    start_bound, end_bound = coverage_bounds(level, cfg)
    if store.has_price_coverage(symbol, level, cfg.adjust, minute_source, start_bound, end_bound):
        db_df = store.load_price_history(symbol, level, cfg.adjust, minute_source, start_bound, end_bound)
        if not db_df.empty:
            return db_df

    cached = load_cached_history(cache_path(cache_dir, symbol, level, cfg, minute_source))
    if not cached.empty:
        store.save_price_history(symbol, level, cfg.adjust, minute_source, cached)
        return cached

    if level in {"daily", "weekly", "monthly"}:
        level_cfg = replace(cfg, period=level)
        out = provider.get_history(symbol, level_cfg)
        if not out.empty:
            save_cached_history(cache_path(cache_dir, symbol, level, cfg, minute_source), out)
            store.save_price_history(symbol, level, cfg.adjust, minute_source, out)
        return out

    if level not in MINUTE_LEVEL_MAP:
        raise ValueError(f"unsupported level: {level}")
    period, bars_per_group = MINUTE_LEVEL_MAP[level]
    base_level = AGGREGATED_BASE_LEVEL.get(level)

    if base_level is not None:
        base_start_bound, base_end_bound = coverage_bounds(base_level, cfg)
        if store.has_price_coverage(symbol, base_level, cfg.adjust, minute_source, base_start_bound, base_end_bound):
            base_df = store.load_price_history(symbol, base_level, cfg.adjust, minute_source, base_start_bound, base_end_bound)
            if not base_df.empty:
                out = aggregate_intraday(base_df, bars_per_group)
                if not out.empty:
                    save_cached_history(cache_path(cache_dir, symbol, level, cfg, minute_source), out)
                    store.save_price_history(symbol, level, cfg.adjust, minute_source, out)
                    return out

        base_cached = load_cached_history(cache_path(cache_dir, symbol, base_level, cfg, minute_source))
        if not base_cached.empty:
            store.save_price_history(symbol, base_level, cfg.adjust, minute_source, base_cached)
            out = aggregate_intraday(base_cached, bars_per_group)
            if not out.empty:
                save_cached_history(cache_path(cache_dir, symbol, level, cfg, minute_source), out)
                store.save_price_history(symbol, level, cfg.adjust, minute_source, out)
                return out

    if minute_source == "baostock":
        base = query_baostock_history(
            symbol=symbol,
            start_date=pd.Timestamp(cfg.start_date).strftime("%Y-%m-%d"),
            end_date=pd.Timestamp(cfg.end_date).strftime("%Y-%m-%d"),
            frequency=period,
            adjust=cfg.adjust,
        )
    else:
        minute_start, minute_end = minute_to_daily_window(cfg.start_date, cfg.end_date)
        raw = provider.ak.stock_zh_a_hist_min_em(
            symbol=symbol,
            start_date=minute_start,
            end_date=minute_end,
            period=period,
            adjust=cfg.adjust,
        )
        if raw is None or raw.empty:
            return pd.DataFrame()
        base = normalize_minute_history(raw)

    if base_level is not None and not base.empty:
        save_cached_history(cache_path(cache_dir, symbol, base_level, cfg, minute_source), base)
        store.save_price_history(symbol, base_level, cfg.adjust, minute_source, base)
    out = base if bars_per_group == 1 else aggregate_intraday(base, bars_per_group)
    if not out.empty:
        save_cached_history(cache_path(cache_dir, symbol, level, cfg, minute_source), out)
        store.save_price_history(symbol, level, cfg.adjust, minute_source, out)
    return out


def calculate_fixed_horizon_metrics(df: pd.DataFrame, signals: pd.DataFrame, horizons: list[int], level: str) -> pd.DataFrame:
    if df.empty or signals.empty:
        return pd.DataFrame()

    price_df = df[["date", "open", "close"]].copy().reset_index(drop=True)
    date_to_index = {pd.Timestamp(row.date): idx for idx, row in price_df.iterrows()}
    rows: list[dict[str, Any]] = []

    for _, signal in signals.iterrows():
        signal_dt = pd.Timestamp(signal["date"])
        signal_idx = date_to_index.get(signal_dt)
        if signal_idx is None:
            continue
        entry_idx = signal_idx + 1
        if entry_idx >= len(price_df):
            continue
        entry_price = float(price_df.iloc[entry_idx]["open"])
        signal_type = str(signal["signal"])
        if signal_type not in LONG_SIGNALS:
            continue

        row: dict[str, Any] = {
            "level": level,
            "date": signal_dt,
            "symbol": signal["symbol"],
            "name": signal["name"],
            "signal": signal_type,
            "score": float(signal["score"]),
            "entry_date": pd.Timestamp(price_df.iloc[entry_idx]["date"]),
            "entry_price": entry_price,
        }
        valid = False
        for horizon in horizons:
            exit_idx = entry_idx + horizon - 1
            if exit_idx >= len(price_df):
                continue
            valid = True
            exit_price = float(price_df.iloc[exit_idx]["close"])
            trade_return = (exit_price - entry_price) / entry_price
            row[f"ret_{horizon}"] = round(trade_return, 6)
            row[f"win_{horizon}"] = int(trade_return > 0)
            row[f"exit_date_{horizon}"] = pd.Timestamp(price_df.iloc[exit_idx]["date"])
        if valid:
            rows.append(row)
    return pd.DataFrame(rows)


def calculate_reverse_signal_metrics(df: pd.DataFrame, signals: pd.DataFrame, level: str) -> pd.DataFrame:
    if df.empty or signals.empty:
        return pd.DataFrame()

    work_signals = signals.copy().sort_values("date").reset_index(drop=True)
    price_df = df[["date", "open"]].copy().reset_index(drop=True)
    date_to_index = {pd.Timestamp(row.date): idx for idx, row in price_df.iterrows()}
    rows: list[dict[str, Any]] = []

    in_position = False
    pending_entry: dict[str, Any] | None = None
    last_exit_dt: pd.Timestamp | None = None

    for _, signal in work_signals.iterrows():
        signal_type = str(signal["signal"])
        signal_dt = pd.Timestamp(signal["date"])
        signal_idx = date_to_index.get(signal_dt)
        if signal_idx is None:
            continue

        if not in_position and signal_type in LONG_SIGNALS:
            entry_idx = signal_idx + 1
            if entry_idx >= len(price_df):
                continue
            pending_entry = {
                "signal": signal,
                "entry_idx": entry_idx,
            }
            in_position = True
            continue

        if in_position and signal_type in SHORT_SIGNALS and pending_entry is not None:
            exit_idx = signal_idx + 1
            if exit_idx >= len(price_df) or exit_idx <= pending_entry["entry_idx"]:
                continue
            entry_signal = pending_entry["signal"]
            entry_dt = pd.Timestamp(price_df.iloc[pending_entry["entry_idx"]]["date"])
            exit_dt = pd.Timestamp(price_df.iloc[exit_idx]["date"])
            entry_price = float(price_df.iloc[pending_entry["entry_idx"]]["open"])
            exit_price = float(price_df.iloc[exit_idx]["open"])
            trade_return = (exit_price - entry_price) / entry_price
            flat_days = elapsed_days(last_exit_dt, entry_dt) if last_exit_dt is not None else None
            rows.append(
                {
                    "level": level,
                    "date": pd.Timestamp(entry_signal["date"]),
                    "symbol": entry_signal["symbol"],
                    "name": entry_signal["name"],
                    "signal": str(entry_signal["signal"]),
                    "score": float(entry_signal["score"]),
                    "entry_date": entry_dt,
                    "entry_price": entry_price,
                    "exit_signal": signal_type,
                    "exit_date": exit_dt,
                    "exit_price": exit_price,
                    "hold_bars": int(exit_idx - pending_entry["entry_idx"] + 1),
                    "hold_days": round(elapsed_days(entry_dt, exit_dt), 6),
                    "flat_days_before_entry": round(flat_days, 6) if flat_days is not None else None,
                    "ret_reverse": round(trade_return, 6),
                    "win_reverse": int(trade_return > 0),
                }
            )
            last_exit_dt = exit_dt
            in_position = False
            pending_entry = None
    return pd.DataFrame(rows)


def max_drawdown_from_trades(level_df: pd.DataFrame) -> float | None:
    if level_df.empty or "ret_reverse" not in level_df:
        return None
    equity = 1.0
    peak = 1.0
    max_drawdown = 0.0
    for trade_return in level_df.sort_values(["exit_date", "entry_date"])["ret_reverse"].tolist():
        equity *= 1.0 + float(trade_return)
        peak = max(peak, equity)
        if peak > 0:
            max_drawdown = max(max_drawdown, (peak - equity) / peak)
    return max_drawdown


def summarize_results(
    trades: pd.DataFrame,
    horizons: list[int],
    exit_mode: str,
    sample_years: float,
    universe_size: int,
) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame()
    summary_rows: list[dict[str, Any]] = []
    for level in sorted(trades["level"].unique(), key=lambda x: LEVEL_CHOICES.index(x) if x in LEVEL_CHOICES else 999):
        level_df = trades[trades["level"] == level].copy()
        row: dict[str, Any] = {"level": level, "trades": int(len(level_df))}
        if exit_mode == "reverse":
            row["avg_ret_reverse"] = round(float(level_df["ret_reverse"].mean()), 6) if not level_df.empty else None
            row["win_rate_reverse"] = round(float(level_df["win_reverse"].mean()), 4) if not level_df.empty else None
            row["avg_hold_bars"] = round(float(level_df["hold_bars"].mean()), 2) if not level_df.empty else None
            row["avg_hold_days"] = round(float(level_df["hold_days"].mean()), 2) if "hold_days" in level_df and not level_df.empty else None
            valid_flat = level_df.dropna(subset=["flat_days_before_entry"]) if "flat_days_before_entry" in level_df else pd.DataFrame()
            row["avg_flat_days"] = round(float(valid_flat["flat_days_before_entry"].mean()), 2) if not valid_flat.empty else None
            row["max_drawdown"] = round(float(max_drawdown_from_trades(level_df) or 0.0), 6) if not level_df.empty else None
            row["sample_years"] = round(sample_years, 2)
            row["annual_signal_freq"] = round(float(len(level_df)) / sample_years, 2) if sample_years > 0 else None
            row["annual_signal_freq_per_symbol"] = round(float(len(level_df)) / sample_years / max(universe_size, 1), 4) if sample_years > 0 else None
        else:
            for horizon in horizons:
                ret_col = f"ret_{horizon}"
                win_col = f"win_{horizon}"
                valid_df = level_df.dropna(subset=[ret_col])
                row[f"avg_ret_{horizon}"] = round(float(valid_df[ret_col].mean()), 6) if not valid_df.empty else None
                row[f"win_rate_{horizon}"] = round(float(valid_df[win_col].mean()), 4) if not valid_df.empty else None
                row[f"count_{horizon}"] = int(len(valid_df))
        summary_rows.append(row)
    return pd.DataFrame(summary_rows)


def backtest_symbol(
    provider: AKShareProvider,
    store: BacktestStore,
    symbol: str,
    name: str,
    levels: list[str],
    cfg: ScanConfig,
    horizons: list[int],
    exit_mode: str,
    minute_source: str,
    cache_dir: str,
) -> pd.DataFrame:
    engine = MACDTimeSignalEngine(cfg)
    all_trades: list[pd.DataFrame] = []
    for level in levels:
        hist = fetch_history(provider, store, symbol, level, cfg, minute_source, cache_dir)
        if hist.empty:
            continue
        signals = engine.scan_dataframe(hist, symbol=symbol, name=name)
        if signals.empty:
            continue
        if exit_mode == "reverse":
            trades = calculate_reverse_signal_metrics(hist, signals, level)
        else:
            trades = calculate_fixed_horizon_metrics(hist, signals, horizons, level)
        if not trades.empty:
            all_trades.append(trades)
    if not all_trades:
        return pd.DataFrame()
    return pd.concat(all_trades, ignore_index=True)


def main() -> None:
    args = parse_args()
    levels = [level.strip() for level in args.levels.split(",") if level.strip()]
    unknown = [level for level in levels if level not in LEVEL_CHOICES]
    if unknown:
        raise SystemExit(f"unsupported levels: {unknown}")
    horizons = [int(item.strip()) for item in args.horizons.split(",") if item.strip()]

    cfg = build_base_config(args)
    provider = AKShareProvider()
    store = BacktestStore(args.db_path)

    if args.symbols:
        items = [item.strip() for item in args.symbols.split(",") if item.strip()]
        universe = pd.DataFrame([{"symbol": item, "name": ""} for item in items])
    elif args.symbol:
        universe = pd.DataFrame([{"symbol": args.symbol, "name": args.name}])
    else:
        universe = provider.get_universe(cfg)
    if universe.empty:
        raise SystemExit("universe is empty")
    effective_end = min(pd.Timestamp(args.end_date).normalize(), pd.Timestamp.now().normalize())
    sample_years = max(elapsed_days(pd.Timestamp(args.start_date).normalize(), effective_end) / 365.25, 1e-9)

    trades_list: list[pd.DataFrame] = []
    total_symbols = len(universe)
    for idx, (_, row) in enumerate(universe.iterrows(), start=1):
        symbol = str(row["symbol"])
        started_at = time.time()
        print(f"[{idx}/{total_symbols}] backtesting {symbol}")
        trades = backtest_symbol(
            provider=provider,
            store=store,
            symbol=symbol,
            name=str(row.get("name", "")),
            levels=levels,
            cfg=cfg,
            horizons=horizons,
            exit_mode=args.exit_mode,
            minute_source=args.minute_source,
            cache_dir=args.cache_dir,
        )
        if not trades.empty:
            trades_list.append(trades)
        print(f"[{idx}/{total_symbols}] done {symbol} rows={len(trades)} elapsed={time.time() - started_at:.2f}s")

    all_trades = pd.concat(trades_list, ignore_index=True) if trades_list else pd.DataFrame()
    summary = summarize_results(all_trades, horizons, args.exit_mode, sample_years, total_symbols)

    output_path = Path(args.output).expanduser().resolve()
    summary_path = Path(args.summary_output).expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    all_trades.to_csv(output_path, index=False, encoding="utf-8-sig")
    summary.to_csv(summary_path, index=False, encoding="utf-8-sig")
    store.save_backtest_run(
        created_at=pd.Timestamp.now().isoformat(),
        start_date=args.start_date,
        end_date=args.end_date,
        levels=levels,
        symbols=[str(item) for item in universe["symbol"].tolist()],
        exit_mode=args.exit_mode,
        minute_source=args.minute_source,
        output_path=str(output_path),
        summary_path=str(summary_path),
        trades_df=all_trades,
        summary_df=summary,
    )
    store.close()

    print(f"macd_timeframe_backtest v{PROJECT_VERSION}")
    print(f"saved trades -> {output_path}")
    print(f"saved summary -> {summary_path}")
    print(f"saved database -> {Path(args.db_path).expanduser().resolve()}")
    if not summary.empty:
        with pd.option_context("display.max_columns", 50, "display.width", 200):
            print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
