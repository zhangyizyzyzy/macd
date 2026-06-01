#!/usr/bin/env python3
"""
box1_vectorbt_backtest.py  ——  一买封箱 HS300 回测 (vectorbt 版)

和 box1_backtest.py 的区别:
  • 信号检测层完全共用 (import 自 box1_backtest)
  • 回测执行层改用 vectorbt.Portfolio.from_signals
  • 框架与 macd第二版 ETH 版本保持一致 (同一个 vectorbt 库)
  • A 股简化版: 无杠杆/无做空/无清算, 用 from_signals 即可

用法:
  python3 box1_vectorbt_backtest.py                              # HS300 × 3 周期
  python3 box1_vectorbt_backtest.py --symbol 600519              # 单股测试
  python3 box1_vectorbt_backtest.py --levels 60m                 # 指定周期
  python3 box1_vectorbt_backtest.py --limit 20                   # 前 20 只
  python3 box1_vectorbt_backtest.py --plot 600519                # 生成单股权益曲线 html
"""
from __future__ import annotations

import argparse
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

try:
    import vectorbt as vbt
except ImportError:
    print("ERROR: vectorbt not installed. Run: pip3 install vectorbt")
    sys.exit(1)

# Reuse signal detection from box1_backtest
sys.path.insert(0, str(Path(__file__).parent))
from box1_backtest import (  # noqa: E402
    Box1Config,
    Signal,
    compute_macd_atr,
    detect_signals,
    load_dataframes,
    HS300_PATH,
)

OUTPUT_DIR = Path("/Users/zhangyi/Documents/code/macd股票版/outputs/box1_vectorbt")


# ==============================
# Signal → vectorbt arrays
# ==============================


def signals_to_vbt_arrays(
    df: pd.DataFrame,
    signals: list[Signal],
    cfg: Box1Config,
) -> tuple[pd.Series, pd.Series, pd.Series, pd.Series, pd.Series]:
    """Convert discrete Signals to vectorbt entries/exits/sl_stop/tp_stop arrays.

    Returns:
        entries       (bool series)  — True when a BUY signal fires
        exits         (bool series)  — True when a reverse SELL signal fires
        short_entries (bool series)  — True when a SELL signal fires (if long_only=False)
        sl_stop       (float series) — per-bar SL distance in % (np.nan when no new entry)
        tp_stop       (float series) — per-bar TP distance in % (np.nan when no new entry)
    """
    data = compute_macd_atr(df, cfg).dropna(
        subset=["open", "high", "low", "close", "macd", "signal", "hist", "atr"]
    ).reset_index(drop=True)
    index = pd.DatetimeIndex(data["date"])
    n = len(data)

    entries = np.zeros(n, dtype=bool)
    exits = np.zeros(n, dtype=bool)
    short_entries = np.zeros(n, dtype=bool)
    short_exits = np.zeros(n, dtype=bool)
    sl_stop = np.full(n, np.nan, dtype=float)
    tp_stop = np.full(n, np.nan, dtype=float)

    close = data["close"].to_numpy(float)
    atr = data["atr"].to_numpy(float)

    for sig in signals:
        i = sig.index
        if i >= n or atr[i] <= 0 or close[i] <= 0:
            continue

        # SL/TP distance as % of entry price (vectorbt convention)
        sl_pct = cfg.sl_atr_mult * atr[i] / close[i]
        tp_pct = cfg.tp_atr_mult * atr[i] / close[i]

        if cfg.only_strong_buy1 and sig.code not in ("STRONG_BUY1", "STRONG_SELL1"):
            continue

        if sig.side == "BUY":
            entries[i] = True
            if not np.isnan(sl_pct):
                sl_stop[i] = sl_pct
                tp_stop[i] = tp_pct
            if cfg.allow_reversal:
                short_exits[i] = True  # close any short on buy signal
        else:  # SELL
            if cfg.long_only:
                # Sell signal as exit only
                exits[i] = True
            else:
                short_entries[i] = True
                if not np.isnan(sl_pct):
                    sl_stop[i] = sl_pct
                    tp_stop[i] = tp_pct
                if cfg.allow_reversal:
                    exits[i] = True  # close any long on sell signal

    return (
        pd.Series(entries, index=index, name="entries"),
        pd.Series(exits, index=index, name="exits"),
        pd.Series(short_entries, index=index, name="short_entries"),
        pd.Series(sl_stop, index=index, name="sl_stop"),
        pd.Series(tp_stop, index=index, name="tp_stop"),
    ), data


# ==============================
# Single-symbol vectorbt portfolio
# ==============================


def run_vbt_portfolio(
    df: pd.DataFrame,
    signals: list[Signal],
    cfg: Box1Config,
) -> tuple[vbt.Portfolio, pd.DataFrame] | tuple[None, None]:
    """Run vectorbt Portfolio.from_signals on one symbol."""
    if not signals:
        return None, None

    arrays, data = signals_to_vbt_arrays(df, signals, cfg)
    entries, exits, short_entries, sl_stop, tp_stop = arrays

    if not entries.any() and not short_entries.any():
        return None, None

    close_series = pd.Series(data["close"].values, index=entries.index, name="close")
    high_series = pd.Series(data["high"].values, index=entries.index)
    low_series = pd.Series(data["low"].values, index=entries.index)
    open_series = pd.Series(data["open"].values, index=entries.index)

    kwargs = dict(
        close=close_series,
        open=open_series,
        high=high_series,
        low=low_series,
        entries=entries,
        exits=exits,
        sl_stop=sl_stop,
        tp_stop=tp_stop,
        init_cash=cfg.initial_capital,
        fees=cfg.fee_rate,
        slippage=0.0,
        size=np.inf,           # 使用全部可用现金
        size_type="amount",
        accumulate=False,
        freq="1D",             # Only used for display frequency; real bar width auto-detected
    )
    if not cfg.long_only:
        kwargs["short_entries"] = short_entries
        kwargs["short_exits"] = exits
        kwargs["direction"] = "both"
    else:
        kwargs["direction"] = "longonly"

    try:
        pf = vbt.Portfolio.from_signals(**kwargs)
    except Exception as exc:
        print(f"  [vbt error] {exc}")
        return None, None

    return pf, data


# ==============================
# Aggregator
# ==============================


def run_level_backtest(
    symbols: list[str],
    level: str,
    cfg: Box1Config,
    verbose: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Run backtest on all symbols for one timeframe.

    Returns:
        per_stock_df: one row per stock with performance metrics
        all_trades_df: concatenated vbt trades across all stocks (stock column added)
    """
    per_stock_rows = []
    all_trades = []

    for idx, sym in enumerate(symbols):
        dfs = load_dataframes(sym, [level])
        df = dfs.get(level)
        if df is None or len(df) < 60:
            continue

        signals = detect_signals(df, cfg)
        if not signals:
            continue

        pf, _ = run_vbt_portfolio(df, signals, cfg)
        if pf is None:
            continue

        trades = pf.trades.records_readable
        if trades.empty:
            continue

        # vectorbt trades columns: Entry Timestamp, Exit Timestamp, Size, Entry Price,
        # Exit Price, PnL, Return, Status, Direction, ...
        trades_copy = trades.copy()
        trades_copy["symbol"] = sym
        all_trades.append(trades_copy)

        total_return = float(pf.total_return())
        max_dd = float(pf.max_drawdown())
        win_rate = float(pf.trades.win_rate())
        trade_count = int(pf.trades.count())
        expectancy = float(pf.trades.expectancy())
        sharpe = float(pf.sharpe_ratio()) if trade_count >= 3 else np.nan

        per_stock_rows.append({
            "symbol": sym,
            "n_signals": len(signals),
            "trade_count": trade_count,
            "total_return_pct": round(total_return * 100, 4),
            "max_drawdown_pct": round(max_dd * 100, 4),
            "win_rate_pct": round(win_rate * 100 if not np.isnan(win_rate) else 0, 4),
            "avg_pnl_pct": round(expectancy / cfg.initial_capital * 100 if cfg.initial_capital > 0 else 0, 4),
            "sharpe": round(sharpe, 4) if not np.isnan(sharpe) else None,
        })

        if verbose and (idx + 1) % 30 == 0:
            print(f"    [{level}] {idx+1}/{len(symbols)} processed, {len(per_stock_rows)} with trades")

    per_stock_df = pd.DataFrame(per_stock_rows)
    all_trades_df = pd.concat(all_trades, ignore_index=True) if all_trades else pd.DataFrame()
    return per_stock_df, all_trades_df


# ==============================
# CLI
# ==============================


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", type=str, default=None, help="Single symbol test")
    parser.add_argument("--levels", type=str, default="15m,60m,4h,daily")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--only-strong", action="store_true")
    parser.add_argument("--no-buy2", action="store_true")
    parser.add_argument("--plot", type=str, default=None, help="Generate single-symbol plot HTML")
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    levels = [lv.strip() for lv in args.levels.split(",")]

    cfg = Box1Config(
        only_strong_buy1=args.only_strong,
        enable_buy2=not args.no_buy2,
        long_only=True,
    )

    hs300 = pd.read_csv(HS300_PATH, dtype={"symbol": str})
    symbols = [str(s).zfill(6) for s in hs300["symbol"]]
    if args.symbol:
        symbols = [args.symbol.zfill(6)]
    elif args.limit:
        symbols = symbols[: args.limit]

    print(f"[Box1-vbt] vectorbt={vbt.__version__}")
    print(f"[Box1-vbt] universe={len(symbols)} levels={levels}")
    print(f"[Box1-vbt] SL={cfg.sl_atr_mult}×ATR TP={cfg.tp_atr_mult}×ATR "
          f"only_strong={cfg.only_strong_buy1} enable_buy2={cfg.enable_buy2}")
    print()

    # ---- Single symbol verbose / plot mode ----
    if args.symbol:
        for lv in levels:
            dfs = load_dataframes(args.symbol, [lv])
            df = dfs.get(lv)
            if df is None or len(df) < 60:
                print(f"{lv}: not enough data")
                continue
            signals = detect_signals(df, cfg)
            pf, data = run_vbt_portfolio(df, signals, cfg)
            print(f"\n=== {args.symbol} {lv} ===")
            print(f"  bars:    {len(data) if data is not None else 0}")
            print(f"  signals: {len(signals)} "
                  f"({sum(1 for s in signals if s.code=='STRONG_BUY1')} STRONG_BUY1, "
                  f"{sum(1 for s in signals if s.code=='BUY1')} BUY1, "
                  f"{sum(1 for s in signals if s.code=='BUY2')} BUY2, "
                  f"{sum(1 for s in signals if s.code=='SELL1')} SELL1, "
                  f"{sum(1 for s in signals if s.code=='STRONG_SELL1')} STRONG_SELL1)")
            if pf is None:
                print("  no trades")
                continue
            trades = pf.trades.records_readable
            print(f"  trades:          {len(trades)}")
            print(f"  total_return:    {pf.total_return() * 100:+.2f}%")
            print(f"  max_drawdown:    {pf.max_drawdown() * 100:.2f}%")
            print(f"  win_rate:        {pf.trades.win_rate() * 100:.2f}%")
            print(f"  avg_pnl/trade:   {pf.trades.expectancy() / cfg.initial_capital * 100:+.4f}%")
            try:
                print(f"  sharpe:          {pf.sharpe_ratio():.4f}")
            except Exception:
                pass
            if len(trades) > 0 and len(trades) <= 30:
                print("\n  Trades:")
                cols = ["Entry Timestamp", "Exit Timestamp", "Entry Price", "Exit Price", "Return"]
                cols = [c for c in cols if c in trades.columns]
                print(trades[cols].to_string(index=False))

        if args.plot:
            # Use the LAST analyzed level to plot
            dfs = load_dataframes(args.plot, [levels[-1]])
            df = dfs.get(levels[-1])
            if df is not None:
                signals = detect_signals(df, cfg)
                pf, _ = run_vbt_portfolio(df, signals, cfg)
                if pf is not None:
                    out_html = OUTPUT_DIR / f"plot_{args.plot}_{levels[-1]}.html"
                    fig = pf.plot()
                    fig.write_html(str(out_html))
                    print(f"\nPlot saved: {out_html}")
        return

    # ---- Multi-stock aggregate mode ----
    all_levels_summary = []
    for lv in levels:
        print(f"\n=== Level: {lv} ===")
        per_stock_df, trades_df = run_level_backtest(symbols, lv, cfg, verbose=True)
        if per_stock_df.empty:
            print(f"  No results for {lv}")
            continue

        total_trades = per_stock_df["trade_count"].sum()
        avg_return = per_stock_df["total_return_pct"].mean()
        median_return = per_stock_df["total_return_pct"].median()
        avg_dd = per_stock_df["max_drawdown_pct"].mean()
        avg_win_rate = per_stock_df["win_rate_pct"].mean()
        positive_pct = (per_stock_df["total_return_pct"] > 0).mean() * 100
        sharpe_avg = per_stock_df["sharpe"].mean()

        all_levels_summary.append({
            "level": lv,
            "n_stocks": len(per_stock_df),
            "total_trades": int(total_trades),
            "avg_trades_per_stock": round(total_trades / len(per_stock_df), 2),
            "avg_win_rate_pct": round(avg_win_rate, 2),
            "avg_return_pct": round(avg_return, 2),
            "median_return_pct": round(median_return, 2),
            "positive_stock_pct": round(positive_pct, 2),
            "avg_max_dd_pct": round(avg_dd, 2),
            "avg_sharpe": round(sharpe_avg, 3) if not pd.isna(sharpe_avg) else None,
        })

        per_stock_path = OUTPUT_DIR / f"box1_vbt_perstock_{lv}.csv"
        per_stock_df.to_csv(per_stock_path, index=False)
        if not trades_df.empty:
            trades_path = OUTPUT_DIR / f"box1_vbt_trades_{lv}.csv"
            trades_df.to_csv(trades_path, index=False)

    if all_levels_summary:
        summary_df = pd.DataFrame(all_levels_summary)
        print()
        print("=" * 100)
        print("BOX1 (一买封箱) × HS300 × VECTORBT SUMMARY")
        print("=" * 100)
        print(summary_df.to_string(index=False))
        print()
        summary_path = OUTPUT_DIR / "box1_vbt_summary.csv"
        summary_df.to_csv(summary_path, index=False)
        print(f"Summary saved: {summary_path}")


if __name__ == "__main__":
    main()
