#!/usr/bin/env python3
"""
box1_param_sweep.py  ——  一买封箱 SL/TP 参数扫描

利用 vectorbt 的参数组合能力, 一次性跑所有 (SL, TP) 组合, 找 A 股最优配置.

扫描空间:
  SL: [1.0, 1.5, 2.0, 2.5, 3.0] × ATR  (5 个)
  TP: [2.0, 3.0, 4.0, 5.0, 6.0, 8.0] × ATR  (6 个)
  = 30 个组合 × HS300 (300 只股票) × 3 周期 = 27000 次回测

评分目标: 综合考虑平均收益 + 胜率 + 回撤, 推出"最适合 A 股一买封箱"的 SL/TP

用法:
  python3 box1_param_sweep.py                                # 默认 HS300 daily
  python3 box1_param_sweep.py --levels 60m,4h                # 只扫 60m + 4h
  python3 box1_param_sweep.py --limit 30                     # 前 30 只测试
"""
from __future__ import annotations

import argparse
import sys
import warnings
from itertools import product
from pathlib import Path

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

try:
    import vectorbt as vbt
except ImportError:
    print("ERROR: vectorbt not installed")
    sys.exit(1)

sys.path.insert(0, str(Path(__file__).parent))
from box1_backtest import (  # noqa: E402
    Box1Config,
    detect_signals,
    load_dataframes,
    compute_macd_atr,
    HS300_PATH,
)

OUTPUT_DIR = Path("/Users/zhangyi/Documents/code/macd股票版/outputs/box1_sweep")


# ==============================
# Param space
# ==============================

SL_CANDIDATES = [1.0, 1.5, 2.0, 2.5, 3.0]
TP_CANDIDATES = [2.0, 3.0, 4.0, 5.0, 6.0, 8.0]


# ==============================
# Single-symbol sweep (vectorbt param combinatorial)
# ==============================


def sweep_one_symbol(
    df: pd.DataFrame,
    signals: list,
    cfg: Box1Config,
) -> pd.DataFrame:
    """Run all (SL, TP) combinations for one symbol using vectorbt param combo.

    Returns one row per (sl_mult, tp_mult) combination with metrics.
    """
    if not signals:
        return pd.DataFrame()

    data = compute_macd_atr(df, cfg).dropna(
        subset=["open", "high", "low", "close", "atr"]
    ).reset_index(drop=True)
    if len(data) < 60:
        return pd.DataFrame()

    index = pd.DatetimeIndex(data["date"])
    n = len(data)
    close = data["close"].to_numpy(float)
    atr = data["atr"].to_numpy(float)

    # Build entries (BUY only — A 股 long only)
    entries_np = np.zeros(n, dtype=bool)
    for sig in signals:
        if sig.side != "BUY":
            continue
        if sig.index >= n:
            continue
        entries_np[sig.index] = True

    if not entries_np.any():
        return pd.DataFrame()

    entries = pd.Series(entries_np, index=index)
    exits = pd.Series(np.zeros(n, dtype=bool), index=index)  # rely on SL/TP
    close_series = pd.Series(close, index=index, name="close")

    # Build per-bar atr/close ratio (the SL/TP distance in % for given multiplier = ratio × mult)
    atr_close = atr / np.where(close > 0, close, 1.0)
    atr_close_series = pd.Series(atr_close, index=index)

    rows = []
    for sl_mult, tp_mult in product(SL_CANDIDATES, TP_CANDIDATES):
        sl_stop = atr_close * sl_mult
        tp_stop = atr_close * tp_mult
        sl_series = pd.Series(np.where(entries_np, sl_stop, np.nan), index=index)
        tp_series = pd.Series(np.where(entries_np, tp_stop, np.nan), index=index)

        try:
            pf = vbt.Portfolio.from_signals(
                close=close_series,
                entries=entries,
                exits=exits,
                sl_stop=sl_series,
                tp_stop=tp_series,
                init_cash=cfg.initial_capital,
                fees=cfg.fee_rate,
                size=np.inf,
                direction="longonly",
                freq="1D",
            )
        except Exception:
            continue

        if pf.trades.count() == 0:
            continue

        total_return = float(pf.total_return())
        max_dd = float(pf.max_drawdown())
        wr = float(pf.trades.win_rate())
        rows.append({
            "sl_mult": sl_mult,
            "tp_mult": tp_mult,
            "trade_count": int(pf.trades.count()),
            "total_return_pct": round(total_return * 100, 4),
            "win_rate_pct": round(wr * 100 if not np.isnan(wr) else 0, 4),
            "max_drawdown_pct": round(max_dd * 100, 4),
        })

    return pd.DataFrame(rows)


# ==============================
# Main
# ==============================


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--levels", type=str, default="daily,4h")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--only-strong", action="store_true")
    parser.add_argument("--no-buy2", action="store_true")
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
    if args.limit:
        symbols = symbols[: args.limit]

    print(f"[Sweep] vectorbt={vbt.__version__}")
    print(f"[Sweep] universe={len(symbols)} levels={levels}")
    print(f"[Sweep] SL grid={SL_CANDIDATES}")
    print(f"[Sweep] TP grid={TP_CANDIDATES}")
    print(f"[Sweep] total combos per symbol per level = {len(SL_CANDIDATES) * len(TP_CANDIDATES)}")
    print()

    # For each level, aggregate across all symbols by (sl, tp) combo
    for lv in levels:
        print(f"\n=== Level: {lv} ===")
        all_rows = []
        ok_symbols = 0

        for idx, sym in enumerate(symbols):
            dfs = load_dataframes(sym, [lv])
            df = dfs.get(lv)
            if df is None or len(df) < 60:
                continue
            signals = detect_signals(df, cfg)
            if not signals:
                continue
            buy_sigs = [s for s in signals if s.side == "BUY"]
            if not buy_sigs:
                continue

            sweep = sweep_one_symbol(df, signals, cfg)
            if sweep.empty:
                continue
            sweep["symbol"] = sym
            all_rows.append(sweep)
            ok_symbols += 1

            if (idx + 1) % 30 == 0:
                print(f"  {idx+1}/{len(symbols)} scanned, {ok_symbols} with trades")

        if not all_rows:
            print(f"  No data for {lv}")
            continue

        all_df = pd.concat(all_rows, ignore_index=True)
        per_stock_path = OUTPUT_DIR / f"sweep_perstock_{lv}.csv"
        all_df.to_csv(per_stock_path, index=False)

        # Aggregate by (sl, tp)
        agg = all_df.groupby(["sl_mult", "tp_mult"]).agg(
            n_stocks=("symbol", "nunique"),
            total_trades=("trade_count", "sum"),
            avg_trades=("trade_count", "mean"),
            avg_return=("total_return_pct", "mean"),
            median_return=("total_return_pct", "median"),
            avg_win_rate=("win_rate_pct", "mean"),
            avg_max_dd=("max_drawdown_pct", "mean"),
            positive_stock_pct=("total_return_pct", lambda s: (s > 0).mean() * 100),
        ).reset_index()

        # Composite score: avg_return − 0.5 × avg_max_dd (favor return, penalize dd)
        agg["composite_score"] = (
            agg["avg_return"] - 0.5 * abs(agg["avg_max_dd"])
        ).round(2)

        agg = agg.sort_values("composite_score", ascending=False)
        agg_path = OUTPUT_DIR / f"sweep_agg_{lv}.csv"
        agg.to_csv(agg_path, index=False)

        print()
        print(f"  Top 10 (SL, TP) combos for {lv}:")
        print(agg.head(10).to_string(index=False))
        print()

        # Pivot: heatmap data
        pivot_return = agg.pivot(index="sl_mult", columns="tp_mult", values="avg_return").round(2)
        pivot_wr = agg.pivot(index="sl_mult", columns="tp_mult", values="avg_win_rate").round(1)
        print(f"  {lv} — avg_return heatmap (row=SL, col=TP, both ×ATR):")
        print(pivot_return.to_string())
        print()
        print(f"  {lv} — avg_win_rate heatmap:")
        print(pivot_wr.to_string())
        print()
        print(f"  Saved: {agg_path}, {per_stock_path}")


if __name__ == "__main__":
    main()
