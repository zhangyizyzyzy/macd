#!/usr/bin/env python3
"""
box1_b1low_backtest.py — 一买封箱 b1_low 动态止损版

核心改动 (相对 box1_vectorbt_backtest.py):
  - SL 不再是固定 entry - N×ATR
  - SL = b1_low - small_buffer (即一买点的 r3_p 支撑位)
  - 只要一买支撑不破就持有, 破了立即止损
  - TP 做参数扫描 (N×ATR 或 N×rise_range)

这才是"一买封箱"真正的缠论含义:
  b1_low 就是"箱底", 跌破等于箱子破了, 出局
  TP 用来捕捉上涨行情

用法:
  python3 box1_b1low_backtest.py                      # 默认 HS300 daily
  python3 box1_b1low_backtest.py --symbol 600519      # 单股详细输出
  python3 box1_b1low_backtest.py --levels daily,4h    # 多周期
  python3 box1_b1low_backtest.py --sweep-tp           # 扫描 TP 参数
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
    Signal,
    compute_macd_atr,
    detect_signals,
    load_dataframes,
    HS300_PATH,
)

OUTPUT_DIR = Path("/Users/zhangyi/Documents/code/macd股票版/outputs/box1_b1low")


# ==============================
# Custom backtest with b1_low stops
# ==============================


def backtest_b1low(
    df: pd.DataFrame,
    signals: list[Signal],
    cfg: Box1Config,
    tp_atr_mult: float,
    sl_buffer_atr: float = 0.3,
    max_hold_bars: int = 250,
    enforce_t1: bool = True,
    entry_kind: str = "1",   # "1"=只在一买进, "2"=只在二买进, "both"=都进
) -> dict:
    """
    Custom loop: one position at a time, SL=b1_low-buffer, TP=entry+N×ATR.

    Returns dict with:
        trades: list of trade dicts
        summary: dict of metrics
    """
    if not signals:
        return {"trades": [], "summary": {}}

    data = compute_macd_atr(df, cfg).dropna(
        subset=["open", "high", "low", "close", "atr"]
    ).reset_index(drop=True)
    n = len(data)
    if n < 30:
        return {"trades": [], "summary": {}}

    high = data["high"].to_numpy(float)
    low = data["low"].to_numpy(float)
    close = data["close"].to_numpy(float)
    open_ = data["open"].to_numpy(float)
    atr = data["atr"].to_numpy(float)
    ts = data["date"].tolist()

    # Only BUY signals (A-share long only)
    allowed_kinds = {"1", "2"} if entry_kind == "both" else {entry_kind}
    sig_by_idx: dict[int, Signal] = {}
    for s in signals:
        if s.side != "BUY":
            continue
        if cfg.only_strong_buy1 and s.code not in ("STRONG_BUY1",):
            continue
        if s.kind not in allowed_kinds:
            continue
        sig_by_idx[s.index] = s

    if not sig_by_idx:
        return {"trades": [], "summary": {}}

    trades = []

    in_pos = False
    entry_i = -1
    entry_price = 0.0
    sl_price = 0.0
    tp_price = 0.0
    b1_low_price = 0.0
    entry_code = ""
    entry_atr = 0.0
    equity = cfg.initial_capital

    for i in range(n):
        if in_pos:
            bar_high = high[i]
            bar_low = low[i]
            exited = False
            exit_price = None
            exit_reason = None

            # Prioritize SL (pessimistic)
            if bar_low <= sl_price:
                exit_price = sl_price
                exit_reason = "SL"
                exited = True
            elif bar_high >= tp_price:
                exit_price = tp_price
                exit_reason = "TP"
                exited = True
            elif (i - entry_i) >= max_hold_bars:
                exit_price = close[i]
                exit_reason = "TIME"
                exited = True

            # T+1: can't close on same-bar as entry
            if enforce_t1 and i == entry_i:
                exited = False

            if exited:
                gross_ret = (exit_price - entry_price) / entry_price
                net_ret = gross_ret - 2 * cfg.fee_rate
                equity *= (1 + net_ret)
                trades.append({
                    "entry_time": pd.Timestamp(ts[entry_i]),
                    "exit_time": pd.Timestamp(ts[i]),
                    "entry_code": entry_code,
                    "entry_price": float(entry_price),
                    "exit_price": float(exit_price),
                    "sl_price": float(sl_price),
                    "tp_price": float(tp_price),
                    "b1_low": float(b1_low_price),
                    "sl_distance_pct": (entry_price - sl_price) / entry_price * 100,
                    "tp_distance_pct": (tp_price - entry_price) / entry_price * 100,
                    "exit_reason": exit_reason,
                    "bars_held": i - entry_i,
                    "gross_return_pct": gross_ret * 100,
                    "net_return_pct": net_ret * 100,
                    "equity_after": equity,
                })
                in_pos = False

        if not in_pos and i in sig_by_idx:
            sig = sig_by_idx[i]
            if atr[i] <= 0 or close[i] <= 0:
                continue
            in_pos = True
            entry_i = i
            entry_price = close[i]
            entry_atr = atr[i]
            entry_code = sig.code
            b1_low_price = sig.pivot_price  # r3_p = 一买点红柱最低价
            # SL = b1_low - small ATR buffer (允许 anchor 被刺穿一点点)
            sl_price = b1_low_price - sl_buffer_atr * atr[i]
            # TP = entry + N × ATR
            tp_price = entry_price + tp_atr_mult * atr[i]

            # Safety check: if SL is above entry (b1_low > close, weird),
            # fallback to entry - 1.5*ATR
            if sl_price >= entry_price:
                sl_price = entry_price - 1.5 * atr[i]

    # Close any open position at end
    if in_pos:
        exit_price = close[-1]
        gross_ret = (exit_price - entry_price) / entry_price
        net_ret = gross_ret - 2 * cfg.fee_rate
        equity *= (1 + net_ret)
        trades.append({
            "entry_time": pd.Timestamp(ts[entry_i]),
            "exit_time": pd.Timestamp(ts[-1]),
            "entry_code": entry_code,
            "entry_price": float(entry_price),
            "exit_price": float(exit_price),
            "sl_price": float(sl_price),
            "tp_price": float(tp_price),
            "b1_low": float(b1_low_price),
            "sl_distance_pct": (entry_price - sl_price) / entry_price * 100,
            "tp_distance_pct": (tp_price - entry_price) / entry_price * 100,
            "exit_reason": "END",
            "bars_held": n - 1 - entry_i,
            "gross_return_pct": gross_ret * 100,
            "net_return_pct": net_ret * 100,
            "equity_after": equity,
        })

    if not trades:
        return {"trades": [], "summary": {}}

    df_tr = pd.DataFrame(trades)
    wins = (df_tr["net_return_pct"] > 0).sum()
    return {
        "trades": trades,
        "summary": {
            "trade_count": len(trades),
            "win_rate_pct": wins / len(trades) * 100,
            "final_equity": equity,
            "total_return_pct": (equity / cfg.initial_capital - 1) * 100,
            "avg_return_pct": df_tr["net_return_pct"].mean(),
            "median_return_pct": df_tr["net_return_pct"].median(),
            "avg_bars_held": df_tr["bars_held"].mean(),
            "avg_sl_distance_pct": df_tr["sl_distance_pct"].mean(),
            "exit_reasons": df_tr["exit_reason"].value_counts().to_dict(),
        }
    }


# ==============================
# Main
# ==============================


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", type=str, default=None)
    parser.add_argument("--levels", type=str, default="daily,4h")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--only-strong", action="store_true")
    parser.add_argument("--sweep-tp", action="store_true", help="Sweep TP multipliers")
    parser.add_argument("--max-hold", type=int, default=250)
    parser.add_argument("--tp-mult", type=float, default=6.0)
    parser.add_argument("--sl-buffer", type=float, default=0.3)
    parser.add_argument("--entry-kind", type=str, default="1", choices=["1", "2", "both"],
                        help="一买='1' / 二买='2' / 都进='both'")
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    levels = [lv.strip() for lv in args.levels.split(",")]

    cfg = Box1Config(
        only_strong_buy1=args.only_strong,
        enable_buy2=True,  # 让 detect_signals 产生 BUY2 信号
        long_only=True,
    )

    hs300 = pd.read_csv(HS300_PATH, dtype={"symbol": str})
    symbols = [str(s).zfill(6) for s in hs300["symbol"]]
    if args.symbol:
        symbols = [args.symbol.zfill(6)]
    elif args.limit:
        symbols = symbols[: args.limit]

    print(f"[b1_low] universe={len(symbols)} levels={levels}")
    print(f"[b1_low] cfg: SL=b1_low-{args.sl_buffer}×ATR  TP=entry+{args.tp_mult}×ATR  max_hold={args.max_hold}bars")
    print()

    # ---- Single symbol detail ----
    if args.symbol:
        for lv in levels:
            dfs = load_dataframes(args.symbol, [lv])
            df = dfs.get(lv)
            if df is None:
                continue
            signals = detect_signals(df, cfg)
            result = backtest_b1low(
                df, signals, cfg,
                tp_atr_mult=args.tp_mult,
                sl_buffer_atr=args.sl_buffer,
                max_hold_bars=args.max_hold,
                entry_kind=args.entry_kind,
            )
            print(f"\n=== {args.symbol} {lv} ===")
            summary = result["summary"]
            if not summary:
                print("  no trades")
                continue
            print(f"  trades:        {summary['trade_count']}")
            print(f"  win_rate:      {summary['win_rate_pct']:.2f}%")
            print(f"  final_equity:  {summary['final_equity']:.2f}")
            print(f"  total_return:  {summary['total_return_pct']:+.2f}%")
            print(f"  avg_return:    {summary['avg_return_pct']:+.4f}%")
            print(f"  avg_bars_held: {summary['avg_bars_held']:.1f}")
            print(f"  avg_SL_dist:   {summary['avg_sl_distance_pct']:.2f}%")
            print(f"  exit_reasons:  {summary['exit_reasons']}")
            if len(result["trades"]) <= 30:
                df_tr = pd.DataFrame(result["trades"])
                print("\n  Trades:")
                cols = ["entry_time", "exit_time", "exit_reason", "entry_price", "exit_price",
                        "sl_distance_pct", "bars_held", "net_return_pct"]
                print(df_tr[cols].to_string(index=False))
        return

    # ---- TP sweep mode ----
    if args.sweep_tp:
        tp_grid = [3.0, 4.0, 5.0, 6.0, 8.0, 10.0, 12.0, 15.0]
        print(f"Sweeping TP multipliers: {tp_grid}")
        print()
        for lv in levels:
            print(f"\n=== Level: {lv} ===")
            rows = []
            for tp_mult in tp_grid:
                # Aggregate across all symbols for this TP
                total_trades = 0
                total_wins = 0
                total_pnl = 0.0
                per_stock_returns = []
                stocks_with_trades = 0
                for sym in symbols:
                    dfs = load_dataframes(sym, [lv])
                    df = dfs.get(lv)
                    if df is None or len(df) < 60:
                        continue
                    signals = detect_signals(df, cfg)
                    result = backtest_b1low(
                        df, signals, cfg,
                        tp_atr_mult=tp_mult,
                        sl_buffer_atr=args.sl_buffer,
                        max_hold_bars=args.max_hold,
                        entry_kind=args.entry_kind,
                    )
                    s = result["summary"]
                    if not s:
                        continue
                    stocks_with_trades += 1
                    total_trades += s["trade_count"]
                    total_wins += s["win_rate_pct"] / 100 * s["trade_count"]
                    per_stock_returns.append(s["total_return_pct"])
                if not per_stock_returns:
                    continue
                rows.append({
                    "tp_mult": tp_mult,
                    "stocks": stocks_with_trades,
                    "trades": total_trades,
                    "avg_trades": round(total_trades / stocks_with_trades, 2),
                    "win_rate": round(total_wins / total_trades * 100, 2) if total_trades else 0,
                    "avg_stock_return": round(np.mean(per_stock_returns), 2),
                    "median_stock_return": round(np.median(per_stock_returns), 2),
                    "positive_pct": round((np.array(per_stock_returns) > 0).mean() * 100, 2),
                })
            df_sweep = pd.DataFrame(rows)
            print(df_sweep.to_string(index=False))
            out = OUTPUT_DIR / f"b1low_sweep_{lv}.csv"
            df_sweep.to_csv(out, index=False)
            print(f"Saved: {out}")
        return

    # ---- Default multi-stock run ----
    all_summary = []
    for lv in levels:
        print(f"\n=== Level: {lv} ===")
        rows = []
        all_trades_rows = []
        for idx, sym in enumerate(symbols):
            dfs = load_dataframes(sym, [lv])
            df = dfs.get(lv)
            if df is None or len(df) < 60:
                continue
            signals = detect_signals(df, cfg)
            result = backtest_b1low(
                df, signals, cfg,
                tp_atr_mult=args.tp_mult,
                sl_buffer_atr=args.sl_buffer,
                max_hold_bars=args.max_hold,
                entry_kind=args.entry_kind,
            )
            s = result["summary"]
            if not s:
                continue
            rows.append({"symbol": sym, **{k: v for k, v in s.items() if not isinstance(v, dict)}})
            for t in result["trades"]:
                all_trades_rows.append({"symbol": sym, "level": lv, **t})
            if (idx + 1) % 30 == 0:
                print(f"  {idx+1}/{len(symbols)} | {len(rows)} stocks with trades")
        if not rows:
            continue
        per_stock = pd.DataFrame(rows)
        all_trades_df = pd.DataFrame(all_trades_rows)
        total_tr = per_stock["trade_count"].sum()
        overall_wr = ((all_trades_df["net_return_pct"] > 0).sum() / len(all_trades_df) * 100) if len(all_trades_df) else 0
        all_summary.append({
            "level": lv,
            "stocks": len(per_stock),
            "total_trades": int(total_tr),
            "overall_win_rate": round(overall_wr, 2),
            "avg_stock_return_pct": round(per_stock["total_return_pct"].mean(), 2),
            "median_stock_return_pct": round(per_stock["total_return_pct"].median(), 2),
            "positive_stock_pct": round((per_stock["total_return_pct"] > 0).mean() * 100, 2),
            "avg_avg_trade_pct": round(per_stock["avg_return_pct"].mean(), 4),
            "avg_bars_held": round(per_stock["avg_bars_held"].mean(), 1),
        })
        per_stock.to_csv(OUTPUT_DIR / f"b1low_perstock_{lv}.csv", index=False)
        all_trades_df.to_csv(OUTPUT_DIR / f"b1low_trades_{lv}.csv", index=False)

    if all_summary:
        summary_df = pd.DataFrame(all_summary)
        print()
        print("=" * 100)
        print("BOX1 b1_low 动态止损版 × HS300")
        print("=" * 100)
        print(summary_df.to_string(index=False))
        print()
        summary_df.to_csv(OUTPUT_DIR / "b1low_summary.csv", index=False)


if __name__ == "__main__":
    main()
