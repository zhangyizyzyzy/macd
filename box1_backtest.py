#!/usr/bin/env python3
"""
box1_backtest.py  ——  一买封箱 HS300 回测

完整 1:1 移植 macd第二版/缠论一类实盘/signal_engine_base.js (lines 1755-2125)
的一买/一卖检测逻辑到 Python. 出场规则也与 ETH 版本保持一致:
  - SL: 1.5 × ATR
  - TP: 11  × ATR
  - 最长持仓 80 bars
  - (限价入场简化为市价入场, 因为 A 股没有 1m 数据来模拟限价成交)

用法:
  python3 box1_backtest.py                          # HS300 × 15m/60m/4h
  python3 box1_backtest.py --symbol 600519          # 单股测试 (打印所有信号)
  python3 box1_backtest.py --levels daily           # 指定周期
  python3 box1_backtest.py --limit 20               # 前 20 只测试
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd


PROJECT_ROOT = Path("/Users/zhangyi/Documents/code/macd股票版")
DB_PATH = PROJECT_ROOT / "data" / "market_data.sqlite"
HS300_PATH = PROJECT_ROOT / "data" / "hs300_current.csv"
OUTPUT_DIR = PROJECT_ROOT / "outputs" / "box1"


# ==============================
# Config
# ==============================


@dataclass(slots=True)
class Box1Config:
    macd_fast: int = 12
    macd_slow: int = 26
    macd_signal: int = 9
    atr_len: int = 14

    # ETH 原版出场参数
    sl_atr_mult: float = 1.5
    tp_atr_mult: float = 11.0
    max_hold_bars: int = 80

    # 信号过滤
    only_strong_buy1: bool = False              # 只取强一买, 过滤普通一买
    enable_buy2: bool = True                    # 是否在二买点也开仓 (补仓)
    allow_reversal: bool = True                 # 反向信号立即平仓反手

    # 仓位与成本
    initial_capital: float = 100_000.0
    fee_rate: float = 0.00025                   # 0.025% (包含印花税估算)

    # 交易方向
    long_only: bool = True                      # A 股默认禁空


# ==============================
# Indicators
# ==============================


def compute_macd_atr(df: pd.DataFrame, cfg: Box1Config) -> pd.DataFrame:
    out = df.copy()
    ema_fast = out["close"].ewm(span=cfg.macd_fast, adjust=False).mean()
    ema_slow = out["close"].ewm(span=cfg.macd_slow, adjust=False).mean()
    out["macd"] = ema_fast - ema_slow
    out["signal"] = out["macd"].ewm(span=cfg.macd_signal, adjust=False).mean()
    out["hist"] = out["macd"] - out["signal"]

    prev_close = out["close"].shift(1)
    tr = pd.concat(
        [
            (out["high"] - out["low"]).abs(),
            (out["high"] - prev_close).abs(),
            (out["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    out["atr"] = tr.rolling(cfg.atr_len, min_periods=1).mean()
    return out


# ==============================
# 1:1 port of signal_engine_base.js runBacktest (lines 1755-2125)
# ==============================


@dataclass(slots=True)
class Signal:
    index: int
    timestamp: pd.Timestamp
    price: float
    side: str           # "BUY" / "SELL"
    kind: str           # "1" / "2"
    code: str           # "BUY1" / "STRONG_BUY1" / "BUY2" / "SELL1" / ...
    strength: str       # "STRONG" / "NORMAL"
    pivot_price: float  # b1_low or s1_high (the sealed-box anchor)


def detect_signals(df: pd.DataFrame, cfg: Box1Config) -> list[Signal]:
    """直译自 signal_engine_base.js runBacktest().

    检测一买/二买/一卖/二卖, 不做任何 A 股特有改动.
    """
    data = compute_macd_atr(df, cfg).dropna(
        subset=["open", "high", "low", "close", "macd", "signal", "hist", "atr"]
    ).reset_index(drop=True)
    n = len(data)
    if n < 5:
        return []

    low_arr = data["low"].to_numpy(float)
    high_arr = data["high"].to_numpy(float)
    close_arr = data["close"].to_numpy(float)
    macd_arr = data["macd"].to_numpy(float)
    signal_arr = data["signal"].to_numpy(float)
    hist_arr = data["hist"].to_numpy(float)
    ts_arr = data["date"].tolist()

    # 3-window red/green trackers
    r1_p = r1_h = r1_m = None
    r2_p = r2_h = r2_m = None
    r3_p = r3_h = r3_m = None
    g1_p = g1_h = g1_m = None
    g2_p = g2_h = g2_m = None
    g3_p = g3_h = g3_m = None

    wait_b2 = False
    b1_low: Optional[float] = None
    arm_b2 = False
    wait_s2 = False
    s1_high: Optional[float] = None
    arm_s2 = False

    signals: list[Signal] = []

    for i in range(2, n):
        low = low_arr[i]
        high = high_arr[i]
        h = hist_arr[i]
        m = macd_arr[i]
        s = signal_arr[i]
        hist1 = hist_arr[i - 1]
        macd1 = macd_arr[i - 1]
        signal1 = signal_arr[i - 1]

        cu_hist = h < 0 and hist1 >= 0      # hist 转红 (由正转负)
        co_hist = h > 0 and hist1 <= 0      # hist 转绿 (由负转正)
        dc = m < s and macd1 >= signal1     # 死叉
        gc = m > s and macd1 <= signal1     # 金叉

        # Red tracker update
        if cu_hist:
            r1_p, r1_h, r1_m = r2_p, r2_h, r2_m
            r2_p, r2_h, r2_m = r3_p, r3_h, r3_m
            r3_p, r3_h, r3_m = low, h, m
        if h < 0:
            r3_p = low if r3_p is None else min(r3_p, low)
            r3_h = h if r3_h is None else min(r3_h, h)
            r3_m = m if r3_m is None else min(r3_m, m)

        # Green tracker update
        if co_hist:
            g1_p, g1_h, g1_m = g2_p, g2_h, g2_m
            g2_p, g2_h, g2_m = g3_p, g3_h, g3_m
            g3_p, g3_h, g3_m = high, h, m
        if h > 0:
            g3_p = high if g3_p is None else max(g3_p, high)
            g3_h = h if g3_h is None else max(g3_h, h)
            g3_m = m if g3_m is None else max(g3_m, m)

        # Divergence cores
        div_dn = r2_h is not None and r3_h is not None and (
            (r3_h > r2_h) or (r3_m is not None and r2_m is not None and r3_m > r2_m)
        )
        div_up = g2_h is not None and g3_h is not None and (
            (g3_h < g2_h) or (g3_m is not None and g2_m is not None and g3_m < g2_m)
        )

        # Buy1 / StrongBuy1
        is_buy1 = (
            gc
            and m < 0
            and r2_p is not None
            and r3_p is not None
            and r3_p < r2_p
            and div_dn
            and not (r1_p is not None and r2_p is not None and r3_p is not None and r3_p < r2_p and r2_p < r1_p)
        )
        is_strong_buy1 = (
            gc
            and m < 0
            and r1_p is not None
            and r2_p is not None
            and r3_p is not None
            and r3_p < r2_p
            and r2_p < r1_p
            and div_dn
        )

        # Sell1 / StrongSell1
        is_sell1 = (
            dc
            and m > 0
            and g2_p is not None
            and g3_p is not None
            and g3_p > g2_p
            and div_up
            and not (g1_p is not None and g2_p is not None and g3_p is not None and g3_p > g2_p and g2_p > g1_p)
        )
        is_strong_sell1 = (
            dc
            and m > 0
            and g1_p is not None
            and g2_p is not None
            and g3_p is not None
            and g3_p > g2_p
            and g2_p > g1_p
            and div_up
        )

        # ---- Seal box state machine (buy side) ----
        if is_buy1 or is_strong_buy1:
            wait_b2 = True
            b1_low = r3_p
            arm_b2 = False

        if wait_b2 and b1_low is not None:
            if low <= b1_low:
                wait_b2 = False
                arm_b2 = False
            elif dc or cu_hist:
                arm_b2 = True
        is_buy2 = wait_b2 and arm_b2 and gc and b1_low is not None and low > b1_low
        if is_buy2:
            wait_b2 = False
            arm_b2 = False

        # ---- Seal box state machine (sell side) ----
        if is_sell1 or is_strong_sell1:
            wait_s2 = True
            s1_high = g3_p
            arm_s2 = False

        if wait_s2 and s1_high is not None:
            if high >= s1_high:
                wait_s2 = False
                arm_s2 = False
            elif gc or co_hist:
                arm_s2 = True
        is_sell2 = wait_s2 and arm_s2 and dc and s1_high is not None and high < s1_high
        if is_sell2:
            wait_s2 = False
            arm_s2 = False

        # Emit signals
        if is_strong_buy1 or is_buy1:
            signals.append(Signal(
                index=i,
                timestamp=pd.Timestamp(ts_arr[i]),
                price=float(close_arr[i]),
                side="BUY",
                kind="1",
                code="STRONG_BUY1" if is_strong_buy1 else "BUY1",
                strength="STRONG" if is_strong_buy1 else "NORMAL",
                pivot_price=float(r3_p) if r3_p is not None else float(low),
            ))
        elif is_buy2 and cfg.enable_buy2:
            signals.append(Signal(
                index=i,
                timestamp=pd.Timestamp(ts_arr[i]),
                price=float(close_arr[i]),
                side="BUY",
                kind="2",
                code="BUY2",
                strength="NORMAL",
                pivot_price=float(b1_low) if b1_low is not None else float(low),
            ))
        elif is_strong_sell1 or is_sell1:
            signals.append(Signal(
                index=i,
                timestamp=pd.Timestamp(ts_arr[i]),
                price=float(close_arr[i]),
                side="SELL",
                kind="1",
                code="STRONG_SELL1" if is_strong_sell1 else "SELL1",
                strength="STRONG" if is_strong_sell1 else "NORMAL",
                pivot_price=float(g3_p) if g3_p is not None else float(high),
            ))
        elif is_sell2 and cfg.enable_buy2:
            signals.append(Signal(
                index=i,
                timestamp=pd.Timestamp(ts_arr[i]),
                price=float(close_arr[i]),
                side="SELL",
                kind="2",
                code="SELL2",
                strength="NORMAL",
                pivot_price=float(s1_high) if s1_high is not None else float(high),
            ))

    return signals


# ==============================
# Backtest engine (ETH-style exit rules)
# ==============================


@dataclass(slots=True)
class Trade:
    entry_time: pd.Timestamp
    exit_time: pd.Timestamp
    entry_price: float
    exit_price: float
    side: str
    entry_code: str  # BUY1 / STRONG_BUY1 / BUY2
    exit_reason: str  # SL / TP / TIME / REVERSAL
    bars_held: int
    sl_price: float
    tp_price: float
    gross_pnl_pct: float
    net_pnl_pct: float


def backtest(
    df: pd.DataFrame,
    signals: list[Signal],
    cfg: Box1Config,
) -> tuple[list[Trade], dict]:
    """ETH 原版简化回测:
    - 每个信号收盘价进场 (替代原版的 0.5×ATR 限价, 因为 A 股没 1m 数据)
    - SL = entry ± 1.5×ATR, TP = entry ± 11×ATR
    - 最长持仓 80 bars
    - 反向信号强制平仓 (若 allow_reversal)
    - 一次只持一仓
    """
    data = compute_macd_atr(df, cfg).dropna(
        subset=["open", "high", "low", "close", "macd", "signal", "hist", "atr"]
    ).reset_index(drop=True)
    if data.empty:
        return [], {"trade_count": 0}

    high_arr = data["high"].to_numpy(float)
    low_arr = data["low"].to_numpy(float)
    close_arr = data["close"].to_numpy(float)
    atr_arr = data["atr"].to_numpy(float)
    ts_arr = data["date"].tolist()

    # Map signals by index for quick lookup (one signal per bar max, but keep list)
    sig_by_idx: dict[int, list[Signal]] = {}
    for s in signals:
        sig_by_idx.setdefault(s.index, []).append(s)

    trades: list[Trade] = []
    n = len(data)

    # Position state
    position_side: Optional[str] = None  # "BUY" / "SELL" / None
    entry_idx = -1
    entry_price = 0.0
    entry_atr = 0.0
    sl_price = 0.0
    tp_price = 0.0
    entry_code = ""

    def close_position(i: int, price: float, reason: str):
        nonlocal position_side
        direction = 1 if position_side == "BUY" else -1
        gross = (price - entry_price) / entry_price * direction
        net = gross - 2 * cfg.fee_rate  # entry + exit fee
        trades.append(Trade(
            entry_time=pd.Timestamp(ts_arr[entry_idx]),
            exit_time=pd.Timestamp(ts_arr[i]),
            entry_price=entry_price,
            exit_price=price,
            side=position_side,  # type: ignore
            entry_code=entry_code,
            exit_reason=reason,
            bars_held=i - entry_idx,
            sl_price=sl_price,
            tp_price=tp_price,
            gross_pnl_pct=gross * 100,
            net_pnl_pct=net * 100,
        ))
        position_side = None

    for i in range(n):
        events = sig_by_idx.get(i, [])

        # 1) If in position, check SL/TP/TIME
        if position_side is not None:
            bar_high = high_arr[i]
            bar_low = low_arr[i]
            if position_side == "BUY":
                hit_sl = bar_low <= sl_price
                hit_tp = bar_high >= tp_price
                if hit_sl and hit_tp:
                    close_position(i, sl_price, "SL")
                elif hit_sl:
                    close_position(i, sl_price, "SL")
                elif hit_tp:
                    close_position(i, tp_price, "TP")
                elif (i - entry_idx) >= cfg.max_hold_bars:
                    close_position(i, close_arr[i], "TIME")
            else:  # SHORT
                hit_sl = bar_high >= sl_price
                hit_tp = bar_low <= tp_price
                if hit_sl and hit_tp:
                    close_position(i, sl_price, "SL")
                elif hit_sl:
                    close_position(i, sl_price, "SL")
                elif hit_tp:
                    close_position(i, tp_price, "TP")
                elif (i - entry_idx) >= cfg.max_hold_bars:
                    close_position(i, close_arr[i], "TIME")

        # 2) Reversal: opposite side signal while holding position
        if position_side is not None and cfg.allow_reversal:
            for sig in events:
                if (position_side == "BUY" and sig.side == "SELL") or (position_side == "SELL" and sig.side == "BUY"):
                    close_position(i, close_arr[i], "REVERSAL")
                    break

        # 3) Open new position if signal and flat
        if position_side is None:
            for sig in events:
                if cfg.only_strong_buy1 and sig.code not in ("STRONG_BUY1", "STRONG_SELL1"):
                    continue
                if cfg.long_only and sig.side == "SELL":
                    continue
                atr_now = atr_arr[i]
                if atr_now <= 0:
                    continue
                position_side = sig.side
                entry_idx = i
                entry_price = close_arr[i]
                entry_atr = float(atr_now)
                entry_code = sig.code
                if sig.side == "BUY":
                    sl_price = entry_price - cfg.sl_atr_mult * atr_now
                    tp_price = entry_price + cfg.tp_atr_mult * atr_now
                else:
                    sl_price = entry_price + cfg.sl_atr_mult * atr_now
                    tp_price = entry_price - cfg.tp_atr_mult * atr_now
                break

    # Force close at end
    if position_side is not None:
        close_position(n - 1, close_arr[n - 1], "END")

    # Summary
    if trades:
        wins = sum(1 for t in trades if t.net_pnl_pct > 0)
        total_net = sum(t.net_pnl_pct for t in trades)
        by_reason = {}
        by_code = {}
        for t in trades:
            by_reason[t.exit_reason] = by_reason.get(t.exit_reason, 0) + 1
            by_code[t.entry_code] = by_code.get(t.entry_code, 0) + 1
    else:
        wins = 0
        total_net = 0
        by_reason = {}
        by_code = {}

    return trades, {
        "trade_count": len(trades),
        "win_rate_pct": (wins / len(trades) * 100) if trades else 0.0,
        "total_net_pnl_pct": total_net,
        "avg_net_pnl_pct": (total_net / len(trades)) if trades else 0.0,
        "avg_bars_held": np.mean([t.bars_held for t in trades]) if trades else 0,
        "by_exit_reason": by_reason,
        "by_entry_code": by_code,
    }


# ==============================
# Data loading
# ==============================


def load_symbol_bars(symbol: str, level: str) -> pd.DataFrame:
    con = sqlite3.connect(str(DB_PATH))
    try:
        df = pd.read_sql_query(
            "SELECT date, open, high, low, close, volume FROM kline "
            "WHERE symbol=? AND level=? AND adjust='qfq' ORDER BY date",
            con, params=(symbol, level),
        )
    finally:
        con.close()
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    return df


def resample_bars(df_15m: pd.DataFrame, rule: str) -> pd.DataFrame:
    if df_15m.empty:
        return df_15m
    dfi = df_15m.set_index("date")
    agg = dfi.resample(rule, label="right", closed="right").agg(
        {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
    ).dropna().reset_index()
    return agg


def load_dataframes(symbol: str, levels: list[str]) -> dict[str, pd.DataFrame]:
    out: dict[str, pd.DataFrame] = {}
    if "daily" in levels:
        df_d = load_symbol_bars(symbol, "daily")
        if not df_d.empty:
            out["daily"] = df_d
    if any(lv in levels for lv in ("15m", "60m", "4h")):
        df_15m = load_symbol_bars(symbol, "15m")
        if not df_15m.empty:
            if "15m" in levels:
                out["15m"] = df_15m
            if "60m" in levels:
                out["60m"] = resample_bars(df_15m, "60min")
            if "4h" in levels:
                out["4h"] = resample_bars(df_15m, "240min")
    return out


# ==============================
# Main
# ==============================


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", type=str, default=None)
    parser.add_argument("--levels", type=str, default="15m,60m,4h,daily")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--only-strong", action="store_true", help="Only STRONG_BUY1/SELL1")
    parser.add_argument("--no-buy2", action="store_true", help="Disable buy2/sell2 entries")
    parser.add_argument("--long-only", action="store_true", default=True)
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    levels = [lv.strip() for lv in args.levels.split(",")]
    cfg = Box1Config(
        only_strong_buy1=args.only_strong,
        enable_buy2=not args.no_buy2,
        long_only=args.long_only,
    )

    hs300 = pd.read_csv(HS300_PATH, dtype={"symbol": str})
    symbols = [str(s).zfill(6) for s in hs300["symbol"]]
    if args.symbol:
        symbols = [args.symbol.zfill(6)]
    elif args.limit:
        symbols = symbols[: args.limit]

    print(f"[Box1] universe={len(symbols)} levels={levels}")
    print(f"[Box1] cfg: SL={cfg.sl_atr_mult}×ATR TP={cfg.tp_atr_mult}×ATR max_hold={cfg.max_hold_bars}bars only_strong={cfg.only_strong_buy1} enable_buy2={cfg.enable_buy2}")
    print()

    # stats[level] = list of per-stock trade records
    level_trades: dict[str, list[Trade]] = {lv: [] for lv in levels}
    level_summary: dict[str, list[dict]] = {lv: [] for lv in levels}
    single_signal_log = []

    for idx, sym in enumerate(symbols):
        dfs = load_dataframes(sym, levels)
        if not dfs:
            continue

        for lv in levels:
            df = dfs.get(lv)
            if df is None or len(df) < 60:
                continue
            signals = detect_signals(df, cfg)
            if args.symbol:
                single_signal_log.append((lv, signals))
            if not signals:
                continue
            trades, summary = backtest(df, signals, cfg)
            if not trades:
                continue
            level_trades[lv].extend(trades)
            level_summary[lv].append({
                "symbol": sym,
                "n_signals": len(signals),
                **{k: v for k, v in summary.items() if not isinstance(v, dict)},
            })

        if (idx + 1) % 30 == 0:
            status = " ".join(f"{lv}={len(level_trades[lv])}" for lv in levels)
            print(f"  [{idx+1:3d}/{len(symbols)}] trades: {status}")

    # ---- Single-symbol verbose output ----
    if args.symbol:
        print()
        print(f"=== Signals for {args.symbol} ===")
        for lv, sigs in single_signal_log:
            print(f"\n-- {lv} ({len(sigs)} signals) --")
            for s in sigs[-20:]:
                print(f"  {s.timestamp}  {s.code:12s}  price={s.price:.2f}  pivot={s.pivot_price:.2f}")
        print()

    # ---- Summary ----
    print()
    print("=" * 95)
    print("BOX1 BACKTEST RESULTS")
    print("=" * 95)
    print(f"{'Level':8s} {'Trades':>8s} {'Win%':>8s} {'Sum Net%':>10s} {'Avg Net%':>10s} {'AvgHold':>9s} {'Reasons':>30s}")
    print("-" * 95)
    report_rows = []
    for lv in levels:
        tr = level_trades[lv]
        if not tr:
            print(f"{lv:8s} {'0':>8s}  (no trades)")
            continue
        wins = sum(1 for t in tr if t.net_pnl_pct > 0)
        win_rate = wins / len(tr) * 100
        total_net = sum(t.net_pnl_pct for t in tr)
        avg_net = total_net / len(tr)
        avg_hold = np.mean([t.bars_held for t in tr])
        reason_counts = {}
        for t in tr:
            reason_counts[t.exit_reason] = reason_counts.get(t.exit_reason, 0) + 1
        reasons_str = " ".join(f"{k}:{v}" for k, v in reason_counts.items())
        print(f"{lv:8s} {len(tr):>8d} {win_rate:>7.2f}% {total_net:>+9.2f}% {avg_net:>+9.2f}% {avg_hold:>8.1f}  {reasons_str}")
        report_rows.append({
            "level": lv,
            "trades": len(tr),
            "win_rate_pct": round(win_rate, 2),
            "total_net_pnl_pct": round(total_net, 2),
            "avg_net_pnl_pct": round(avg_net, 4),
            "avg_bars_held": round(avg_hold, 1),
            **{f"exit_{k}": v for k, v in reason_counts.items()},
        })
    print()

    if report_rows and not args.symbol:
        # save
        summary_df = pd.DataFrame(report_rows)
        summary_path = OUTPUT_DIR / f"box1_summary.csv"
        summary_df.to_csv(summary_path, index=False)
        print(f"Summary: {summary_path}")

        # trades dump
        all_trades_rows = []
        for lv, trs in level_trades.items():
            for t in trs:
                all_trades_rows.append({
                    "level": lv,
                    "entry_time": t.entry_time,
                    "exit_time": t.exit_time,
                    "side": t.side,
                    "entry_code": t.entry_code,
                    "exit_reason": t.exit_reason,
                    "entry_price": round(t.entry_price, 4),
                    "exit_price": round(t.exit_price, 4),
                    "bars_held": t.bars_held,
                    "sl_price": round(t.sl_price, 4),
                    "tp_price": round(t.tp_price, 4),
                    "gross_pnl_pct": round(t.gross_pnl_pct, 4),
                    "net_pnl_pct": round(t.net_pnl_pct, 4),
                })
        if all_trades_rows:
            trades_path = OUTPUT_DIR / f"box1_trades.csv"
            pd.DataFrame(all_trades_rows).to_csv(trades_path, index=False)
            print(f"Trades:  {trades_path}")


if __name__ == "__main__":
    main()
