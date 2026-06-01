#!/usr/bin/env python3
"""
IBUY2 (macd股票版 v1.3.0) vs family2 二买 (macd第二版) on A-share HS300.

使用现成的 Chan Quant 回测 SDK (Playground/python/backtest.py) 作为引擎，
跑反向信号出场，对比两个独立二买策略。

时间周期: 15m / 60m / 4h (60m/4h 从 15m 重采样得到)
出场逻辑: 反向信号出场 (IBUY2<->ISELL2, family2_BUY<->family2_SELL)
"""
from __future__ import annotations

import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

# Add SDK path
PLAYGROUND_ROOT = Path("/Users/zhangyi/Documents/Playground")
sys.path.insert(0, str(PLAYGROUND_ROOT))
from python.backtest import run_signal_backtest  # noqa: E402

# Add scanner path
SCANNER_ROOT = Path("/Users/zhangyi/Documents/code/macd股票版")
sys.path.insert(0, str(SCANNER_ROOT))
from macd_time_signal_scanner import ScanConfig, MACDTimeSignalEngine  # noqa: E402


DB_PATH = SCANNER_ROOT / "data" / "market_data.sqlite"
HS300_PATH = SCANNER_ROOT / "data" / "hs300_current.csv"
OUTPUT_DIR = SCANNER_ROOT / "outputs" / "ibuy2_vs_family2"


# ==============================
# Data loader
# ==============================


def load_hs300_symbols() -> list[str]:
    df = pd.read_csv(HS300_PATH, dtype={"symbol": str})
    return [s.zfill(6) for s in df["symbol"].tolist()]


def load_symbol_bars(symbol: str, level: str = "15m") -> pd.DataFrame:
    """Load one symbol's OHLCV from sqlite. Returns df indexed by datetime."""
    con = sqlite3.connect(str(DB_PATH))
    try:
        df = pd.read_sql_query(
            "SELECT date, open, high, low, close, volume FROM kline "
            "WHERE symbol=? AND level=? AND adjust='qfq' "
            "ORDER BY date",
            con,
            params=(symbol, level),
        )
    finally:
        con.close()
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date").astype(float)
    return df


def resample_bars(df_15m: pd.DataFrame, rule: str) -> pd.DataFrame:
    """Resample 15m bars to coarser timeframe. rule: '60min' or '240min'."""
    if df_15m.empty:
        return df_15m
    agg = df_15m.resample(rule, label="right", closed="right").agg(
        {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
    ).dropna()
    return agg


# ==============================
# Signal adapter: IBUY2
# ==============================


def generate_ibuy2_signals(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    """
    Return signals DataFrame: signal (B1/S1), exec_pos (int).
    Entry:  IBUY2 only (the new independent second buy)
    Exit :  全家族 SELL 信号 (SELL1 / SELL2 / SELL3 / ISELL2)
            — 实盘中任何结构性卖点都应该触发退出, 否则 IBUY2 出场信号过稀无法形成有效交易对.
    """
    cfg = ScanConfig(latest_only=False, recent_bars=9999)
    engine = MACDTimeSignalEngine(cfg)

    inp = df.reset_index().rename(columns={"date": "date"})
    if "date" not in inp.columns:
        inp = inp.rename(columns={inp.columns[0]: "date"})

    result = engine.scan_dataframe(inp, symbol=symbol)
    if result.empty:
        return pd.DataFrame(columns=["signal", "exec_pos"])

    buy_types = {"IBUY2"}
    sell_types = {"SELL1", "SELL2", "SELL3", "ISELL2"}
    result = result[result["signal"].isin(buy_types | sell_types)].copy()
    if result.empty:
        return pd.DataFrame(columns=["signal", "exec_pos"])

    date_to_idx = {d: i for i, d in enumerate(df.index)}
    result["exec_pos"] = result["date"].map(date_to_idx)
    result = result.dropna(subset=["exec_pos"]).copy()
    result["exec_pos"] = result["exec_pos"].astype(int)

    result["signal"] = result["signal"].apply(lambda s: "B1" if s in buy_types else "S1")
    return result[["signal", "exec_pos"]].sort_values("exec_pos").reset_index(drop=True)


# ==============================
# Signal adapter: family2 二买 (ported from macd第二版)
# ==============================

PIVOT_K = 3


@dataclass
class Pivot:
    idx: int
    kind: str  # 'low' or 'high'
    value: float


def _compute_macd(close: pd.Series) -> tuple[pd.Series, pd.Series, pd.Series]:
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    dif = ema12 - ema26
    dea = dif.ewm(span=9, adjust=False).mean()
    histogram = dif - dea
    return dif, dea, histogram


def _find_hist_pivots(hist: np.ndarray, k: int = PIVOT_K) -> list[Pivot]:
    pivots: list[Pivot] = []
    n = len(hist)
    for i in range(k, n - k):
        window = hist[i - k : i + k + 1]
        center = hist[i]
        if np.isnan(center) or np.isnan(window).any():
            continue
        if center == window.min() and np.count_nonzero(window == center) == 1:
            pivots.append(Pivot(i, "low", float(center)))
        elif center == window.max() and np.count_nonzero(window == center) == 1:
            pivots.append(Pivot(i, "high", float(center)))
    pivots.sort(key=lambda p: p.idx)
    return pivots


def _build_failure_swing_signals(
    hist: np.ndarray,
    dif: np.ndarray,
    dea: np.ndarray,
    require_macd_line: bool = False,
) -> list[tuple[int, str]]:
    """
    Return [(bar_idx, 'BUY'|'SELL'), ...] for failure swing L1/H1/L2 pattern.
    Ported from macd第二版/research/research_failure_swing_structure_vectorbt.py
    """
    pivots = _find_hist_pivots(hist, PIVOT_K)
    signals: list[tuple[int, str]] = []
    n = len(hist)

    for i in range(len(pivots) - 2):
        p1, p2, p3 = pivots[i], pivots[i + 1], pivots[i + 2]

        # BUY pattern: L1(<0) -> H1(>0) -> L2(>L1)
        # Then wait for histogram to cross above H1.value
        if (
            p1.kind == "low"
            and p2.kind == "high"
            and p3.kind == "low"
            and p1.value < 0
            and p3.value > p1.value
        ):
            search_end = n - 2
            for j in range(i + 3, len(pivots)):
                if pivots[j].kind == "low":
                    search_end = min(search_end, pivots[j].idx - 1)
                    break
            for t in range(p3.idx + 1, search_end + 1):
                if hist[t - 1] <= p2.value < hist[t]:
                    if require_macd_line and not (dif[t] > dea[t]):
                        continue
                    signal_idx = min(t + 1, n - 1)
                    signals.append((signal_idx, "BUY"))
                    break

        # SELL pattern: H1(>0) -> L1(<0) -> H2(<H1)
        if (
            p1.kind == "high"
            and p2.kind == "low"
            and p3.kind == "high"
            and p1.value > 0
            and p3.value < p1.value
        ):
            search_end = n - 2
            for j in range(i + 3, len(pivots)):
                if pivots[j].kind == "high":
                    search_end = min(search_end, pivots[j].idx - 1)
                    break
            for t in range(p3.idx + 1, search_end + 1):
                if hist[t - 1] >= p2.value > hist[t]:
                    if require_macd_line and not (dif[t] < dea[t]):
                        continue
                    signal_idx = min(t + 1, n - 1)
                    signals.append((signal_idx, "SELL"))
                    break

    # Dedupe by idx (keep first)
    seen: set[int] = set()
    unique: list[tuple[int, str]] = []
    for idx, side in sorted(signals):
        if idx in seen:
            continue
        unique.append((idx, side))
        seen.add(idx)
    return unique


def generate_family2_signals(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    """Return signals DataFrame: signal (B1/S1), exec_pos (int)."""
    if df.empty or len(df) < 60:
        return pd.DataFrame(columns=["signal", "exec_pos"])

    dif, dea, hist = _compute_macd(df["close"])
    sigs = _build_failure_swing_signals(
        hist.to_numpy(),
        dif.to_numpy(),
        dea.to_numpy(),
        require_macd_line=False,
    )
    if not sigs:
        return pd.DataFrame(columns=["signal", "exec_pos"])

    rows = [{"signal": "B1" if side == "BUY" else "S1", "exec_pos": idx} for idx, side in sigs]
    return pd.DataFrame(rows).sort_values("exec_pos").reset_index(drop=True)


# ==============================
# Per-symbol backtest
# ==============================


def backtest_symbol(
    df: pd.DataFrame,
    signals: pd.DataFrame,
    initial_cash: float = 100_000.0,
) -> dict:
    """Run reverse-signal-exit backtest on a single symbol."""
    if df.empty or signals.empty:
        return {"trade_count": 0, "win_rate_pct": 0.0, "total_return_pct": 0.0,
                "max_drawdown_pct": 0.0, "signal_count": len(signals)}

    # The SDK needs raw_df with a 'time' column and numeric OHLC
    raw_df = df.reset_index().rename(columns={"date": "time"}).copy()
    raw_df["time"] = raw_df["time"].astype(str)
    raw_df = raw_df[["time", "open", "high", "low", "close"]].copy()

    try:
        trades_df, equity_df, summary = run_signal_backtest(
            raw_df,
            signals,
            initial_cash=initial_cash,
            position_pct=100.0,
            leverage=1.0,
            fee_rate_pct=0.025,  # A-share: ~万2.5 commission + stamp tax (roughly)
            take_profit_pct=0.0,
            stop_loss_pct=0.0,
            side_mode="long_only",  # A-share no shorting
        )
    except Exception as exc:
        return {"trade_count": 0, "win_rate_pct": 0.0, "total_return_pct": 0.0,
                "max_drawdown_pct": 0.0, "signal_count": len(signals), "error": str(exc)}

    return {
        "trade_count": summary["trade_count"],
        "win_rate_pct": summary["win_rate_pct"],
        "total_return_pct": summary["total_return_pct"],
        "max_drawdown_pct": summary["max_drawdown_pct"],
        "signal_count": summary["signal_count"],
        "trades": trades_df,
    }


# ==============================
# Comparison runner
# ==============================


TIMEFRAMES = {
    "15m": None,      # Native
    "60m": "60min",   # Resample from 15m
    "4h": "240min",   # Resample from 15m
}

STRATEGIES = {
    "IBUY2": generate_ibuy2_signals,
    "family2": generate_family2_signals,
}


def run_comparison(
    symbols: list[str],
    initial_cash_per_stock: float = 100_000.0,
    limit_symbols: int | None = None,
    verbose: bool = True,
) -> dict:
    """Run full comparison. Returns dict of aggregated results."""
    if limit_symbols:
        symbols = symbols[:limit_symbols]

    # results[strategy][timeframe] = list of per-stock trade records
    aggregate: dict[str, dict[str, list[dict]]] = {
        strategy: {tf: [] for tf in TIMEFRAMES} for strategy in STRATEGIES
    }
    per_symbol: dict[str, dict] = {}

    loaded = 0
    for idx, sym in enumerate(symbols):
        try:
            df15 = load_symbol_bars(sym, level="15m")
        except Exception as exc:
            if verbose:
                print(f"[{idx+1}/{len(symbols)}] {sym}: load error {exc}")
            continue
        if df15.empty or len(df15) < 100:
            continue
        loaded += 1

        # Prepare dataframes for each timeframe
        dfs = {"15m": df15}
        dfs["60m"] = resample_bars(df15, "60min")
        dfs["4h"] = resample_bars(df15, "240min")

        for strategy, gen_fn in STRATEGIES.items():
            for tf, df in dfs.items():
                if df.empty or len(df) < 80:
                    continue
                signals = gen_fn(df, sym)
                if signals.empty:
                    continue
                result = backtest_symbol(df, signals, initial_cash_per_stock)
                if result["trade_count"] > 0:
                    aggregate[strategy][tf].append({
                        "symbol": sym,
                        **{k: v for k, v in result.items() if k != "trades"},
                    })

        per_symbol[sym] = {"bars_15m": len(df15)}
        if verbose and (idx + 1) % 20 == 0:
            print(f"  ...processed {idx+1}/{len(symbols)} ({loaded} loaded)")

    if verbose:
        print(f"Loaded {loaded}/{len(symbols)} symbols with sufficient data")

    return {"aggregate": aggregate, "per_symbol": per_symbol, "loaded_count": loaded}


# ==============================
# Report
# ==============================


def summarize_group(records: list[dict]) -> dict:
    if not records:
        return {
            "n_stocks": 0,
            "total_trades": 0,
            "avg_win_rate": 0.0,
            "avg_return": 0.0,
            "median_return": 0.0,
            "positive_stock_pct": 0.0,
            "avg_max_dd": 0.0,
        }
    df = pd.DataFrame(records)
    return {
        "n_stocks": len(df),
        "total_trades": int(df["trade_count"].sum()),
        "avg_win_rate": float(df["win_rate_pct"].mean()),
        "avg_return": float(df["total_return_pct"].mean()),
        "median_return": float(df["total_return_pct"].median()),
        "positive_stock_pct": float((df["total_return_pct"] > 0).mean() * 100),
        "avg_max_dd": float(df["max_drawdown_pct"].mean()),
    }


def build_markdown_report(results: dict) -> str:
    agg = results["aggregate"]
    lines = [
        "# IBUY2 vs family2 二买 对比回测",
        "",
        f"- 股票池: HS300 (加载 {results['loaded_count']} 只)",
        "- 周期: 15m / 60m / 4h (60m 和 4h 从 15m 重采样)",
        "- 出场规则:",
        "  - IBUY2: 入场=IBUY2, 出场=任意 SELL 族信号 (SELL1/SELL2/SELL3/ISELL2)",
        "  - family2: 入场=family2 失败摆动 BUY, 出场=family2 失败摆动 SELL",
        "- 单股初始资金: 100,000 元, 手续费: 0.025% (含印花税等)",
        "- 禁止做空 (A 股规则)",
        "",
        "| 策略 | 周期 | 有信号股票数 | 总交易数 | 平均胜率 | 平均收益 | 收益中位数 | 盈利股票占比 | 平均最大回撤 |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]

    for strategy in STRATEGIES:
        for tf in TIMEFRAMES:
            s = summarize_group(agg[strategy][tf])
            lines.append(
                f"| {strategy} | {tf} | {s['n_stocks']} | {s['total_trades']} | "
                f"{s['avg_win_rate']:.2f}% | {s['avg_return']:+.2f}% | "
                f"{s['median_return']:+.2f}% | {s['positive_stock_pct']:.1f}% | "
                f"{s['avg_max_dd']:.2f}% |"
            )

    return "\n".join(lines) + "\n"


# ==============================
# Main
# ==============================


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None, help="Limit number of symbols")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    symbols = load_hs300_symbols()
    print(f"HS300 symbols: {len(symbols)}")
    if args.limit:
        print(f"Limiting to first {args.limit} symbols")

    results = run_comparison(symbols, limit_symbols=args.limit, verbose=not args.quiet)

    report = build_markdown_report(results)
    print()
    print(report)

    out_md = OUTPUT_DIR / "ibuy2_vs_family2_report.md"
    out_md.write_text(report, encoding="utf-8")
    print(f"\nReport saved to {out_md}")

    # Dump per-stock details for debugging
    rows = []
    for strategy, by_tf in results["aggregate"].items():
        for tf, recs in by_tf.items():
            for r in recs:
                rows.append({"strategy": strategy, "timeframe": tf, **r})
    if rows:
        pd.DataFrame(rows).to_csv(OUTPUT_DIR / "per_stock_details.csv", index=False)
        print(f"Per-stock details saved to {OUTPUT_DIR / 'per_stock_details.csv'}")


if __name__ == "__main__":
    main()
