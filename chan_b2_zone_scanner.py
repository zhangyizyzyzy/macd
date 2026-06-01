#!/usr/bin/env python3
"""
chan_b2_zone_scanner.py — 缠论二买状态区识别器

核心思想:
  二买不是"某一天的金叉", 而是一段"冲破平台后健康整理"的状态.
  每根 bar 都打分, 输出连续的"二买区间"而不是离散的触发信号.

状态定义:
  (bar t 处于二买区) iff
    1. 向前 80 bars 内存在一次 "平台突破冲锋":
       - 冲锋起点: 一个 pivot low (低点)
       - 冲锋期间: price 穿破了更早之前的 pivot high (突破平台)
       - 冲锋幅度: ≥ 3×ATR
       - 冲锋时长: ≥ 5 bars
       - 冲锋期间: DIF 或 DEA 至少触及过 0 轴以上 (动能确认)

    2. 从冲锋顶点 (charge_peak) 到当前 t, 整理质量健康:
       - 整理时长: 3-50 bars
       - 最低点 ≥ 冲锋起点 (Low_Anchor) (严格不破)
       - 最低点 ≥ 冲锋顶点的 50% 回撤位 (不能深套)
       - 实际回撤比 ∈ [0.15, 0.80]

    3. MACD 已回到 0 轴附近:
       - DEA / ATR ≤ 1.0 (或 hist 已经开始走绿)

输出:
  scan_zones(df, symbol) -> DataFrame of (date, in_zone, zone_id, score, features)

  每个连续的 "in_zone=True" 段是一个"二买状态区", 同一个 zone_id.
"""
from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Optional

import numpy as np
import pandas as pd


# ==============================
# Config
# ==============================


@dataclass(slots=True)
class ZoneConfig:
    # MACD
    macd_fast: int = 12
    macd_slow: int = 26
    macd_signal: int = 9
    atr_len: int = 14

    # Pivot
    pivot_k: int = 3                          # pivot 半径

    # 冲锋要求
    zone_lookback: int = 120                  # 每个 bar 向前找冲锋的最大范围 (扩大, 捕捉更早的大顶)
    min_charge_bars: int = 5                  # 冲锋最小持续 bars
    max_charge_bars: int = 60                 # 冲锋最大持续 bars
    min_charge_atr: float = 3.0               # 冲锋最小幅度 (× ATR)
    require_breakout: bool = True             # 冲锋是否必须突破前期平台
    breakout_lookback: int = 60               # 找前期平台 pivot high 的回溯范围

    # 整理区要求
    min_consolidation_bars: int = 3           # 整理最小 bars
    max_consolidation_bars: int = 50          # 整理最大 bars
    min_retrace: float = 0.15                 # 最小回撤比例
    max_retrace: float = 0.618                # 最大回撤比例 (斐波那契黄金位)
    max_low_break_ratio: float = 0.50         # 最低点不能跌破冲锋顶点的 50% 回撤

    # MACD 约束
    max_dea_atr_ratio: float = 1.0            # DEA / ATR 上界 (不能还在高位, 必须回到 0 轴附近)
    min_dea_atr_ratio: float = -0.5           # DEA / ATR 下界 (不能深水下, 防止已转跌)
    require_dif_above_dea_close: bool = False # 是否要求 DIF 已靠近或超过 DEA (动能转好)

    # "真横盘" 过滤
    consolidation_range_lookback: int = 10    # 检查最近 N bars 的价格收敛性
    max_recent_range_atr: float = 4.0         # 最近 N bars 的 (high-low) / ATR ≤ 此值

    # 当前价位要求
    min_close_vs_peak: float = 0.60           # 当前 close ≥ peak × 0.60 (防止深度下跌被误判)

    # 评分权重
    score_base: float = 50.0


# ==============================
# Indicators
# ==============================


def compute_macd_atr(df: pd.DataFrame, cfg: ZoneConfig) -> pd.DataFrame:
    out = df.copy()
    ema_fast = out["close"].ewm(span=cfg.macd_fast, adjust=False).mean()
    ema_slow = out["close"].ewm(span=cfg.macd_slow, adjust=False).mean()
    out["dif"] = ema_fast - ema_slow
    out["dea"] = out["dif"].ewm(span=cfg.macd_signal, adjust=False).mean()
    out["hist"] = out["dif"] - out["dea"]

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
# Pivot detection
# ==============================


def find_pivots(high: np.ndarray, low: np.ndarray, k: int) -> tuple[list[int], list[int]]:
    """Return (pivot_low_indices, pivot_high_indices)."""
    n = len(high)
    lows, highs = [], []
    for i in range(k, n - k):
        center_lo = low[i]
        center_hi = high[i]
        is_lo = True
        is_hi = True
        has_higher_l = False
        has_lower_h = False
        for j in range(1, k + 1):
            if low[i - j] < center_lo or low[i + j] < center_lo:
                is_lo = False
            if low[i - j] > center_lo or low[i + j] > center_lo:
                has_higher_l = True
            if high[i - j] > center_hi or high[i + j] > center_hi:
                is_hi = False
            if high[i - j] < center_hi or high[i + j] < center_hi:
                has_lower_h = True
        if is_lo and has_higher_l:
            lows.append(i)
        if is_hi and has_lower_h:
            highs.append(i)
    return lows, highs


# ==============================
# Charge detection (around a peak)
# ==============================


@dataclass(slots=True)
class Charge:
    start_idx: int       # 冲锋起点 (pivot low)
    start_price: float
    peak_idx: int        # 冲锋顶点
    peak_price: float
    bars: int
    rise_atr: float      # 幅度 (× ATR)
    broke_platform: bool # 是否突破前期平台
    platform_high: float # 被突破的平台高点 (若有)
    touched_zero_line: bool  # DIF/DEA 是否触及过 0 轴以上


def find_recent_charge(
    t: int,
    high: np.ndarray,
    low: np.ndarray,
    dif: np.ndarray,
    dea: np.ndarray,
    atr: np.ndarray,
    pivot_lows: list[int],
    pivot_highs: list[int],
    cfg: ZoneConfig,
) -> Optional[Charge]:
    """从 t 向前找"主冲锋" — 窗口内最高的 pivot high 作为 peak.

    核心修复: 不再是"最近的 pivot high", 而是"窗口内绝对最高的 pivot high".
    这样一个已经跌破的大顶会被强制当成 peak, 从而触发深度回撤过滤.
    """
    window_start = max(cfg.pivot_k, t - cfg.zone_lookback)
    candidate_peaks = [p for p in pivot_highs if window_start <= p <= t]
    if not candidate_peaks:
        return None

    # --- KEY FIX 1: pick the HIGHEST pivot_high in window, not the most recent ---
    # 这样如果 120 bars 内出现过 53.88 的大顶, 它就是 charge_peak, 不会被更小的
    # 38.83 局部高点掩盖.
    peak_idx = max(candidate_peaks, key=lambda i: high[i])
    peak_price = high[peak_idx]

    # --- KEY FIX 2: ensure there's no later high that invalidates this peak ---
    # 如果 peak 之后又创了 0.5×ATR 以上的新高, 说明这个 peak 不是真正的顶点
    # 而是冲锋途中的小回调, 应该排除
    if peak_idx < t and atr[peak_idx] > 0:
        subsequent_max = float(np.max(high[peak_idx + 1 : t + 1])) if peak_idx + 1 <= t else peak_price
        if subsequent_max > peak_price + 0.5 * atr[peak_idx]:
            return None

    # Find the LOWEST pivot low before this peak within window
    preceding_lows = [pl for pl in pivot_lows if window_start <= pl < peak_idx]
    if not preceding_lows:
        return None
    start_idx = min(preceding_lows, key=lambda i: low[i])
    start_price = low[start_idx]

    bars = peak_idx - start_idx
    if bars < cfg.min_charge_bars or bars > cfg.max_charge_bars:
        return None

    if atr[peak_idx] <= 0:
        return None
    rise_atr = (peak_price - start_price) / atr[peak_idx]
    if rise_atr < cfg.min_charge_atr:
        return None

    # Breakout check: is there a pivot_high before start_idx whose high < peak_price?
    broke_platform = False
    platform_high = np.nan
    if cfg.require_breakout:
        earlier_start = max(0, start_idx - cfg.breakout_lookback)
        earlier_highs = [ph for ph in pivot_highs if earlier_start <= ph < start_idx]
        if earlier_highs:
            best_prev = max(earlier_highs, key=lambda i: high[i])
            platform_high = high[best_prev]
            if peak_price > platform_high:
                broke_platform = True
        if not broke_platform:
            return None

    # MACD 0-axis touch during charge
    touched = False
    for k in range(start_idx, peak_idx + 1):
        if dif[k] > 0 or dea[k] > 0:
            touched = True
            break
    if not touched:
        return None

    return Charge(
        start_idx=start_idx,
        start_price=float(start_price),
        peak_idx=peak_idx,
        peak_price=float(peak_price),
        bars=bars,
        rise_atr=float(rise_atr),
        broke_platform=broke_platform,
        platform_high=float(platform_high),
        touched_zero_line=True,
    )


# ==============================
# Zone state check
# ==============================


@dataclass(slots=True)
class ZoneState:
    in_zone: bool
    score: float
    charge_start_idx: int
    charge_start_price: float
    charge_peak_idx: int
    charge_peak_price: float
    platform_high: float
    bars_since_peak: int
    lowest_since_peak: float
    retrace: float
    dea_atr_ratio: float
    hist_val: float
    broke_platform: bool
    reason_fail: str = ""  # 若不满足, 说明哪个条件挂了


def _empty_state(reason: str) -> ZoneState:
    return ZoneState(
        in_zone=False,
        score=0.0,
        charge_start_idx=-1,
        charge_start_price=0.0,
        charge_peak_idx=-1,
        charge_peak_price=0.0,
        platform_high=0.0,
        bars_since_peak=0,
        lowest_since_peak=0.0,
        retrace=0.0,
        dea_atr_ratio=0.0,
        hist_val=0.0,
        broke_platform=False,
        reason_fail=reason,
    )


def check_zone_state(
    t: int,
    high: np.ndarray,
    low: np.ndarray,
    close: np.ndarray,
    dif: np.ndarray,
    dea: np.ndarray,
    hist: np.ndarray,
    atr: np.ndarray,
    pivot_lows: list[int],
    pivot_highs: list[int],
    cfg: ZoneConfig,
) -> ZoneState:
    """判断 bar t 是否处于二买状态区."""
    if t < cfg.zone_lookback or atr[t] <= 0:
        return _empty_state("not_enough_history")

    charge = find_recent_charge(t, high, low, dif, dea, atr, pivot_lows, pivot_highs, cfg)
    if charge is None:
        return _empty_state("no_charge")

    # Current bar must be AFTER the peak
    bars_since_peak = t - charge.peak_idx
    if bars_since_peak < cfg.min_consolidation_bars:
        return _empty_state("too_soon_after_peak")
    if bars_since_peak > cfg.max_consolidation_bars:
        return _empty_state("consolidation_too_long")

    # Low anchor check
    lowest_since_peak = float(np.min(low[charge.peak_idx : t + 1]))
    if lowest_since_peak < charge.start_price:
        return _empty_state("broke_low_anchor")

    # Retrace ratio
    rise_range = charge.peak_price - charge.start_price
    if rise_range <= 0:
        return _empty_state("degenerate_rise")
    retrace = (charge.peak_price - lowest_since_peak) / rise_range
    if retrace < cfg.min_retrace:
        return _empty_state("retrace_too_shallow")
    if retrace > cfg.max_retrace:
        return _empty_state("retrace_too_deep")

    # Deep break check (max_low_break_ratio)
    low_break_threshold = charge.peak_price - cfg.max_low_break_ratio * rise_range
    if lowest_since_peak < low_break_threshold:
        return _empty_state("broke_50pct")

    # MACD back to 0 axis (BOTH upper and lower bound)
    dea_atr_ratio = float(dea[t] / atr[t])
    if dea_atr_ratio > cfg.max_dea_atr_ratio:
        return _empty_state("dea_still_high")
    if dea_atr_ratio < cfg.min_dea_atr_ratio:
        return _empty_state("dea_too_deep")  # KEY FIX 3: 防止"下水太低"

    # Current close must not be too far below peak (防止深度下跌被误判)
    close_vs_peak = close[t] / charge.peak_price
    if close_vs_peak < cfg.min_close_vs_peak:
        return _empty_state("close_too_far_below_peak")

    # Recent price range must be converging (真的在横盘)
    lookback = min(cfg.consolidation_range_lookback, bars_since_peak + 1)
    if lookback >= 3:
        recent_slice_start = max(charge.peak_idx, t - lookback + 1)
        recent_high = float(np.max(high[recent_slice_start : t + 1]))
        recent_low = float(np.min(low[recent_slice_start : t + 1]))
        recent_range_atr = (recent_high - recent_low) / atr[t]
        if recent_range_atr > cfg.max_recent_range_atr:
            return _empty_state("range_too_wide")  # KEY FIX 4: 不是横盘, 是趋势性下跌

    # -------- All conditions passed: compute health score --------
    score = cfg.score_base

    # 回撤在 0.382-0.618 黄金区间给高分
    if 0.382 <= retrace <= 0.618:
        score += 10.0
    elif 0.30 <= retrace < 0.382 or 0.618 < retrace <= 0.70:
        score += 6.0
    else:
        score += 2.0

    # 冲锋幅度越大分越高 (up to 10 pts)
    score += min(10.0, charge.rise_atr * 2)

    # 突破平台加分
    if charge.broke_platform:
        score += 8.0

    # DEA 越接近 0 越好 (up to 8 pts)
    score += max(0.0, 8.0 * (1.0 - dea_atr_ratio))

    # hist 已经开始转正更好 (up to 5 pts)
    if hist[t] > 0:
        score += 5.0
    elif hist[t] > hist[t - 1]:
        score += 2.0

    # 整理时间适中加分 (10-25 bars 最好)
    if 10 <= bars_since_peak <= 25:
        score += 5.0
    elif 25 < bars_since_peak <= 40:
        score += 2.0

    score = round(min(100.0, score), 2)

    return ZoneState(
        in_zone=True,
        score=score,
        charge_start_idx=charge.start_idx,
        charge_start_price=charge.start_price,
        charge_peak_idx=charge.peak_idx,
        charge_peak_price=charge.peak_price,
        platform_high=charge.platform_high,
        bars_since_peak=bars_since_peak,
        lowest_since_peak=lowest_since_peak,
        retrace=round(retrace, 4),
        dea_atr_ratio=round(dea_atr_ratio, 4),
        hist_val=round(float(hist[t]), 6),
        broke_platform=charge.broke_platform,
    )


# ==============================
# Scanner entry
# ==============================


def scan_zones(df: pd.DataFrame, cfg: Optional[ZoneConfig] = None) -> pd.DataFrame:
    """Scan a single OHLCV dataframe and return per-bar zone state.

    Input df must have columns: date, open, high, low, close, volume
    Returns DataFrame with same length + columns:
      in_zone, zone_id, zone_score, bars_since_peak, retrace, dea_atr_ratio, ...
    """
    cfg = cfg or ZoneConfig()
    if df.empty or len(df) < 100:
        return pd.DataFrame()

    data = compute_macd_atr(df, cfg).dropna(
        subset=["open", "high", "low", "close", "dif", "dea", "hist", "atr"]
    ).reset_index(drop=True)
    if len(data) < 100:
        return pd.DataFrame()

    high = data["high"].to_numpy(float)
    low = data["low"].to_numpy(float)
    close = data["close"].to_numpy(float)
    dif = data["dif"].to_numpy(float)
    dea = data["dea"].to_numpy(float)
    hist = data["hist"].to_numpy(float)
    atr = data["atr"].to_numpy(float)

    pivot_lows, pivot_highs = find_pivots(high, low, cfg.pivot_k)

    n = len(data)
    in_zone = np.zeros(n, dtype=bool)
    scores = np.zeros(n, dtype=float)
    bars_since = np.zeros(n, dtype=int)
    retrace_arr = np.zeros(n, dtype=float)
    dea_ratio_arr = np.zeros(n, dtype=float)
    charge_start_arr = np.zeros(n, dtype=int)
    charge_peak_arr = np.zeros(n, dtype=int)
    charge_start_price = np.zeros(n, dtype=float)
    charge_peak_price = np.zeros(n, dtype=float)
    platform_high_arr = np.zeros(n, dtype=float)
    broke_platform = np.zeros(n, dtype=bool)

    for t in range(n):
        st = check_zone_state(t, high, low, close, dif, dea, hist, atr, pivot_lows, pivot_highs, cfg)
        if st.in_zone:
            in_zone[t] = True
            scores[t] = st.score
            bars_since[t] = st.bars_since_peak
            retrace_arr[t] = st.retrace
            dea_ratio_arr[t] = st.dea_atr_ratio
            charge_start_arr[t] = st.charge_start_idx
            charge_peak_arr[t] = st.charge_peak_idx
            charge_start_price[t] = st.charge_start_price
            charge_peak_price[t] = st.charge_peak_price
            platform_high_arr[t] = st.platform_high
            broke_platform[t] = st.broke_platform

    # Assign zone_id: consecutive in_zone bars → same id
    zone_id = np.zeros(n, dtype=int)
    current_id = 0
    for i in range(n):
        if in_zone[i]:
            if i == 0 or not in_zone[i - 1]:
                current_id += 1
            zone_id[i] = current_id
        else:
            zone_id[i] = 0

    out = data.copy()
    out["in_zone"] = in_zone
    out["zone_id"] = zone_id
    out["zone_score"] = scores
    out["bars_since_peak"] = bars_since
    out["retrace"] = retrace_arr
    out["dea_atr_ratio"] = dea_ratio_arr
    out["charge_start_idx"] = charge_start_arr
    out["charge_peak_idx"] = charge_peak_arr
    out["charge_start_price"] = charge_start_price
    out["charge_peak_price"] = charge_peak_price
    out["platform_high"] = platform_high_arr
    out["broke_platform"] = broke_platform
    return out


def extract_zone_ranges(scan_result: pd.DataFrame) -> pd.DataFrame:
    """Collapse per-bar scan into per-zone ranges.

    One row per contiguous zone with:
      zone_id, start_date, end_date, length_bars, max_score, avg_score,
      charge_peak_price, charge_start_price, broke_platform
    """
    if scan_result.empty:
        return pd.DataFrame()
    in_zone = scan_result[scan_result["in_zone"]]
    if in_zone.empty:
        return pd.DataFrame()

    groups = []
    for zid, grp in in_zone.groupby("zone_id"):
        groups.append({
            "zone_id": int(zid),
            "start_date": grp["date"].iloc[0],
            "end_date": grp["date"].iloc[-1],
            "length_bars": len(grp),
            "max_score": grp["zone_score"].max(),
            "avg_score": round(grp["zone_score"].mean(), 2),
            "charge_start_price": grp["charge_start_price"].iloc[0],
            "charge_peak_price": grp["charge_peak_price"].iloc[0],
            "platform_high": grp["platform_high"].iloc[0],
            "broke_platform": bool(grp["broke_platform"].iloc[0]),
            "max_retrace": grp["retrace"].max(),
            "min_retrace": grp["retrace"].min(),
        })
    return pd.DataFrame(groups)


# ==============================
# CLI — daily HS300 scan
# ==============================


def main() -> None:
    import argparse
    import sqlite3
    from pathlib import Path

    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=str, default="/Users/zhangyi/Documents/code/macd股票版/data/market_data.sqlite")
    parser.add_argument("--universe", type=str, default="/Users/zhangyi/Documents/code/macd股票版/data/hs300_current.csv")
    parser.add_argument("--level", type=str, default="daily", choices=["daily", "15m", "60m", "4h"])
    parser.add_argument("--output-dir", type=str, default="/Users/zhangyi/Documents/code/macd股票版/outputs/chan_b2_zone")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--min-score", type=float, default=55.0, help="Filter current candidates by min score")
    parser.add_argument("--symbol", type=str, default=None, help="Test single symbol verbose")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    universe = pd.read_csv(args.universe, dtype={"symbol": str})
    symbols = [str(s).zfill(6) for s in universe["symbol"]]
    if args.symbol:
        symbols = [args.symbol.zfill(6)]
    elif args.limit:
        symbols = symbols[: args.limit]

    # Map level to sqlite query
    if args.level == "daily":
        sql_level = "daily"
        resample_rule = None
    elif args.level == "15m":
        sql_level = "15m"
        resample_rule = None
    elif args.level == "60m":
        sql_level = "15m"
        resample_rule = "60min"
    elif args.level == "4h":
        sql_level = "15m"
        resample_rule = "240min"
    else:
        raise ValueError(args.level)

    con = sqlite3.connect(args.db)
    cfg = ZoneConfig()

    current_candidates = []  # stocks currently in a zone
    all_zones_flat = []  # all historical zones (for research)

    for idx, sym in enumerate(symbols):
        df = pd.read_sql_query(
            "SELECT date, open, high, low, close, volume FROM kline "
            "WHERE symbol=? AND level=? AND adjust='qfq' ORDER BY date",
            con, params=(sym, sql_level),
        )
        if df.empty or len(df) < 150:
            continue
        df["date"] = pd.to_datetime(df["date"])

        if resample_rule:
            df = df.set_index("date").resample(
                resample_rule, label="right", closed="right"
            ).agg({
                "open": "first", "high": "max", "low": "min",
                "close": "last", "volume": "sum"
            }).dropna().reset_index()

        scan = scan_zones(df, cfg)
        if scan.empty:
            continue

        # Last bar status
        last = scan.iloc[-1]
        if last["in_zone"] and last["zone_score"] >= args.min_score:
            zone_start = scan[scan["zone_id"] == last["zone_id"]]["date"].iloc[0]
            current_candidates.append({
                "symbol": sym,
                "last_date": last["date"],
                "score": last["zone_score"],
                "bars_in_zone": int(last["bars_since_peak"]),
                "zone_start_date": zone_start,
                "current_close": round(float(last["close"]), 4),
                "charge_start_price": round(float(last["charge_start_price"]), 4),
                "charge_peak_price": round(float(last["charge_peak_price"]), 4),
                "retrace": last["retrace"],
                "dea_atr_ratio": last["dea_atr_ratio"],
                "broke_platform": bool(last["broke_platform"]),
                "platform_high": round(float(last["platform_high"]), 4),
            })

        # Collect all zones for research
        zones = extract_zone_ranges(scan)
        if not zones.empty:
            zones["symbol"] = sym
            all_zones_flat.append(zones)

        if args.symbol and not zones.empty:
            print(f"\n== {sym} zones ({args.level}) ==")
            print(zones.to_string(index=False))

        if (idx + 1) % 30 == 0:
            print(f"  scanned {idx+1}/{len(symbols)} (current candidates: {len(current_candidates)})")

    con.close()

    # ----- Outputs -----
    today = pd.Timestamp.now().strftime("%Y%m%d")
    level_tag = args.level.replace("m", "min") if args.level.endswith("m") else args.level

    if current_candidates:
        df_curr = pd.DataFrame(current_candidates).sort_values("score", ascending=False)
        curr_path = output_dir / f"current_candidates_{level_tag}_{today}.csv"
        df_curr.to_csv(curr_path, index=False)
        print()
        print("=" * 70)
        print(f"CURRENT CANDIDATES ({args.level}) — {len(df_curr)} stocks in B2 zone")
        print("=" * 70)
        print(df_curr.head(30).to_string(index=False))
        print()
        print(f"Saved: {curr_path}")
    else:
        print("\nNo current candidates above min_score threshold.")

    if all_zones_flat:
        df_all = pd.concat(all_zones_flat, ignore_index=True)
        all_path = output_dir / f"all_historical_zones_{level_tag}_{today}.csv"
        df_all.to_csv(all_path, index=False)
        print(f"Historical zones: {len(df_all)} rows -> {all_path}")


if __name__ == "__main__":
    main()
