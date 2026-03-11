#!/usr/bin/env python3
from __future__ import annotations

import argparse
import concurrent.futures as futures
import dataclasses
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import numpy as np
import pandas as pd


PROJECT_VERSION = "1.3.0"


# ==============================
# Config
# ==============================


@dataclass(slots=True)
class ScanConfig:
    start_date: str = "20220101"
    end_date: str = "20300101"
    adjust: str = "qfq"
    period: str = "daily"
    workers: int = 4
    min_amount: float = 1.0e8
    min_price: float = 2.0
    exclude_st: bool = True
    exclude_delisting: bool = True
    limit: Optional[int] = None
    recent_bars: int = 5
    latest_only: bool = True

    # MACD / ATR parameters
    macd_fast: int = 12
    macd_slow: int = 26
    macd_signal: int = 9
    atr_len: int = 14

    # price / zero tolerance
    eps_price_atr: float = 0.20
    eps_zero_atr: float = 0.05
    revisit_band_atr: float = 0.50

    # cross-density
    cross_window: int = 20
    max_cross_density_left: float = 0.25
    max_cross_density_1: float = 0.20
    max_cross_density_2: float = 0.20
    max_cross_density_3: float = 0.18

    # left-bottom / left-top
    min_div_votes_left: int = 2
    min_left_lag: int = 1
    max_dwell_left: int = 5
    max_shrink_age_left: int = 4

    # buy1 / sell1
    min_div_votes_1: int = 2
    min_lag_1: int = 1
    max_lag_1: int = 8
    max_conf1: int = 6

    # buy2 / sell2
    pull2_area_ratio: float = 0.45
    min_rise1_bars: int = 2
    max_pull2_bars: int = 8
    max_R2: float = 0.90
    max_revisit_low_bars: int = 3
    max_revisit_high_bars: int = 3

    # buy3 / sell3
    pull3_area_ratio: float = 0.40
    max_pull3_bars: int = 6
    max_B0_bars: int = 1
    max_near0_bars: int = 4


UP = 1
DOWN = -1


# ==============================
# Segment stats
# ==============================


@dataclass(slots=True)
class SegStats:
    dir: int
    start_idx: int
    end_idx: int
    start_close: float
    end_close: float
    low_price: float
    low_idx: int
    high_price: float
    high_idx: int
    bar_min: float
    bar_min_idx: int
    bar_max: float
    bar_max_idx: int
    dif_min: float
    dif_min_idx: int
    dif_max: float
    dif_max_idx: int
    dea_min: float
    dea_max: float
    area_abs: float
    near0_bars: int
    below0_bars: int
    above0_bars: int

    @property
    def bars(self) -> int:
        return self.end_idx - self.start_idx + 1

    def lag_bar_bottom(self) -> int:
        return self.low_idx - self.bar_min_idx

    def lag_dif_bottom(self) -> int:
        return self.low_idx - self.dif_min_idx

    def lag_bar_top(self) -> int:
        return self.high_idx - self.bar_max_idx

    def lag_dif_top(self) -> int:
        return self.high_idx - self.dif_max_idx

    def space_atr(self, atr_ref: float) -> float:
        if atr_ref <= 0:
            return 0.0
        if self.dir == DOWN:
            return (self.start_close - self.low_price) / atr_ref
        return (self.high_price - self.start_close) / atr_ref

    def efficiency(self, atr_ref: float) -> float:
        return self.space_atr(atr_ref) / max(self.bars, 1)


# ==============================
# State contexts
# ==============================


LONG_NONE = 0
LONG_LEFT_BOTTOM = 1
LONG_BUY1 = 2
LONG_PULL2 = 3
LONG_BUY2 = 4
LONG_ABOVE0 = 5
LONG_PULL3 = 6
LONG_BUY3 = 7

SHORT_NONE = 0
SHORT_LEFT_TOP = 1
SHORT_SELL1 = 2
SHORT_PULL2 = 3
SHORT_SELL2 = 4
SHORT_BELOW0 = 5
SHORT_PULL3 = 6
SHORT_SELL3 = 7


@dataclass(slots=True)
class LongCtx:
    state: int = LONG_NONE
    left_bottom_idx: Optional[int] = None
    left_bottom_price: Optional[float] = None
    buy1_idx: Optional[int] = None
    buy1_price: Optional[float] = None
    buy1_low: Optional[float] = None
    buy1_main_down_area: Optional[float] = None
    buy1_main_down_eff: Optional[float] = None
    pull2_start_idx: Optional[int] = None
    rise1_bars: Optional[int] = None
    rise1_high: Optional[float] = None
    buy2_idx: Optional[int] = None
    zero_up_idx: Optional[int] = None
    pull3_start_idx: Optional[int] = None
    buy3_idx: Optional[int] = None


@dataclass(slots=True)
class ShortCtx:
    state: int = SHORT_NONE
    left_top_idx: Optional[int] = None
    left_top_price: Optional[float] = None
    sell1_idx: Optional[int] = None
    sell1_price: Optional[float] = None
    sell1_high: Optional[float] = None
    sell1_main_up_area: Optional[float] = None
    sell1_main_up_eff: Optional[float] = None
    pull2_start_idx: Optional[int] = None
    drop1_bars: Optional[int] = None
    drop1_low: Optional[float] = None
    sell2_idx: Optional[int] = None
    zero_down_idx: Optional[int] = None
    pull3_start_idx: Optional[int] = None
    sell3_idx: Optional[int] = None


# ==============================
# Helpers
# ==============================


def ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()


def compute_indicators(df: pd.DataFrame, cfg: ScanConfig) -> pd.DataFrame:
    out = df.copy()
    out["dif"] = ema(out["close"], cfg.macd_fast) - ema(out["close"], cfg.macd_slow)
    out["dea"] = ema(out["dif"], cfg.macd_signal)
    out["bar"] = out["dif"] - out["dea"]

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


def cross_up(bar: np.ndarray, t: int) -> bool:
    return t > 0 and bar[t - 1] <= 0 and bar[t] > 0


def cross_down(bar: np.ndarray, t: int) -> bool:
    return t > 0 and bar[t - 1] >= 0 and bar[t] < 0


def dual_above_zero(dif: np.ndarray, dea: np.ndarray, t: int) -> bool:
    return dif[t] > 0 and dea[t] > 0


def dual_below_zero(dif: np.ndarray, dea: np.ndarray, t: int) -> bool:
    return dif[t] < 0 and dea[t] < 0


def seg_new(
    dir_: int,
    i: int,
    high_: float,
    low_: float,
    close_: float,
    dif_: float,
    dea_: float,
    bar_: float,
    eps0: float,
) -> SegStats:
    return SegStats(
        dir=dir_,
        start_idx=i,
        end_idx=i,
        start_close=close_,
        end_close=close_,
        low_price=low_,
        low_idx=i,
        high_price=high_,
        high_idx=i,
        bar_min=bar_,
        bar_min_idx=i,
        bar_max=bar_,
        bar_max_idx=i,
        dif_min=dif_,
        dif_min_idx=i,
        dif_max=dif_,
        dif_max_idx=i,
        dea_min=dea_,
        dea_max=dea_,
        area_abs=abs(bar_),
        near0_bars=1 if abs(dif_) <= eps0 and abs(dea_) <= eps0 else 0,
        below0_bars=1 if dif_ < 0 and dea_ < 0 else 0,
        above0_bars=1 if dif_ > 0 and dea_ > 0 else 0,
    )


def seg_update(
    seg: SegStats,
    i: int,
    high_: float,
    low_: float,
    close_: float,
    dif_: float,
    dea_: float,
    bar_: float,
    eps0: float,
) -> SegStats:
    seg.end_idx = i
    seg.end_close = close_

    if low_ < seg.low_price:
        seg.low_price = low_
        seg.low_idx = i
    if high_ > seg.high_price:
        seg.high_price = high_
        seg.high_idx = i
    if bar_ < seg.bar_min:
        seg.bar_min = bar_
        seg.bar_min_idx = i
    if bar_ > seg.bar_max:
        seg.bar_max = bar_
        seg.bar_max_idx = i
    if dif_ < seg.dif_min:
        seg.dif_min = dif_
        seg.dif_min_idx = i
    if dif_ > seg.dif_max:
        seg.dif_max = dif_
        seg.dif_max_idx = i

    seg.dea_min = min(seg.dea_min, dea_)
    seg.dea_max = max(seg.dea_max, dea_)
    seg.area_abs += abs(bar_)

    if abs(dif_) <= eps0 and abs(dea_) <= eps0:
        seg.near0_bars += 1
    if dif_ < 0 and dea_ < 0:
        seg.below0_bars += 1
    if dif_ > 0 and dea_ > 0:
        seg.above0_bars += 1
    return seg


def cross_density(bar: np.ndarray, t: int, window: int) -> float:
    start = max(1, t - window + 1)
    flips = 0
    for k in range(start, t + 1):
        if (bar[k] > 0 and bar[k - 1] <= 0) or (bar[k] < 0 and bar[k - 1] >= 0):
            flips += 1
    return flips / max(window, 1)


def first_shrink_down(bar: np.ndarray, t: int) -> bool:
    if t < 2:
        return False
    return bar[t] < 0 and abs(bar[t]) < abs(bar[t - 1]) and abs(bar[t - 1]) >= abs(bar[t - 2])


def first_shrink_up(bar: np.ndarray, t: int) -> bool:
    if t < 2:
        return False
    return bar[t] > 0 and abs(bar[t]) < abs(bar[t - 1]) and abs(bar[t - 1]) >= abs(bar[t - 2])


def count_bars_near_level(
    price: np.ndarray,
    atr: np.ndarray,
    start_idx: int,
    end_idx: int,
    level: float,
    band_atr: float,
) -> int:
    cnt = 0
    for k in range(start_idx, end_idx + 1):
        if abs(price[k] - level) <= band_atr * atr[k]:
            cnt += 1
    return cnt


def bottom_divergence_votes(
    curr_down: SegStats,
    prev_down: SegStats,
    atr_ref: float,
    cfg: ScanConfig,
) -> tuple[int, dict[str, Any]]:
    price_new_low = curr_down.low_price <= prev_down.low_price + cfg.eps_price_atr * atr_ref
    if not price_new_low:
        return 0, {"price_new_low": False}

    votes = 0
    detail: dict[str, Any] = {"price_new_low": True}

    detail["bar_min_div"] = curr_down.bar_min > prev_down.bar_min
    detail["dif_min_div"] = curr_down.dif_min > prev_down.dif_min
    detail["area_div"] = curr_down.area_abs < prev_down.area_abs
    detail["eff_div"] = curr_down.efficiency(atr_ref) < prev_down.efficiency(atr_ref)
    votes += int(detail["bar_min_div"])
    votes += int(detail["dif_min_div"])
    votes += int(detail["area_div"])
    votes += int(detail["eff_div"])
    return votes, detail


def top_divergence_votes(
    curr_up: SegStats,
    prev_up: SegStats,
    atr_ref: float,
    cfg: ScanConfig,
) -> tuple[int, dict[str, Any]]:
    price_new_high = curr_up.high_price >= prev_up.high_price - cfg.eps_price_atr * atr_ref
    if not price_new_high:
        return 0, {"price_new_high": False}

    votes = 0
    detail: dict[str, Any] = {"price_new_high": True}

    detail["bar_max_div"] = curr_up.bar_max < prev_up.bar_max
    detail["dif_max_div"] = curr_up.dif_max < prev_up.dif_max
    detail["area_div"] = curr_up.area_abs < prev_up.area_abs
    detail["eff_div"] = curr_up.efficiency(atr_ref) < prev_up.efficiency(atr_ref)
    votes += int(detail["bar_max_div"])
    votes += int(detail["dif_max_div"])
    votes += int(detail["area_div"])
    votes += int(detail["eff_div"])
    return votes, detail


def detect_left_bottom_live(
    t: int,
    curr_down: Optional[SegStats],
    prev_down: Optional[SegStats],
    dif: np.ndarray,
    dea: np.ndarray,
    bar: np.ndarray,
    atr: np.ndarray,
    cfg: ScanConfig,
) -> tuple[bool, dict[str, Any]]:
    if curr_down is None or prev_down is None or curr_down.dir != DOWN:
        return False, {}
    if not dual_below_zero(dif, dea, t):
        return False, {}

    votes, detail = bottom_divergence_votes(curr_down, prev_down, atr[t], cfg)
    lag_bar = curr_down.lag_bar_bottom()
    lag_dif = curr_down.lag_dif_bottom()
    lag_ok = max(lag_bar, lag_dif) >= cfg.min_left_lag
    dwell = t - curr_down.low_idx
    shrink_age = t - curr_down.bar_min_idx
    first_shrink = first_shrink_down(bar, t)
    noise = cross_density(bar, t, cfg.cross_window)

    ok = (
        votes >= cfg.min_div_votes_left
        and lag_ok
        and first_shrink
        and dwell <= cfg.max_dwell_left
        and shrink_age <= cfg.max_shrink_age_left
        and noise <= cfg.max_cross_density_left
    )
    feat = {
        "votes": votes,
        "lag_bar": lag_bar,
        "lag_dif": lag_dif,
        "dwell_low": dwell,
        "shrink_age": shrink_age,
        "cross_density": noise,
        **detail,
    }
    return ok, feat


def detect_left_top_live(
    t: int,
    curr_up: Optional[SegStats],
    prev_up: Optional[SegStats],
    dif: np.ndarray,
    dea: np.ndarray,
    bar: np.ndarray,
    atr: np.ndarray,
    cfg: ScanConfig,
) -> tuple[bool, dict[str, Any]]:
    if curr_up is None or prev_up is None or curr_up.dir != UP:
        return False, {}
    if not dual_above_zero(dif, dea, t):
        return False, {}

    votes, detail = top_divergence_votes(curr_up, prev_up, atr[t], cfg)
    lag_bar = curr_up.lag_bar_top()
    lag_dif = curr_up.lag_dif_top()
    lag_ok = max(lag_bar, lag_dif) >= cfg.min_left_lag
    dwell = t - curr_up.high_idx
    shrink_age = t - curr_up.bar_max_idx
    first_shrink = first_shrink_up(bar, t)
    noise = cross_density(bar, t, cfg.cross_window)

    ok = (
        votes >= cfg.min_div_votes_left
        and lag_ok
        and first_shrink
        and dwell <= cfg.max_dwell_left
        and shrink_age <= cfg.max_shrink_age_left
        and noise <= cfg.max_cross_density_left
    )
    feat = {
        "votes": votes,
        "lag_bar": lag_bar,
        "lag_dif": lag_dif,
        "dwell_high": dwell,
        "shrink_age": shrink_age,
        "cross_density": noise,
        **detail,
    }
    return ok, feat


def detect_buy1_from_closed_down(
    t: int,
    closed_down: Optional[SegStats],
    prev_down: Optional[SegStats],
    dif: np.ndarray,
    dea: np.ndarray,
    bar: np.ndarray,
    atr: np.ndarray,
    cfg: ScanConfig,
) -> tuple[bool, dict[str, Any]]:
    if closed_down is None or prev_down is None:
        return False, {}
    if not cross_up(bar, t):
        return False, {}
    if not (dif[t] < 0 and dea[t] < 0):
        return False, {}

    votes, detail = bottom_divergence_votes(closed_down, prev_down, atr[t], cfg)
    lag_bar = closed_down.lag_bar_bottom()
    lag_dif = closed_down.lag_dif_bottom()
    lag = max(lag_bar, lag_dif)
    lag_ok = cfg.min_lag_1 <= lag <= cfg.max_lag_1
    t_conf1_buy = t - closed_down.low_idx
    conf_ok = t_conf1_buy <= cfg.max_conf1
    noise = cross_density(bar, t, cfg.cross_window)

    ok = (
        votes >= cfg.min_div_votes_1
        and lag_ok
        and conf_ok
        and noise <= cfg.max_cross_density_1
    )
    feat = {
        "votes": votes,
        "lag_bar": lag_bar,
        "lag_dif": lag_dif,
        "T_conf1_buy": t_conf1_buy,
        "cross_density": noise,
        "buy1_low": closed_down.low_price,
        "buy1_main_down_area": closed_down.area_abs,
        "buy1_main_down_eff": closed_down.efficiency(atr[t]),
        **detail,
    }
    return ok, feat


def detect_sell1_from_closed_up(
    t: int,
    closed_up: Optional[SegStats],
    prev_up: Optional[SegStats],
    dif: np.ndarray,
    dea: np.ndarray,
    bar: np.ndarray,
    atr: np.ndarray,
    cfg: ScanConfig,
) -> tuple[bool, dict[str, Any]]:
    if closed_up is None or prev_up is None:
        return False, {}
    if not cross_down(bar, t):
        return False, {}
    if not (dif[t] > 0 and dea[t] > 0):
        return False, {}

    votes, detail = top_divergence_votes(closed_up, prev_up, atr[t], cfg)
    lag_bar = closed_up.lag_bar_top()
    lag_dif = closed_up.lag_dif_top()
    lag = max(lag_bar, lag_dif)
    lag_ok = cfg.min_lag_1 <= lag <= cfg.max_lag_1
    t_conf1_sell = t - closed_up.high_idx
    conf_ok = t_conf1_sell <= cfg.max_conf1
    noise = cross_density(bar, t, cfg.cross_window)

    ok = (
        votes >= cfg.min_div_votes_1
        and lag_ok
        and conf_ok
        and noise <= cfg.max_cross_density_1
    )
    feat = {
        "votes": votes,
        "lag_bar": lag_bar,
        "lag_dif": lag_dif,
        "T_conf1_sell": t_conf1_sell,
        "cross_density": noise,
        "sell1_high": closed_up.high_price,
        "sell1_main_up_area": closed_up.area_abs,
        "sell1_main_up_eff": closed_up.efficiency(atr[t]),
        **detail,
    }
    return ok, feat


def detect_buy2_from_closed_pullback_down(
    t: int,
    closed_pull_down: Optional[SegStats],
    long_ctx: LongCtx,
    low: np.ndarray,
    bar: np.ndarray,
    atr: np.ndarray,
    cfg: ScanConfig,
) -> tuple[bool, dict[str, Any]]:
    if closed_pull_down is None:
        return False, {}
    if long_ctx.buy1_idx is None or long_ctx.pull2_start_idx is None:
        return False, {}
    if not cross_up(bar, t):
        return False, {}

    assert long_ctx.buy1_low is not None
    assert long_ctx.buy1_main_down_area is not None
    assert long_ctx.rise1_bars is not None

    not_break_buy1 = closed_pull_down.low_price >= long_ctx.buy1_low - cfg.eps_price_atr * atr[t]
    t_rise1 = long_ctx.rise1_bars
    t_pull2 = t - long_ctx.pull2_start_idx
    r_2 = t_pull2 / max(t_rise1, 1)
    revisit_low_bars = count_bars_near_level(
        low, atr, long_ctx.pull2_start_idx, t - 1, long_ctx.buy1_low, cfg.revisit_band_atr
    )
    area_ok = closed_pull_down.area_abs <= cfg.pull2_area_ratio * max(long_ctx.buy1_main_down_area, 1e-9)
    noise = cross_density(bar, t, cfg.cross_window)

    ok = (
        not_break_buy1
        and t_rise1 >= cfg.min_rise1_bars
        and t_pull2 <= cfg.max_pull2_bars
        and r_2 <= cfg.max_R2
        and revisit_low_bars <= cfg.max_revisit_low_bars
        and area_ok
        and noise <= cfg.max_cross_density_2
    )
    feat = {
        "T_rise1": t_rise1,
        "T_pull2": t_pull2,
        "R_2": r_2,
        "revisit_low_bars": revisit_low_bars,
        "pull2_area": closed_pull_down.area_abs,
        "cross_density": noise,
    }
    return ok, feat


def detect_sell2_from_closed_rebound_up(
    t: int,
    closed_rebound_up: Optional[SegStats],
    short_ctx: ShortCtx,
    high: np.ndarray,
    bar: np.ndarray,
    atr: np.ndarray,
    cfg: ScanConfig,
) -> tuple[bool, dict[str, Any]]:
    if closed_rebound_up is None:
        return False, {}
    if short_ctx.sell1_idx is None or short_ctx.pull2_start_idx is None:
        return False, {}
    if not cross_down(bar, t):
        return False, {}

    assert short_ctx.sell1_high is not None
    assert short_ctx.sell1_main_up_area is not None
    assert short_ctx.drop1_bars is not None

    not_break_sell1 = closed_rebound_up.high_price <= short_ctx.sell1_high + cfg.eps_price_atr * atr[t]
    t_drop1 = short_ctx.drop1_bars
    t_pull2 = t - short_ctx.pull2_start_idx
    r_2 = t_pull2 / max(t_drop1, 1)
    revisit_high_bars = count_bars_near_level(
        high, atr, short_ctx.pull2_start_idx, t - 1, short_ctx.sell1_high, cfg.revisit_band_atr
    )
    area_ok = closed_rebound_up.area_abs <= cfg.pull2_area_ratio * max(short_ctx.sell1_main_up_area, 1e-9)
    noise = cross_density(bar, t, cfg.cross_window)

    ok = (
        not_break_sell1
        and t_drop1 >= cfg.min_rise1_bars
        and t_pull2 <= cfg.max_pull2_bars
        and r_2 <= cfg.max_R2
        and revisit_high_bars <= cfg.max_revisit_high_bars
        and area_ok
        and noise <= cfg.max_cross_density_2
    )
    feat = {
        "T_drop1": t_drop1,
        "T_pull2": t_pull2,
        "R_2": r_2,
        "revisit_high_bars": revisit_high_bars,
        "pull2_area": closed_rebound_up.area_abs,
        "cross_density": noise,
    }
    return ok, feat


def detect_buy3_from_closed_pullback_down(
    t: int,
    closed_pull_down: Optional[SegStats],
    long_ctx: LongCtx,
    bar: np.ndarray,
    cfg: ScanConfig,
) -> tuple[bool, dict[str, Any]]:
    if closed_pull_down is None:
        return False, {}
    if long_ctx.zero_up_idx is None or long_ctx.pull3_start_idx is None:
        return False, {}
    if not cross_up(bar, t):
        return False, {}

    assert long_ctx.buy1_main_down_area is not None

    t_pull3 = t - long_ctx.pull3_start_idx
    b0 = closed_pull_down.below0_bars
    t_near0 = closed_pull_down.near0_bars
    area_ok = closed_pull_down.area_abs <= cfg.pull3_area_ratio * max(long_ctx.buy1_main_down_area, 1e-9)
    noise = cross_density(bar, t, cfg.cross_window)

    ok = (
        t_pull3 <= cfg.max_pull3_bars
        and b0 <= cfg.max_B0_bars
        and t_near0 <= cfg.max_near0_bars
        and area_ok
        and noise <= cfg.max_cross_density_3
    )
    feat = {
        "T_pull3": t_pull3,
        "B0": b0,
        "T_near0": t_near0,
        "pull3_area": closed_pull_down.area_abs,
        "cross_density": noise,
    }
    return ok, feat


def detect_sell3_from_closed_rebound_up(
    t: int,
    closed_rebound_up: Optional[SegStats],
    short_ctx: ShortCtx,
    bar: np.ndarray,
    cfg: ScanConfig,
) -> tuple[bool, dict[str, Any]]:
    if closed_rebound_up is None:
        return False, {}
    if short_ctx.zero_down_idx is None or short_ctx.pull3_start_idx is None:
        return False, {}
    if not cross_down(bar, t):
        return False, {}

    assert short_ctx.sell1_main_up_area is not None

    t_pull3 = t - short_ctx.pull3_start_idx
    a0 = closed_rebound_up.above0_bars
    t_near0 = closed_rebound_up.near0_bars
    area_ok = closed_rebound_up.area_abs <= cfg.pull3_area_ratio * max(short_ctx.sell1_main_up_area, 1e-9)
    noise = cross_density(bar, t, cfg.cross_window)

    ok = (
        t_pull3 <= cfg.max_pull3_bars
        and a0 <= cfg.max_B0_bars
        and t_near0 <= cfg.max_near0_bars
        and area_ok
        and noise <= cfg.max_cross_density_3
    )
    feat = {
        "T_pull3": t_pull3,
        "A0": a0,
        "T_near0": t_near0,
        "pull3_area": closed_rebound_up.area_abs,
        "cross_density": noise,
    }
    return ok, feat


def reset_long() -> LongCtx:
    return LongCtx()


def reset_short() -> ShortCtx:
    return ShortCtx()


def on_left_bottom(long_ctx: LongCtx, t: int, price: float) -> None:
    if long_ctx.state == LONG_NONE:
        long_ctx.state = LONG_LEFT_BOTTOM
        long_ctx.left_bottom_idx = t
        long_ctx.left_bottom_price = price


def on_left_top(short_ctx: ShortCtx, t: int, price: float) -> None:
    if short_ctx.state == SHORT_NONE:
        short_ctx.state = SHORT_LEFT_TOP
        short_ctx.left_top_idx = t
        short_ctx.left_top_price = price


def on_buy1(long_ctx: LongCtx, t: int, price: float, feat: dict[str, Any]) -> None:
    long_ctx.state = LONG_BUY1
    long_ctx.buy1_idx = t
    long_ctx.buy1_price = price
    long_ctx.buy1_low = float(feat["buy1_low"])
    long_ctx.buy1_main_down_area = float(feat["buy1_main_down_area"])
    long_ctx.buy1_main_down_eff = float(feat["buy1_main_down_eff"])
    long_ctx.pull2_start_idx = None
    long_ctx.rise1_bars = None
    long_ctx.rise1_high = None
    long_ctx.buy2_idx = None
    long_ctx.zero_up_idx = None
    long_ctx.pull3_start_idx = None
    long_ctx.buy3_idx = None


def on_sell1(short_ctx: ShortCtx, t: int, price: float, feat: dict[str, Any]) -> None:
    short_ctx.state = SHORT_SELL1
    short_ctx.sell1_idx = t
    short_ctx.sell1_price = price
    short_ctx.sell1_high = float(feat["sell1_high"])
    short_ctx.sell1_main_up_area = float(feat["sell1_main_up_area"])
    short_ctx.sell1_main_up_eff = float(feat["sell1_main_up_eff"])
    short_ctx.pull2_start_idx = None
    short_ctx.drop1_bars = None
    short_ctx.drop1_low = None
    short_ctx.sell2_idx = None
    short_ctx.zero_down_idx = None
    short_ctx.pull3_start_idx = None
    short_ctx.sell3_idx = None


def on_first_cross_down_after_buy1(long_ctx: LongCtx, t: int, high: np.ndarray) -> None:
    if long_ctx.buy1_idx is None:
        return
    if long_ctx.zero_up_idx is None:
        long_ctx.state = LONG_PULL2
        long_ctx.pull2_start_idx = t
        long_ctx.rise1_bars = t - long_ctx.buy1_idx
        long_ctx.rise1_high = float(np.max(high[long_ctx.buy1_idx:t])) if t > long_ctx.buy1_idx else None


def on_first_cross_up_after_sell1(short_ctx: ShortCtx, t: int, low: np.ndarray) -> None:
    if short_ctx.sell1_idx is None:
        return
    if short_ctx.zero_down_idx is None:
        short_ctx.state = SHORT_PULL2
        short_ctx.pull2_start_idx = t
        short_ctx.drop1_bars = t - short_ctx.sell1_idx
        short_ctx.drop1_low = float(np.min(low[short_ctx.sell1_idx:t])) if t > short_ctx.sell1_idx else None


def on_buy2(long_ctx: LongCtx, t: int) -> None:
    long_ctx.state = LONG_BUY2
    long_ctx.buy2_idx = t


def on_sell2(short_ctx: ShortCtx, t: int) -> None:
    short_ctx.state = SHORT_SELL2
    short_ctx.sell2_idx = t


def on_zero_up(long_ctx: LongCtx, t: int) -> None:
    if long_ctx.buy1_idx is not None and long_ctx.zero_up_idx is None:
        long_ctx.zero_up_idx = t
        long_ctx.state = LONG_ABOVE0


def on_zero_down(short_ctx: ShortCtx, t: int) -> None:
    if short_ctx.sell1_idx is not None and short_ctx.zero_down_idx is None:
        short_ctx.zero_down_idx = t
        short_ctx.state = SHORT_BELOW0


def on_first_cross_down_after_zero_up(long_ctx: LongCtx, t: int) -> None:
    if long_ctx.zero_up_idx is not None:
        long_ctx.state = LONG_PULL3
        long_ctx.pull3_start_idx = t


def on_first_cross_up_after_zero_down(short_ctx: ShortCtx, t: int) -> None:
    if short_ctx.zero_down_idx is not None:
        short_ctx.state = SHORT_PULL3
        short_ctx.pull3_start_idx = t


def on_buy3(long_ctx: LongCtx, t: int) -> None:
    long_ctx.state = LONG_BUY3
    long_ctx.buy3_idx = t


def on_sell3(short_ctx: ShortCtx, t: int) -> None:
    short_ctx.state = SHORT_SELL3
    short_ctx.sell3_idx = t


def simple_signal_score(signal_type: str, feat: dict[str, Any], cfg: ScanConfig) -> float:
    del cfg
    score = 50.0
    if signal_type in {"LEFT_BOTTOM", "LEFT_TOP"}:
        score += 4 * min(float(feat.get("votes", 0)), 4)
        score += 2 * max(min(float(feat.get("lag_bar", 0)), 5), 0)
        score -= 10 * float(feat.get("cross_density", 0))
    elif signal_type in {"BUY1", "SELL1"}:
        score += 4 * min(float(feat.get("votes", 0)), 4)
        score += 2 * max(min(float(feat.get("lag_bar", 0)), 5), 0)
        score += max(0, 6 - float(feat.get("T_conf1_buy", feat.get("T_conf1_sell", 6))))
        score -= 12 * float(feat.get("cross_density", 0))
    elif signal_type in {"BUY2", "SELL2"}:
        score += max(0, 10 - 10 * float(feat.get("R_2", 10)))
        score += max(0, 6 - float(feat.get("revisit_low_bars", feat.get("revisit_high_bars", 6))))
        score -= 10 * float(feat.get("cross_density", 0))
    elif signal_type in {"BUY3", "SELL3"}:
        score += max(0, 6 - float(feat.get("T_pull3", 6)))
        score += max(0, 3 - float(feat.get("B0", feat.get("A0", 3))))
        score += max(0, 4 - float(feat.get("T_near0", 4)))
        score -= 12 * float(feat.get("cross_density", 0))
    return round(float(score), 4)


# ==============================
# Engine
# ==============================


class MACDTimeSignalEngine:
    def __init__(self, cfg: ScanConfig) -> None:
        self.cfg = cfg

    def scan_dataframe(self, df: pd.DataFrame, symbol: str = "", name: str = "") -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame()

        data = compute_indicators(df, self.cfg).copy()
        data = data.dropna(
            subset=["date", "open", "high", "low", "close", "dif", "dea", "bar", "atr"]
        ).reset_index(drop=True)
        if len(data) < 80:
            return pd.DataFrame()

        dt = data["date"]
        high = data["high"].to_numpy(dtype=float)
        low = data["low"].to_numpy(dtype=float)
        close = data["close"].to_numpy(dtype=float)
        dif = data["dif"].to_numpy(dtype=float)
        dea = data["dea"].to_numpy(dtype=float)
        bar = data["bar"].to_numpy(dtype=float)
        atr = data["atr"].to_numpy(dtype=float)

        start = 2
        init_dir = UP if bar[start] >= 0 else DOWN
        current_seg = seg_new(
            init_dir,
            start,
            high[start],
            low[start],
            close[start],
            dif[start],
            dea[start],
            bar[start],
            self.cfg.eps_zero_atr * atr[start],
        )

        last_closed_down: Optional[SegStats] = None
        last_closed_up: Optional[SegStats] = None
        long_ctx = reset_long()
        short_ctx = reset_short()
        signals: list[dict[str, Any]] = []

        for t in range(start + 1, len(data)):
            eps0 = self.cfg.eps_zero_atr * atr[t]
            cu = cross_up(bar, t)
            cd = cross_down(bar, t)

            if not cu and not cd:
                current_seg = seg_update(current_seg, t, high[t], low[t], close[t], dif[t], dea[t], bar[t], eps0)

                if current_seg.dir == DOWN and last_closed_down is not None:
                    ok_lb, feat_lb = detect_left_bottom_live(
                        t, current_seg, last_closed_down, dif, dea, bar, atr, self.cfg
                    )
                    if ok_lb:
                        on_left_bottom(long_ctx, t, low[t])
                        signals.append(self._make_signal_row(dt[t], symbol, name, close[t], "LEFT_BOTTOM", feat_lb))

                if current_seg.dir == UP and last_closed_up is not None:
                    ok_lt, feat_lt = detect_left_top_live(
                        t, current_seg, last_closed_up, dif, dea, bar, atr, self.cfg
                    )
                    if ok_lt:
                        on_left_top(short_ctx, t, high[t])
                        signals.append(self._make_signal_row(dt[t], symbol, name, close[t], "LEFT_TOP", feat_lt))

            elif cu:
                closed_down = dataclasses.replace(current_seg)
                closed_down.end_idx = t - 1

                ok_b1, feat_b1 = detect_buy1_from_closed_down(
                    t, closed_down, last_closed_down, dif, dea, bar, atr, self.cfg
                )
                ok_b2, feat_b2 = detect_buy2_from_closed_pullback_down(
                    t, closed_down, long_ctx, low, bar, atr, self.cfg
                )
                ok_b3, feat_b3 = detect_buy3_from_closed_pullback_down(t, closed_down, long_ctx, bar, self.cfg)

                if ok_b3:
                    on_buy3(long_ctx, t)
                    short_ctx = reset_short()
                    signals.append(self._make_signal_row(dt[t], symbol, name, close[t], "BUY3", feat_b3))
                elif ok_b2:
                    on_buy2(long_ctx, t)
                    short_ctx = reset_short()
                    signals.append(self._make_signal_row(dt[t], symbol, name, close[t], "BUY2", feat_b2))
                elif ok_b1:
                    on_buy1(long_ctx, t, close[t], feat_b1)
                    short_ctx = reset_short()
                    signals.append(self._make_signal_row(dt[t], symbol, name, close[t], "BUY1", feat_b1))

                last_closed_down = closed_down
                current_seg = seg_new(UP, t, high[t], low[t], close[t], dif[t], dea[t], bar[t], eps0)

            else:
                closed_up = dataclasses.replace(current_seg)
                closed_up.end_idx = t - 1

                ok_s1, feat_s1 = detect_sell1_from_closed_up(
                    t, closed_up, last_closed_up, dif, dea, bar, atr, self.cfg
                )
                ok_s2, feat_s2 = detect_sell2_from_closed_rebound_up(
                    t, closed_up, short_ctx, high, bar, atr, self.cfg
                )
                ok_s3, feat_s3 = detect_sell3_from_closed_rebound_up(t, closed_up, short_ctx, bar, self.cfg)

                if ok_s3:
                    on_sell3(short_ctx, t)
                    long_ctx = reset_long()
                    signals.append(self._make_signal_row(dt[t], symbol, name, close[t], "SELL3", feat_s3))
                elif ok_s2:
                    on_sell2(short_ctx, t)
                    long_ctx = reset_long()
                    signals.append(self._make_signal_row(dt[t], symbol, name, close[t], "SELL2", feat_s2))
                elif ok_s1:
                    on_sell1(short_ctx, t, close[t], feat_s1)
                    long_ctx = reset_long()
                    signals.append(self._make_signal_row(dt[t], symbol, name, close[t], "SELL1", feat_s1))

                last_closed_up = closed_up
                current_seg = seg_new(DOWN, t, high[t], low[t], close[t], dif[t], dea[t], bar[t], eps0)

            if long_ctx.buy1_idx is not None and long_ctx.zero_up_idx is None and dual_above_zero(dif, dea, t):
                on_zero_up(long_ctx, t)
            if short_ctx.sell1_idx is not None and short_ctx.zero_down_idx is None and dual_below_zero(dif, dea, t):
                on_zero_down(short_ctx, t)

            if cd and long_ctx.buy1_idx is not None and long_ctx.zero_up_idx is None:
                on_first_cross_down_after_buy1(long_ctx, t, high)
            if cu and short_ctx.sell1_idx is not None and short_ctx.zero_down_idx is None:
                on_first_cross_up_after_sell1(short_ctx, t, low)
            if cd and long_ctx.zero_up_idx is not None:
                on_first_cross_down_after_zero_up(long_ctx, t)
            if cu and short_ctx.zero_down_idx is not None:
                on_first_cross_up_after_zero_down(short_ctx, t)

            if long_ctx.buy1_low is not None and low[t] < long_ctx.buy1_low - self.cfg.eps_price_atr * atr[t]:
                long_ctx = reset_long()
            if short_ctx.sell1_high is not None and high[t] > short_ctx.sell1_high + self.cfg.eps_price_atr * atr[t]:
                short_ctx = reset_short()

        sig_df = pd.DataFrame(signals)
        if sig_df.empty:
            return sig_df

        if self.cfg.latest_only:
            sig_df = (
                sig_df.sort_values(["date", "score"], ascending=[False, False])
                .groupby(["symbol"], as_index=False)
                .head(1)
                .reset_index(drop=True)
            )

        last_date = dt.iloc[-1]
        recent_start = last_date - pd.Timedelta(days=max(self.cfg.recent_bars * 2, 3))
        sig_df = sig_df[sig_df["date"] >= recent_start].copy()
        if not sig_df.empty:
            sig_df = sig_df.sort_values(["date", "score"], ascending=[False, False]).reset_index(drop=True)
        return sig_df

    def _make_signal_row(
        self,
        dt: pd.Timestamp,
        symbol: str,
        name: str,
        price: float,
        signal_type: str,
        feat: dict[str, Any],
    ) -> dict[str, Any]:
        row: dict[str, Any] = {
            "date": pd.Timestamp(dt),
            "symbol": symbol,
            "name": name,
            "signal": signal_type,
            "close": round(float(price), 4),
            "score": simple_signal_score(signal_type, feat, self.cfg),
        }
        for k, v in feat.items():
            if isinstance(v, (np.floating, float)):
                row[k] = round(float(v), 6)
            elif isinstance(v, (np.integer, int)):
                row[k] = int(v)
            else:
                row[k] = v
        return row


# ==============================
# Data provider
# ==============================


class AKShareProvider:
    def __init__(self) -> None:
        try:
            import akshare as ak  # lazy import
        except ImportError as exc:
            raise SystemExit(
                "akshare is not installed. Run `pip install -r requirements.txt` first."
            ) from exc
        self.ak = ak

    def get_universe(self, cfg: ScanConfig) -> pd.DataFrame:
        spot = self.ak.stock_zh_a_spot_em().copy()
        keep_cols = [c for c in ["代码", "名称", "最新价", "成交额", "流通市值", "换手率"] if c in spot.columns]
        spot = spot[keep_cols].copy()
        spot = spot.rename(
            columns={
                "代码": "symbol",
                "名称": "name",
                "最新价": "last",
                "成交额": "amount",
                "流通市值": "float_mkt_cap",
                "换手率": "turnover",
            }
        )
        if cfg.exclude_st:
            spot = spot[~spot["name"].astype(str).str.contains("ST", case=False, na=False)]
        if cfg.exclude_delisting:
            spot = spot[~spot["name"].astype(str).str.contains("退", na=False)]
        if "amount" in spot.columns:
            spot = spot[pd.to_numeric(spot["amount"], errors="coerce").fillna(0) >= cfg.min_amount]
        if "last" in spot.columns:
            spot = spot[pd.to_numeric(spot["last"], errors="coerce").fillna(0) >= cfg.min_price]
        spot = spot.drop_duplicates(subset=["symbol"]).reset_index(drop=True)
        if cfg.limit is not None:
            spot = spot.head(cfg.limit).copy()
        return spot

    def get_history(self, symbol: str, cfg: ScanConfig) -> pd.DataFrame:
        df = self.ak.stock_zh_a_hist(
            symbol=symbol,
            period=cfg.period,
            start_date=cfg.start_date,
            end_date=cfg.end_date,
            adjust=cfg.adjust,
        )
        if df is None or df.empty:
            return pd.DataFrame()
        return normalize_akshare_history(df)


# ==============================
# Normalization / universe scan
# ==============================


def normalize_akshare_history(df: pd.DataFrame) -> pd.DataFrame:
    mapping = {
        "日期": "date",
        "date": "date",
        "开盘": "open",
        "open": "open",
        "最高": "high",
        "high": "high",
        "最低": "low",
        "low": "low",
        "收盘": "close",
        "close": "close",
        "成交量": "volume",
        "volume": "volume",
        "成交额": "amount",
        "amount": "amount",
        "换手率": "turnover",
        "turnover": "turnover",
    }
    out = df.rename(columns=mapping).copy()
    need = ["date", "open", "high", "low", "close"]
    for col in need:
        if col not in out.columns:
            raise ValueError(f"history dataframe missing column: {col}")
    out["date"] = pd.to_datetime(out["date"])
    for col in ["open", "high", "low", "close", "volume", "amount", "turnover"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    out = (
        out.dropna(subset=["date", "open", "high", "low", "close"])
        .sort_values("date")
        .reset_index(drop=True)
    )
    return out


def scan_symbol_task(row: dict[str, Any], cfg: ScanConfig) -> pd.DataFrame:
    provider = AKShareProvider()
    hist = provider.get_history(str(row["symbol"]), cfg)
    if hist.empty:
        return pd.DataFrame()
    engine = MACDTimeSignalEngine(cfg)
    return engine.scan_dataframe(hist, symbol=str(row["symbol"]), name=str(row.get("name", "")))


def run_universe_scan(cfg: ScanConfig) -> pd.DataFrame:
    provider = AKShareProvider()
    universe = provider.get_universe(cfg)
    if universe.empty:
        return pd.DataFrame()

    rows = universe.to_dict("records")
    chunks: list[pd.DataFrame] = []

    with futures.ThreadPoolExecutor(max_workers=max(cfg.workers, 1)) as ex:
        tasks = {ex.submit(scan_symbol_task, row, cfg): row for row in rows}
        for fut in futures.as_completed(tasks):
            row = tasks[fut]
            try:
                df = fut.result()
                if df is not None and not df.empty:
                    chunks.append(df)
            except Exception as exc:
                chunks.append(
                    pd.DataFrame(
                        [
                            {
                                "date": pd.NaT,
                                "symbol": row.get("symbol"),
                                "name": row.get("name"),
                                "signal": "ERROR",
                                "close": np.nan,
                                "score": -1,
                                "error": str(exc),
                            }
                        ]
                    )
                )

    if not chunks:
        return pd.DataFrame()

    out = pd.concat(chunks, ignore_index=True)
    if "signal" in out.columns:
        out = out[out["signal"] != "ERROR"].copy()
    if not out.empty:
        out = out.sort_values(["date", "score", "symbol"], ascending=[False, False, True]).reset_index(drop=True)
    return out


def run_single_symbol_scan(symbol: str, cfg: ScanConfig, name: str = "") -> pd.DataFrame:
    provider = AKShareProvider()
    hist = provider.get_history(symbol, cfg)
    if hist.empty:
        return pd.DataFrame()
    engine = MACDTimeSignalEngine(cfg)
    return engine.scan_dataframe(hist, symbol=symbol, name=name)


# ==============================
# CLI
# ==============================


def default_output_path() -> Path:
    return Path(__file__).resolve().parent / "outputs" / f"a_share_signals_v{PROJECT_VERSION.replace('.', '_')}.csv"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="A-share MACD time-signal scanner")
    p.add_argument("--start-date", default="20220101")
    p.add_argument("--end-date", default="20300101")
    p.add_argument("--adjust", default="qfq", choices=["", "qfq", "hfq"])
    p.add_argument("--period", default="daily", choices=["daily", "weekly", "monthly"])
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--min-amount", type=float, default=1.0e8)
    p.add_argument("--min-price", type=float, default=2.0)
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--recent-bars", type=int, default=5)
    p.add_argument("--symbol", default=None, help="Scan a single A-share symbol, for example 000001")
    p.add_argument("--name", default="", help="Optional name used together with --symbol")
    p.add_argument("--all-signals", action="store_true", help="Keep all recent signals instead of latest signal per symbol")
    p.add_argument("--include-st", action="store_true")
    p.add_argument("--include-delisting", action="store_true")
    p.add_argument("--output", default=str(default_output_path()))
    p.add_argument("--version", action="store_true", help="Print version and exit")
    return p.parse_args()


def build_config(args: argparse.Namespace) -> ScanConfig:
    return ScanConfig(
        start_date=args.start_date,
        end_date=args.end_date,
        adjust=args.adjust,
        period=args.period,
        workers=args.workers,
        min_amount=args.min_amount,
        min_price=args.min_price,
        limit=args.limit,
        recent_bars=args.recent_bars,
        latest_only=not args.all_signals,
        exclude_st=not args.include_st,
        exclude_delisting=not args.include_delisting,
    )


def save_output(out: pd.DataFrame, output_path: str) -> Path:
    path = Path(output_path).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(path, index=False, encoding="utf-8-sig")
    return path


def main() -> None:
    args = parse_args()
    if args.version:
        print(PROJECT_VERSION)
        return

    cfg = build_config(args)
    out = run_single_symbol_scan(args.symbol, cfg, args.name) if args.symbol else run_universe_scan(cfg)
    saved_path = save_output(out, args.output)
    scan_mode = f"symbol={args.symbol}" if args.symbol else "universe"
    print(f"macd_time_signal_scanner v{PROJECT_VERSION}")
    print(f"mode: {scan_mode}")
    print(f"saved {len(out)} rows -> {saved_path}")
    if not out.empty:
        with pd.option_context("display.max_columns", 50, "display.width", 180):
            print(out.head(30).to_string(index=False))


if __name__ == "__main__":
    main()
