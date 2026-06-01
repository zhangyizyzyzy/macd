#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from backtest_store import BacktestStore
from macd_time_signal_scanner import AKShareProvider, MACDTimeSignalEngine, PROJECT_VERSION, ScanConfig, compute_indicators
from macd_timeframe_backtest import aggregate_intraday, elapsed_days
from market_data_store import MarketDataStore


MACD_FAST_ALPHA = 2 / (12 + 1)
MACD_SLOW_ALPHA = 2 / (26 + 1)
MACD_SIGNAL_ALPHA = 2 / (9 + 1)
BUY1_SIGNAL = "BUY1"
SELL1_SIGNAL = "SELL1"
SIGNAL_LEVEL_CHOICES = ["15m", "60m", "240m"]
FILTER_LEVEL_CHOICES = ["240m", "daily", "weekly"]
SIGNAL_LEVEL_TO_MINUTES = {"15m": 15, "60m": 60, "240m": 240}
SIGNAL_LEVEL_TO_GROUP = {"15m": 1, "60m": 4, "240m": 16}
SIGNAL_LEVEL_ALIASES = {"15m": "15m", "1h": "60m", "60m": "60m", "4h": "240m", "240m": "240m"}
FILTER_LEVEL_ALIASES = {"4h": "240m", "240m": "240m", "1d": "daily", "daily": "daily", "1w": "weekly", "week": "weekly", "weekly": "weekly"}


@dataclass(slots=True)
class DailyGateState:
    dif: float
    dea: float
    zone: str
    relation: str
    label: str
    allow_buy: bool
    allow_sell: bool
    buy_reason: str
    sell_reason: str


@dataclass(slots=True)
class TransitionState:
    allow_buy: bool
    allow_sell: bool
    buy_phase: str | None
    sell_phase: str | None
    hist: float
    previous_hist: float


@dataclass(slots=True)
class EntryPlan:
    entry_idx: int
    entry_time: pd.Timestamp
    entry_price: float
    entry_method: str
    atr_value: float
    stop_price: float
    risk_distance: float


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="A-share family1 research-enhanced backtest on HS300")
    parser.add_argument("--start-date", default="20240101")
    parser.add_argument("--end-date", default="20300101")
    parser.add_argument("--adjust", default="qfq", choices=["", "qfq", "hfq"])
    parser.add_argument("--symbols", default="", help="Optional comma-separated symbols for smoke tests")
    parser.add_argument("--limit", type=int, default=None, help="Optional universe cap after HS300 selection")
    parser.add_argument("--db-path", default=str(Path("data") / "market_data.sqlite"))
    parser.add_argument("--backtest-db-path", default=str(Path("outputs") / "backtest_store.sqlite"))
    parser.add_argument("--hs300-cache", default=str(Path("data") / "hs300_current.csv"))
    parser.add_argument("--output", default=str(Path("outputs") / "family1_research_enhanced_a_share_trades.csv"))
    parser.add_argument("--summary-output", default=str(Path("outputs") / "family1_research_enhanced_a_share_summary.csv"))
    parser.add_argument("--symbol-summary-output", default=str(Path("outputs") / "family1_research_enhanced_a_share_symbols.csv"))
    parser.add_argument("--max-hold-bars", type=int, default=80)
    parser.add_argument("--stop-buffer-atr", type=float, default=0.2)
    parser.add_argument("--signal-level", default="15m")
    parser.add_argument("--gate-level", default="daily")
    parser.add_argument("--transition-level", default="240m")
    args = parser.parse_args()
    args.signal_level = normalize_signal_level(args.signal_level)
    args.gate_level = normalize_filter_level(args.gate_level)
    args.transition_level = normalize_filter_level(args.transition_level)
    return args


def build_scan_config(args: argparse.Namespace) -> ScanConfig:
    return ScanConfig(
        start_date=args.start_date,
        end_date=args.end_date,
        adjust=args.adjust,
        period="daily",
        workers=1,
        min_amount=0.0,
        min_price=0.0,
        limit=None,
        recent_bars=9999,
        latest_only=False,
        exclude_st=False,
        exclude_delisting=False,
    )


def normalize_symbol(value: Any) -> str:
    match = re.search(r"(\d{6})", str(value))
    return match.group(1) if match else str(value).strip()


def normalize_signal_level(value: str) -> str:
    normalized = SIGNAL_LEVEL_ALIASES.get(str(value).strip().lower())
    if normalized is None:
        raise SystemExit(f"unsupported signal level: {value}")
    return normalized


def normalize_filter_level(value: str) -> str:
    normalized = FILTER_LEVEL_ALIASES.get(str(value).strip().lower())
    if normalized is None:
        raise SystemExit(f"unsupported filter level: {value}")
    return normalized


def normalize_hs300_frame(raw: pd.DataFrame) -> pd.DataFrame:
    if raw is None or raw.empty:
        return pd.DataFrame(columns=["symbol", "name"])
    symbol_col = next((col for col in ["品种代码", "成分券代码", "证券代码", "代码", "symbol"] if col in raw.columns), None)
    name_col = next((col for col in ["品种名称", "成分券名称", "证券简称", "名称", "name"] if col in raw.columns), None)
    if symbol_col is None:
        raise ValueError(f"unable to locate HS300 symbol column: {list(raw.columns)}")
    out = pd.DataFrame()
    out["symbol"] = raw[symbol_col].map(normalize_symbol)
    out["name"] = raw[name_col].astype(str).str.strip() if name_col else ""
    out = out[out["symbol"].str.fullmatch(r"\d{6}", na=False)].drop_duplicates(subset=["symbol"]).reset_index(drop=True)
    return out


def fetch_hs300_universe(store: MarketDataStore, cache_path: Path) -> pd.DataFrame:
    raw_df = pd.DataFrame()
    fetch_error: Exception | None = None
    provider = None
    try:
        provider = AKShareProvider()
        for func_name in [
            "index_stock_cons_csindex",
            "index_stock_cons_weight_csindex",
            "index_stock_cons_sina",
            "index_stock_cons",
        ]:
            func = getattr(provider.ak, func_name, None)
            if func is None:
                continue
            try:
                candidate = func(symbol="000300")
            except Exception as exc:
                fetch_error = exc
                continue
            if candidate is not None and not candidate.empty:
                raw_df = candidate
                break
    except Exception as exc:
        fetch_error = exc

    if raw_df is not None and not raw_df.empty:
        out = normalize_hs300_frame(raw_df)
        latest_universe = store.load_latest_universe()
        if not latest_universe.empty:
            meta = latest_universe[["symbol", "name"]].drop_duplicates(subset=["symbol"]).rename(columns={"name": "market_name"})
            out = out.merge(meta, on="symbol", how="left")
            out["name"] = out["market_name"].fillna(out["name"]).fillna("")
            out = out.drop(columns=["market_name"])
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        out.to_csv(cache_path, index=False, encoding="utf-8-sig")
        return out

    if cache_path.exists():
        cached = pd.read_csv(cache_path, dtype={"symbol": str})
        return normalize_hs300_frame(cached)

    if fetch_error is not None:
        raise RuntimeError(f"failed to fetch HS300 universe and no cache found: {fetch_error}") from fetch_error
    raise RuntimeError("failed to fetch HS300 universe and no cache found")


def load_universe(args: argparse.Namespace, store: MarketDataStore) -> tuple[pd.DataFrame, str]:
    latest_universe = store.load_latest_universe()
    latest_name_map = {}
    if not latest_universe.empty:
        latest_name_map = {
            str(row["symbol"]): str(row.get("name", ""))
            for _, row in latest_universe.iterrows()
        }

    if args.symbols:
        symbols = [normalize_symbol(item) for item in args.symbols.split(",") if item.strip()]
        rows = [{"symbol": symbol, "name": latest_name_map.get(symbol, "")} for symbol in symbols]
        return pd.DataFrame(rows).drop_duplicates(subset=["symbol"]).reset_index(drop=True), "custom_symbols"

    universe = fetch_hs300_universe(store, Path(args.hs300_cache).expanduser().resolve())
    if latest_name_map:
        universe["name"] = universe["symbol"].map(latest_name_map).fillna(universe["name"])
    if args.limit is not None:
        universe = universe.head(args.limit).copy()
    return universe.reset_index(drop=True), "HS300_current"


def load_market_kline(
    store: MarketDataStore,
    symbol: str,
    level: str,
    adjust: str,
    start_dt: str,
    end_dt: str,
    sources: list[str],
) -> pd.DataFrame:
    for source in sources:
        df = store.load_kline(symbol, level, adjust, source, start_dt, end_dt)
        if not df.empty:
            return df
    return pd.DataFrame()


def aggregate_daily_to_weekly(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    work = df.copy()
    work["week_key"] = work["date"].dt.to_period("W-FRI")
    grouped = (
        work.groupby("week_key", as_index=False)
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


def load_signal_level_data(
    market_store: MarketDataStore,
    symbol: str,
    signal_level: str,
    cfg: ScanConfig,
) -> pd.DataFrame:
    start_intraday = f"{pd.Timestamp(cfg.start_date).strftime('%Y-%m-%d')} 00:00:00"
    end_intraday = f"{min(pd.Timestamp(cfg.end_date), pd.Timestamp.now()).strftime('%Y-%m-%d')} 23:59:59"
    base_15m = load_market_kline(
        market_store,
        symbol,
        "15m",
        cfg.adjust,
        start_intraday,
        end_intraday,
        sources=["baostock"],
    )
    if base_15m.empty:
        return pd.DataFrame()
    bars_per_group = SIGNAL_LEVEL_TO_GROUP[signal_level]
    return base_15m if bars_per_group == 1 else aggregate_intraday(base_15m, bars_per_group)


def load_filter_level_data(
    market_store: MarketDataStore,
    symbol: str,
    level: str,
    cfg: ScanConfig,
    signal_df: pd.DataFrame,
    signal_level: str,
) -> pd.DataFrame:
    if signal_df.empty:
        return pd.DataFrame()
    if level == "240m":
        if signal_level == "240m":
            return signal_df.copy()
        start_intraday = f"{pd.Timestamp(cfg.start_date).strftime('%Y-%m-%d')} 00:00:00"
        end_intraday = f"{min(pd.Timestamp(cfg.end_date), pd.Timestamp.now()).strftime('%Y-%m-%d')} 23:59:59"
        base_15m = load_market_kline(
            market_store,
            symbol,
            "15m",
            cfg.adjust,
            start_intraday,
            end_intraday,
            sources=["baostock"],
        )
        return aggregate_intraday(base_15m, 16) if not base_15m.empty else pd.DataFrame()

    start_intraday = f"{pd.Timestamp(cfg.start_date).strftime('%Y-%m-%d')} 00:00:00"
    end_intraday = f"{min(pd.Timestamp(cfg.end_date), pd.Timestamp.now()).strftime('%Y-%m-%d')} 23:59:59"
    daily_df = load_market_kline(
        market_store,
        symbol,
        "daily",
        cfg.adjust,
        start_intraday,
        end_intraday,
        sources=["akshare", "baostock"],
    )
    if daily_df.empty:
        bars_per_day = max(1, 240 // SIGNAL_LEVEL_TO_MINUTES[signal_level])
        daily_df = aggregate_intraday(signal_df, bars_per_day)
        if not daily_df.empty:
            daily_df["date"] = daily_df["date"].dt.normalize()
    if level == "daily":
        return daily_df
    if level == "weekly":
        return aggregate_daily_to_weekly(daily_df) if not daily_df.empty else pd.DataFrame()
    raise ValueError(f"unsupported filter level: {level}")


def build_daily_gate_label(zone: str, relation: str) -> str:
    if zone == "ABOVE" and relation == "ABOVE":
        return "水上绿"
    if zone == "ABOVE" and relation == "BELOW":
        return "水上红"
    if zone == "BELOW" and relation == "ABOVE":
        return "水下绿"
    return "水下红"


def flip_state(value: str) -> str:
    return "ABOVE" if value == "BELOW" else "BELOW"


def resolve_zone(dif: float, previous_zone: str | None) -> str:
    if dif > 0:
        return "ABOVE"
    if dif < 0:
        return "BELOW"
    return flip_state(previous_zone) if previous_zone else "BELOW"


def resolve_relation(dif: float, dea: float, previous_relation: str | None) -> str:
    if dif > dea:
        return "ABOVE"
    if dif < dea:
        return "BELOW"
    return flip_state(previous_relation) if previous_relation else "BELOW"


def higher_period_key(dt: pd.Timestamp, level: str) -> Any:
    ts = pd.Timestamp(dt)
    if level in {"daily", "240m"}:
        return ts.normalize()
    if level == "weekly":
        return ts.to_period("W-FRI")
    raise ValueError(f"unsupported higher level: {level}")


def build_projected_gate_map(signal_df: pd.DataFrame, higher_df: pd.DataFrame, higher_level: str) -> dict[pd.Timestamp, DailyGateState]:
    if signal_df.empty or higher_df.empty:
        return {}
    higher_bars = higher_df.sort_values("date").reset_index(drop=True)
    higher_states: list[dict[str, Any]] = []

    ema_fast: float | None = None
    ema_slow: float | None = None
    ema_signal: float | None = None
    for _, bar in higher_bars.iterrows():
        close_price = float(bar["close"])
        if ema_fast is None:
            ema_fast = close_price
            ema_slow = close_price
            ema_signal = 0.0
        else:
            ema_fast = close_price * MACD_FAST_ALPHA + ema_fast * (1 - MACD_FAST_ALPHA)
            ema_slow = close_price * MACD_SLOW_ALPHA + ema_slow * (1 - MACD_SLOW_ALPHA)
            dif = ema_fast - ema_slow
            ema_signal = dif * MACD_SIGNAL_ALPHA + float(ema_signal) * (1 - MACD_SIGNAL_ALPHA)
        higher_states.append(
            {
                "period_key": higher_period_key(pd.Timestamp(bar["date"]), higher_level),
                "ema_fast": float(ema_fast),
                "ema_slow": float(ema_slow),
                "ema_signal": float(ema_signal),
            }
        )

    completed_by_period = {row["period_key"]: row for row in higher_states}
    ordered_periods = [row["period_key"] for row in higher_states]
    previous_period_map = {ordered_periods[idx]: ordered_periods[idx - 1] if idx > 0 else None for idx in range(len(ordered_periods))}

    state_map: dict[pd.Timestamp, DailyGateState] = {}
    previous_zone: str | None = None
    previous_relation: str | None = None

    for _, bar in signal_df.sort_values("date").iterrows():
        bar_dt = pd.Timestamp(bar["date"])
        current_period = higher_period_key(bar_dt, higher_level)
        base_period = previous_period_map.get(current_period)
        if base_period is None:
            continue
        base_state = completed_by_period.get(base_period)
        if base_state is None:
            continue

        close_price = float(bar["close"])
        current_fast = close_price * MACD_FAST_ALPHA + float(base_state["ema_fast"]) * (1 - MACD_FAST_ALPHA)
        current_slow = close_price * MACD_SLOW_ALPHA + float(base_state["ema_slow"]) * (1 - MACD_SLOW_ALPHA)
        dif = current_fast - current_slow
        dea = dif * MACD_SIGNAL_ALPHA + float(base_state["ema_signal"]) * (1 - MACD_SIGNAL_ALPHA)

        zone = resolve_zone(dif, previous_zone)
        relation = resolve_relation(dif, dea, previous_relation)
        label = build_daily_gate_label(zone, relation)
        blocked_buy = zone == "BELOW" and relation == "BELOW"
        blocked_sell = zone == "ABOVE" and relation == "ABOVE"

        state_map[bar_dt] = DailyGateState(
            dif=dif,
            dea=dea,
            zone=zone,
            relation=relation,
            label=label,
            allow_buy=not blocked_buy,
            allow_sell=not blocked_sell,
            buy_reason="日线处于水下红，逆势一买禁做" if blocked_buy else "日线DIF/DEA权限放行",
            sell_reason="日线处于水上绿，逆势一卖禁做" if blocked_sell else "日线DIF/DEA权限放行",
        )
        previous_zone = zone
        previous_relation = relation
    return state_map


def build_projected_transition_map(signal_df: pd.DataFrame, higher_df: pd.DataFrame, higher_level: str) -> dict[pd.Timestamp, TransitionState]:
    if signal_df.empty or higher_df.empty:
        return {}
    higher_ind = compute_indicators(higher_df, ScanConfig()).sort_values("date").reset_index(drop=True)
    higher_records = [
        {
            "period_key": higher_period_key(pd.Timestamp(row["date"]), higher_level),
            "close": float(row["close"]),
            "hist": float(row["bar"]),
            "dif": float(row["dif"]),
            "dea": float(row["dea"]),
        }
        for _, row in higher_ind.iterrows()
    ]
    completed_chain: list[dict[str, Any]] = []
    ema_fast: float | None = None
    ema_slow: float | None = None
    ema_signal: float | None = None
    for row in higher_records:
        close_price = float(row["close"])
        if ema_fast is None:
            ema_fast = close_price
            ema_slow = close_price
            ema_signal = 0.0
        else:
            ema_fast = close_price * MACD_FAST_ALPHA + ema_fast * (1 - MACD_FAST_ALPHA)
            ema_slow = close_price * MACD_SLOW_ALPHA + ema_slow * (1 - MACD_SLOW_ALPHA)
            dif = ema_fast - ema_slow
            ema_signal = dif * MACD_SIGNAL_ALPHA + float(ema_signal) * (1 - MACD_SIGNAL_ALPHA)
        dif = float(ema_fast) - float(ema_slow)
        dea = float(ema_signal)
        completed_chain.append(
            {
                "period_key": row["period_key"],
                "ema_fast": float(ema_fast),
                "ema_slow": float(ema_slow),
                "ema_signal": float(ema_signal),
                "hist": dif - dea,
            }
        )

    completed_by_period = {row["period_key"]: row for row in completed_chain}
    ordered_periods = [row["period_key"] for row in completed_chain]
    previous_period_map = {ordered_periods[idx]: ordered_periods[idx - 1] if idx > 0 else None for idx in range(len(ordered_periods))}

    state_map: dict[pd.Timestamp, TransitionState] = {}
    for _, bar in signal_df.sort_values("date").iterrows():
        bar_dt = pd.Timestamp(bar["date"])
        current_period = higher_period_key(bar_dt, higher_level)
        previous_period = previous_period_map.get(current_period)
        if previous_period is None:
            continue
        previous_state = completed_by_period.get(previous_period)
        if previous_state is None:
            continue

        close_price = float(bar["close"])
        current_fast = close_price * MACD_FAST_ALPHA + float(previous_state["ema_fast"]) * (1 - MACD_FAST_ALPHA)
        current_slow = close_price * MACD_SLOW_ALPHA + float(previous_state["ema_slow"]) * (1 - MACD_SLOW_ALPHA)
        current_dif = current_fast - current_slow
        current_dea = current_dif * MACD_SIGNAL_ALPHA + float(previous_state["ema_signal"]) * (1 - MACD_SIGNAL_ALPHA)
        current_hist = current_dif - current_dea
        previous_hist = float(previous_state["hist"])

        current_abs = abs(current_hist)
        prev_abs = abs(previous_hist)
        same_sign = current_hist * previous_hist > 0
        lightening = same_sign and current_abs < prev_abs
        deepening = same_sign and current_abs > prev_abs
        flip_to_green = previous_hist < 0 < current_hist
        flip_to_red = previous_hist > 0 > current_hist

        buy_phase = (
            "red_lightening"
            if lightening and current_hist < 0
            else "first_green"
            if flip_to_green
            else "green_deepening"
            if current_hist > 0 and previous_hist > 0 and deepening
            else None
        )
        sell_phase = (
            "green_lightening"
            if lightening and current_hist > 0
            else "first_red"
            if flip_to_red
            else "red_deepening"
            if current_hist < 0 and previous_hist < 0 and deepening
            else None
        )
        state_map[bar_dt] = TransitionState(
            allow_buy=bool(buy_phase),
            allow_sell=bool(sell_phase),
            buy_phase=buy_phase,
            sell_phase=sell_phase,
            hist=current_hist,
            previous_hist=previous_hist,
        )
    return state_map


def scaled_max_hold_bars(base_bars_15m: int, signal_level: str) -> int:
    base_minutes = base_bars_15m * 15
    level_minutes = SIGNAL_LEVEL_TO_MINUTES[signal_level]
    return max(1, round(base_minutes / level_minutes))


def resolve_market_entry(signal_row: pd.Series, signal_idx: int, price_df: pd.DataFrame, atr_series: pd.Series, stop_buffer_atr: float) -> EntryPlan | None:
    atr_value = float(atr_series.iloc[signal_idx]) if signal_idx < len(atr_series) else float("nan")
    if not pd.notna(atr_value) or atr_value <= 0:
        return None

    entry_idx = signal_idx + 1
    if entry_idx >= len(price_df):
        return None

    buy1_low = pd.to_numeric(pd.Series([signal_row.get("buy1_low")]), errors="coerce").iloc[0]
    if not pd.notna(buy1_low):
        return None

    entry_bar = price_df.iloc[entry_idx]
    entry_price = float(entry_bar["open"])
    stop_price = float(buy1_low) - stop_buffer_atr * atr_value
    risk_distance = entry_price - stop_price
    if not pd.notna(stop_price) or risk_distance <= 0:
        return None

    return EntryPlan(
        entry_idx=entry_idx,
        entry_time=pd.Timestamp(entry_bar["date"]),
        entry_price=entry_price,
        entry_method="next_open_market",
        atr_value=atr_value,
        stop_price=stop_price,
        risk_distance=risk_distance,
    )


def filter_family1_signals(
    signals: pd.DataFrame,
    signal_code: str,
    side: str,
    date_to_index: dict[pd.Timestamp, int],
    atr_series: pd.Series,
    gate_map: dict[pd.Timestamp, DailyGateState],
    transition_map: dict[pd.Timestamp, TransitionState],
    signal_level: str,
    gate_level: str,
    transition_level: str,
) -> tuple[pd.DataFrame, dict[str, int]]:
    filtered = signals[signals["signal"] == signal_code].sort_values("date").reset_index(drop=True)
    counts = {
        "raw": int(len(filtered)),
        "pass_gate": 0,
        "pass_transition": 0,
        "eligible": 0,
    }
    if filtered.empty:
        return pd.DataFrame(), counts

    candidate_rows: list[dict[str, Any]] = []
    for _, signal_row in filtered.iterrows():
        signal_dt = pd.Timestamp(signal_row["date"])
        signal_idx = date_to_index.get(signal_dt)
        if signal_idx is None:
            continue

        gate = gate_map.get(signal_dt)
        gate_allowed = gate.allow_buy if gate is not None and side == "buy" else gate.allow_sell if gate is not None else True
        if gate is not None and not gate_allowed:
            continue
        counts["pass_gate"] += 1

        transition = transition_map.get(signal_dt)
        transition_allowed = transition.allow_buy if transition is not None and side == "buy" else transition.allow_sell if transition is not None else False
        if transition is None or not transition_allowed:
            continue
        counts["pass_transition"] += 1

        atr_value = float(atr_series.iloc[signal_idx]) if signal_idx < len(atr_series) else float("nan")
        if not pd.notna(atr_value) or atr_value <= 0:
            continue

        transition_phase = transition.buy_phase if side == "buy" else transition.sell_phase
        gate_reason = gate.buy_reason if gate is not None and side == "buy" else gate.sell_reason if gate is not None else None

        candidate = signal_row.to_dict()
        candidate.update(
            {
                "signal_idx": int(signal_idx),
                "atr_value": round(atr_value, 6),
                "daily_gate_label": gate.label if gate is not None else None,
                "daily_gate_reason": gate_reason,
                "transition_phase": transition_phase,
                "transition_hist": round(float(transition.hist), 6),
                "transition_previous_hist": round(float(transition.previous_hist), 6),
                "four_hour_transition_phase": transition_phase,
                "four_hour_hist": round(float(transition.hist), 6),
                "four_hour_previous_hist": round(float(transition.previous_hist), 6),
                "signal_level": signal_level,
                "gate_level": gate_level,
                "transition_level": transition_level,
            }
        )
        candidate_rows.append(candidate)
        counts["eligible"] += 1

    candidates_df = pd.DataFrame(candidate_rows)
    if not candidates_df.empty:
        candidates_df = candidates_df.sort_values("date").reset_index(drop=True)
    return candidates_df, counts


def simulate_market_reverse_trade(
    signal_row: pd.Series,
    price_df: pd.DataFrame,
    entry_plan: EntryPlan,
    reverse_signal_map: dict[pd.Timestamp, dict[str, Any]],
    max_hold_bars: int,
    last_exit_dt: pd.Timestamp | None,
) -> dict[str, Any] | None:
    if price_df.empty or entry_plan.entry_idx >= len(price_df):
        return None

    last_eval_idx = min(len(price_df) - 1, entry_plan.entry_idx + max_hold_bars - 1)
    trigger_idx = last_eval_idx
    trigger_reason = "TIME"
    trigger_price = float(price_df.iloc[last_eval_idx]["close"])
    reverse_signal: dict[str, Any] | None = None

    for idx in range(entry_plan.entry_idx, last_eval_idx + 1):
        bar = price_df.iloc[idx]
        bar_dt = pd.Timestamp(bar["date"])
        if float(bar["low"]) <= entry_plan.stop_price:
            trigger_idx = idx
            trigger_reason = "STRUCT_STOP"
            trigger_price = entry_plan.stop_price
            break

        reverse_candidate = reverse_signal_map.get(bar_dt)
        if reverse_candidate is not None:
            trigger_idx = idx
            trigger_reason = f"REV_{reverse_candidate['signal']}"
            trigger_price = float(reverse_candidate["close"])
            reverse_signal = reverse_candidate
            break

        if idx == last_eval_idx:
            trigger_idx = idx
            trigger_reason = "TIME"
            trigger_price = float(bar["close"])
            break

    fill_idx = trigger_idx + 1 if trigger_idx + 1 < len(price_df) else trigger_idx
    fill_bar = price_df.iloc[fill_idx]
    if fill_idx > trigger_idx:
        exit_price = float(fill_bar["open"])
        exit_dt = pd.Timestamp(fill_bar["date"])
    else:
        exit_price = float(fill_bar["close"])
        exit_dt = pd.Timestamp(fill_bar["date"])

    gap_exit = int(
        fill_idx > trigger_idx
        and trigger_reason == "STRUCT_STOP"
        and exit_price < trigger_price
    )
    exit_reason = f"{trigger_reason}_GAP" if gap_exit else trigger_reason

    entry_dt = pd.Timestamp(entry_plan.entry_time)
    trade_return = (exit_price - entry_plan.entry_price) / entry_plan.entry_price
    flat_days = elapsed_days(last_exit_dt, entry_dt) if last_exit_dt is not None else None

    return {
        "level": str(signal_row.get("signal_level", "15m")),
        "strategy": "family1_research_enhanced_a_share",
        "symbol": str(signal_row["symbol"]),
        "name": str(signal_row["name"]),
        "signal": str(signal_row["signal"]),
        "signal_date": pd.Timestamp(signal_row["date"]),
        "signal_price": round(float(signal_row["close"]), 6),
        "score": round(float(signal_row["score"]), 6),
        "entry_date": entry_dt,
        "entry_price": round(float(entry_plan.entry_price), 6),
        "entry_method": entry_plan.entry_method,
        "stop_price": round(float(entry_plan.stop_price), 6),
        "exit_date": exit_dt,
        "exit_price": round(float(exit_price), 6),
        "exit_reason": exit_reason,
        "hold_bars": int(fill_idx - entry_plan.entry_idx + 1),
        "hold_days": round(elapsed_days(entry_dt, exit_dt), 6),
        "flat_days_before_entry": round(flat_days, 6) if flat_days is not None else None,
        "ret": round(float(trade_return), 6),
        "win": int(trade_return > 0),
        "atr_value": round(float(entry_plan.atr_value), 6),
        "risk_distance": round(float(entry_plan.risk_distance), 6),
        "trigger_date": pd.Timestamp(price_df.iloc[trigger_idx]["date"]),
        "trigger_price": round(float(trigger_price), 6),
        "reverse_signal_date": pd.Timestamp(reverse_signal["date"]) if reverse_signal is not None else None,
        "reverse_signal_price": round(float(reverse_signal["close"]), 6) if reverse_signal is not None else None,
        "reverse_daily_gate_label": reverse_signal.get("daily_gate_label") if reverse_signal is not None else None,
        "reverse_daily_gate_reason": reverse_signal.get("daily_gate_reason") if reverse_signal is not None else None,
        "reverse_transition_phase": reverse_signal.get("transition_phase") if reverse_signal is not None else None,
        "reverse_transition_hist": reverse_signal.get("transition_hist") if reverse_signal is not None else None,
        "reverse_transition_previous_hist": reverse_signal.get("transition_previous_hist") if reverse_signal is not None else None,
        "gap_exit": gap_exit,
        "exit_idx": int(fill_idx),
    }


def summarize_symbol_results(trades: pd.DataFrame, stats_rows: list[dict[str, Any]]) -> pd.DataFrame:
    base = pd.DataFrame(stats_rows)
    if base.empty:
        return base
    if trades.empty:
        for col in ["trades", "win_rate", "avg_ret", "avg_hold_days", "avg_flat_days"]:
            base[col] = 0 if col == "trades" else None
        return base

    metrics = (
        trades.groupby(["symbol", "name"], as_index=False)
        .agg(
            trades=("ret", "count"),
            win_rate=("win", "mean"),
            avg_ret=("ret", "mean"),
            avg_hold_days=("hold_days", "mean"),
            avg_flat_days=("flat_days_before_entry", "mean"),
        )
    )
    out = base.merge(metrics, on=["symbol", "name"], how="left")
    out["trades"] = out["trades"].fillna(0).astype(int)
    for col in ["win_rate", "avg_ret", "avg_hold_days", "avg_flat_days"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")
        out[col] = out[col].round(6)
    return out


def max_drawdown_from_trade_returns(trades: pd.DataFrame) -> float | None:
    if trades.empty:
        return None
    equity = 1.0
    peak = 1.0
    max_drawdown = 0.0
    for trade_return in trades.sort_values(["exit_date", "entry_date"])["ret"].tolist():
        equity *= 1.0 + float(trade_return)
        peak = max(peak, equity)
        if peak > 0:
            max_drawdown = max(max_drawdown, (peak - equity) / peak)
    return max_drawdown


def summarize_results(
    trades: pd.DataFrame,
    symbol_summary: pd.DataFrame,
    stats_rows: list[dict[str, Any]],
    sample_years: float,
    universe_size: int,
    universe_label: str,
    signal_level: str = "15m",
    gate_level: str = "daily",
    transition_level: str = "240m",
) -> pd.DataFrame:
    totals = pd.DataFrame(stats_rows) if stats_rows else pd.DataFrame()
    raw_buy1 = int(totals["raw_buy1"].sum()) if not totals.empty else 0
    pass_daily_gate = int(totals["pass_daily_gate"].sum()) if not totals.empty else 0
    pass_transition_gate = int(totals["pass_transition_gate"].sum()) if not totals.empty else 0
    eligible_entries = int(totals["eligible_entries"].sum()) if not totals.empty else 0
    raw_sell1 = int(totals["raw_sell1"].sum()) if not totals.empty else 0
    pass_reverse_gate = int(totals["pass_reverse_gate"].sum()) if not totals.empty else 0
    pass_reverse_transition = int(totals["pass_reverse_transition"].sum()) if not totals.empty else 0
    eligible_reverse_exits = int(totals["eligible_reverse_exits"].sum()) if not totals.empty else 0

    row: dict[str, Any] = {
        "level": signal_level,
        "strategy": "family1_research_enhanced_a_share",
        "universe": universe_label,
        "symbols": universe_size,
        "symbols_with_trades": int(symbol_summary["trades"].gt(0).sum()) if not symbol_summary.empty else 0,
        "raw_buy1_signals": raw_buy1,
        "pass_daily_gate": pass_daily_gate,
        "pass_transition_gate": pass_transition_gate,
        "eligible_entries": eligible_entries,
        "raw_sell1_signals": raw_sell1,
        "pass_reverse_gate": pass_reverse_gate,
        "pass_reverse_transition": pass_reverse_transition,
        "eligible_reverse_exits": eligible_reverse_exits,
        "trades": int(len(trades)),
        "sample_years": round(sample_years, 4),
        "annual_trades": round(float(len(trades)) / sample_years, 4) if sample_years > 0 else None,
        "annual_trades_per_symbol": round(float(len(trades)) / sample_years / max(universe_size, 1), 6) if sample_years > 0 else None,
        "signal_level": signal_level,
        "gate_level": gate_level,
        "transition_level": transition_level,
    }

    if trades.empty:
        return pd.DataFrame([row])

    if "signal_level" in trades.columns:
        row["signal_level"] = trades["signal_level"].iloc[0]
    if "gate_level" in trades.columns:
        row["gate_level"] = trades["gate_level"].iloc[0]
    if "transition_level" in trades.columns:
        row["transition_level"] = trades["transition_level"].iloc[0]

    valid_flat = trades.dropna(subset=["flat_days_before_entry"])
    row.update(
        {
            "win_rate": round(float(trades["win"].mean()), 6),
            "avg_ret": round(float(trades["ret"].mean()), 6),
            "median_ret": round(float(trades["ret"].median()), 6),
            "avg_hold_bars": round(float(trades["hold_bars"].mean()), 4),
            "avg_hold_days": round(float(trades["hold_days"].mean()), 4),
            "avg_flat_days": round(float(valid_flat["flat_days_before_entry"].mean()), 4) if not valid_flat.empty else None,
            "next_open_market_rate": round(float((trades["entry_method"] == "next_open_market").mean()), 6),
            "struct_stop_rate": round(float(trades["exit_reason"].isin(["STRUCT_STOP", "STRUCT_STOP_GAP"]).mean()), 6),
            "reverse_sell1_rate": round(float((trades["exit_reason"] == "REV_SELL1").mean()), 6),
            "time_exit_rate": round(float((trades["exit_reason"] == "TIME").mean()), 6),
            "gap_exit_rate": round(float(trades.get("gap_exit", pd.Series(dtype=float)).mean()), 6) if "gap_exit" in trades.columns else None,
            "max_drawdown": round(float(max_drawdown_from_trade_returns(trades) or 0.0), 6),
        }
    )
    return pd.DataFrame([row])


def prepare_family1_symbol_candidates(
    symbol: str,
    name: str,
    market_store: MarketDataStore,
    cfg: ScanConfig,
    signal_level: str = "15m",
    gate_level: str = "daily",
    transition_level: str = "240m",
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    signal_df = load_signal_level_data(market_store, symbol, signal_level, cfg)
    if signal_df.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {
            "symbol": symbol,
            "name": name,
            "raw_buy1": 0,
            "pass_daily_gate": 0,
            "pass_transition_gate": 0,
            "eligible_entries": 0,
            "raw_sell1": 0,
            "pass_reverse_gate": 0,
            "pass_reverse_transition": 0,
            "eligible_reverse_exits": 0,
        }

    gate_df = load_filter_level_data(market_store, symbol, gate_level, cfg, signal_df, signal_level)
    transition_df = gate_df if transition_level == gate_level else load_filter_level_data(market_store, symbol, transition_level, cfg, signal_df, signal_level)

    indicator_df = compute_indicators(signal_df, cfg).reset_index(drop=True)
    engine = MACDTimeSignalEngine(cfg)
    signals = engine.scan_dataframe(
        indicator_df[["date", "open", "high", "low", "close", "volume", "amount", "turnover"]],
        symbol=symbol,
        name=name,
    )
    if signals.empty:
        return indicator_df, pd.DataFrame(), pd.DataFrame(), {
            "symbol": symbol,
            "name": name,
            "raw_buy1": 0,
            "pass_daily_gate": 0,
            "pass_transition_gate": 0,
            "eligible_entries": 0,
            "raw_sell1": 0,
            "pass_reverse_gate": 0,
            "pass_reverse_transition": 0,
            "eligible_reverse_exits": 0,
        }

    stats = {
        "symbol": symbol,
        "name": name,
        "raw_buy1": 0,
        "pass_daily_gate": 0,
        "pass_transition_gate": 0,
        "eligible_entries": 0,
        "raw_sell1": 0,
        "pass_reverse_gate": 0,
        "pass_reverse_transition": 0,
        "eligible_reverse_exits": 0,
    }

    date_to_index = {pd.Timestamp(row["date"]): idx for idx, row in indicator_df.iterrows()}
    daily_gate_map = build_projected_gate_map(indicator_df, gate_df, gate_level) if not gate_df.empty else {}
    transition_map = build_projected_transition_map(indicator_df, transition_df, transition_level) if not transition_df.empty else {}
    atr_series = indicator_df["atr"].reset_index(drop=True)
    candidates_df, buy_counts = filter_family1_signals(
        signals=signals,
        signal_code=BUY1_SIGNAL,
        side="buy",
        date_to_index=date_to_index,
        atr_series=atr_series,
        gate_map=daily_gate_map,
        transition_map=transition_map,
        signal_level=signal_level,
        gate_level=gate_level,
        transition_level=transition_level,
    )
    reverse_candidates_df, sell_counts = filter_family1_signals(
        signals=signals,
        signal_code=SELL1_SIGNAL,
        side="sell",
        date_to_index=date_to_index,
        atr_series=atr_series,
        gate_map=daily_gate_map,
        transition_map=transition_map,
        signal_level=signal_level,
        gate_level=gate_level,
        transition_level=transition_level,
    )
    stats.update(
        {
            "raw_buy1": buy_counts["raw"],
            "pass_daily_gate": buy_counts["pass_gate"],
            "pass_transition_gate": buy_counts["pass_transition"],
            "eligible_entries": buy_counts["eligible"],
            "raw_sell1": sell_counts["raw"],
            "pass_reverse_gate": sell_counts["pass_gate"],
            "pass_reverse_transition": sell_counts["pass_transition"],
            "eligible_reverse_exits": sell_counts["eligible"],
        }
    )
    return indicator_df, candidates_df, reverse_candidates_df, stats


def backtest_symbol(
    symbol: str,
    name: str,
    market_store: MarketDataStore,
    cfg: ScanConfig,
    max_hold_bars: int,
    stop_buffer_atr: float,
    signal_level: str,
    gate_level: str,
    transition_level: str,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    indicator_df, candidates_df, reverse_candidates_df, stats = prepare_family1_symbol_candidates(
        symbol=symbol,
        name=name,
        market_store=market_store,
        cfg=cfg,
        signal_level=signal_level,
        gate_level=gate_level,
        transition_level=transition_level,
    )
    if indicator_df.empty or candidates_df.empty:
        return pd.DataFrame(), stats

    atr_series = indicator_df["atr"].reset_index(drop=True)
    reverse_signal_map = {
        pd.Timestamp(row["date"]): row
        for _, row in reverse_candidates_df.iterrows()
    }

    trades: list[dict[str, Any]] = []
    active_until_idx = -1
    last_exit_dt: pd.Timestamp | None = None

    for _, signal_row in candidates_df.iterrows():
        signal_dt = pd.Timestamp(signal_row["date"])
        signal_idx = int(signal_row["signal_idx"])
        if signal_idx <= active_until_idx:
            continue

        entry_plan = resolve_market_entry(signal_row, signal_idx, indicator_df, atr_series, stop_buffer_atr)
        if entry_plan is None:
            continue

        trade = simulate_market_reverse_trade(
            signal_row=signal_row,
            price_df=indicator_df,
            entry_plan=entry_plan,
            reverse_signal_map=reverse_signal_map,
            max_hold_bars=max_hold_bars,
            last_exit_dt=last_exit_dt,
        )
        if trade is None:
            continue

        trade["daily_gate_label"] = signal_row.get("daily_gate_label")
        trade["daily_gate_reason"] = signal_row.get("daily_gate_reason")
        trade["transition_phase"] = signal_row.get("transition_phase", signal_row.get("four_hour_transition_phase"))
        trade["transition_hist"] = signal_row.get("transition_hist", signal_row.get("four_hour_hist"))
        trade["transition_previous_hist"] = signal_row.get("transition_previous_hist", signal_row.get("four_hour_previous_hist"))
        trade["four_hour_transition_phase"] = signal_row.get("four_hour_transition_phase", signal_row.get("transition_phase"))
        trade["four_hour_hist"] = signal_row.get("four_hour_hist", signal_row.get("transition_hist"))
        trade["four_hour_previous_hist"] = signal_row.get("four_hour_previous_hist", signal_row.get("transition_previous_hist"))
        trade["signal_level"] = signal_row.get("signal_level", signal_level)
        trade["gate_level"] = signal_row.get("gate_level", gate_level)
        trade["transition_level"] = signal_row.get("transition_level", transition_level)
        trades.append(trade)

        active_until_idx = int(trade["exit_idx"])
        last_exit_dt = pd.Timestamp(trade["exit_date"])

    trades_df = pd.DataFrame(trades)
    if not trades_df.empty and "exit_idx" in trades_df.columns:
        trades_df = trades_df.drop(columns=["exit_idx"])
    return trades_df, stats


def main() -> None:
    args = parse_args()
    cfg = build_scan_config(args)
    market_store = MarketDataStore(args.db_path)
    backtest_store = BacktestStore(args.backtest_db_path)

    universe, universe_label = load_universe(args, market_store)
    if universe.empty:
        raise SystemExit("universe is empty")

    effective_end = min(pd.Timestamp(args.end_date).normalize(), pd.Timestamp.now().normalize())
    sample_years = max(elapsed_days(pd.Timestamp(args.start_date).normalize(), effective_end) / 365.25, 1e-9)
    effective_max_hold_bars = scaled_max_hold_bars(args.max_hold_bars, args.signal_level)

    trade_records: list[dict[str, Any]] = []
    stats_rows: list[dict[str, Any]] = []
    total_symbols = len(universe)

    for idx, (_, row) in enumerate(universe.iterrows(), start=1):
        symbol = str(row["symbol"])
        name = str(row.get("name", ""))
        print(f"[{idx}/{total_symbols}] family1 research enhanced -> {symbol}")
        trades_df, stats = backtest_symbol(
            symbol=symbol,
            name=name,
            market_store=market_store,
            cfg=cfg,
            max_hold_bars=effective_max_hold_bars,
            stop_buffer_atr=args.stop_buffer_atr,
            signal_level=args.signal_level,
            gate_level=args.gate_level,
            transition_level=args.transition_level,
        )
        stats_rows.append(stats)
        if not trades_df.empty:
            trade_records.extend(trades_df.to_dict("records"))
        print(f"[{idx}/{total_symbols}] done {symbol} trades={len(trades_df)}")

    all_trades = pd.DataFrame(trade_records) if trade_records else pd.DataFrame()
    symbol_summary = summarize_symbol_results(all_trades, stats_rows)
    summary = summarize_results(
        all_trades,
        symbol_summary,
        stats_rows,
        sample_years,
        total_symbols,
        universe_label,
        signal_level=args.signal_level,
        gate_level=args.gate_level,
        transition_level=args.transition_level,
    )

    output_path = Path(args.output).expanduser().resolve()
    summary_path = Path(args.summary_output).expanduser().resolve()
    symbol_summary_path = Path(args.symbol_summary_output).expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    symbol_summary_path.parent.mkdir(parents=True, exist_ok=True)

    all_trades.to_csv(output_path, index=False, encoding="utf-8-sig")
    summary.to_csv(summary_path, index=False, encoding="utf-8-sig")
    symbol_summary.to_csv(symbol_summary_path, index=False, encoding="utf-8-sig")

    backtest_store.save_backtest_run(
        created_at=pd.Timestamp.now().isoformat(),
        start_date=args.start_date,
        end_date=args.end_date,
        levels=[args.signal_level],
        symbols=[str(item) for item in universe["symbol"].tolist()],
        exit_mode="family1_research_enhanced_a_share_rev_sell1",
        minute_source="market_data_sqlite",
        output_path=str(output_path),
        summary_path=str(summary_path),
        trades_df=all_trades,
        summary_df=summary,
    )

    market_store.close()
    backtest_store.close()

    print(f"family1_research_enhanced_a_share_backtest v{PROJECT_VERSION}")
    print(f"saved trades -> {output_path}")
    print(f"saved summary -> {summary_path}")
    print(f"saved symbol summary -> {symbol_summary_path}")
    if not summary.empty:
        with pd.option_context("display.max_columns", 50, "display.width", 200):
            print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
