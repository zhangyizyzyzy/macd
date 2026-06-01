#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import backtrader as bt
import pandas as pd

from backtest_store import BacktestStore
from macd_family1_research_enhanced_backtest import (
    PROJECT_VERSION,
    build_scan_config,
    load_universe,
    normalize_filter_level,
    normalize_signal_level,
    prepare_family1_symbol_candidates,
    scaled_max_hold_bars,
    summarize_results,
    summarize_symbol_results,
)
from macd_timeframe_backtest import elapsed_days
from market_data_store import MarketDataStore


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backtrader A-share family1 research-enhanced backtest on HS300")
    parser.add_argument("--start-date", default="20240101")
    parser.add_argument("--end-date", default="20300101")
    parser.add_argument("--adjust", default="qfq", choices=["", "qfq", "hfq"])
    parser.add_argument("--symbols", default="", help="Optional comma-separated symbols for smoke tests")
    parser.add_argument("--limit", type=int, default=None, help="Optional universe cap after HS300 selection")
    parser.add_argument("--db-path", default=str(Path("data") / "market_data.sqlite"))
    parser.add_argument("--backtest-db-path", default=str(Path("outputs") / "backtest_store.sqlite"))
    parser.add_argument("--hs300-cache", default=str(Path("data") / "hs300_current.csv"))
    parser.add_argument("--output", default=str(Path("outputs") / "family1_research_enhanced_a_share_bt_trades.csv"))
    parser.add_argument("--summary-output", default=str(Path("outputs") / "family1_research_enhanced_a_share_bt_summary.csv"))
    parser.add_argument("--symbol-summary-output", default=str(Path("outputs") / "family1_research_enhanced_a_share_bt_symbols.csv"))
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


class Family1ResearchEnhancedData(bt.feeds.PandasData):
    lines = ("amount", "turnover")
    params = (
        ("datetime", None),
        ("open", "open"),
        ("high", "high"),
        ("low", "low"),
        ("close", "close"),
        ("volume", "volume"),
        ("openinterest", -1),
        ("amount", "amount"),
        ("turnover", "turnover"),
    )


def to_bt_datetime(value: Any) -> pd.Timestamp:
    dt = pd.Timestamp(value)
    if dt.tzinfo is not None:
        dt = dt.tz_convert(None)
    return dt.tz_localize(None) if dt.tzinfo is not None else dt


class Family1ResearchEnhancedStrategy(bt.Strategy):
    params = dict(
        symbol="",
        name="",
        signal_rows=None,
        reverse_signal_rows=None,
        stop_buffer_atr=0.2,
        max_hold_bars=80,
    )

    def __init__(self) -> None:
        self.buy_signal_map = {
            to_bt_datetime(row["date"]): row
            for row in (self.p.signal_rows or [])
        }
        self.reverse_signal_map = {
            to_bt_datetime(row["date"]): row
            for row in (self.p.reverse_signal_rows or [])
        }
        self.pending_signal: dict[str, Any] | None = None
        self.position_state: dict[str, Any] | None = None
        self.trade_records: list[dict[str, Any]] = []
        self.last_exit_dt: pd.Timestamp | None = None

    def next(self) -> None:
        bar_dt = to_bt_datetime(bt.num2date(self.datas[0].datetime[0]))
        open_price = float(self.datas[0].open[0])
        low_price = float(self.datas[0].low[0])
        close_price = float(self.datas[0].close[0])
        bar_number = len(self)

        if self.position_state is not None and self.position_state.get("queued_exit_reason"):
            queued_reason = str(self.position_state["queued_exit_reason"])
            trigger_price = float(self.position_state["queued_exit_trigger_price"])
            gap_exit = int(queued_reason == "STRUCT_STOP" and open_price < trigger_price)
            exit_reason = f"{queued_reason}_GAP" if gap_exit else queued_reason
            self._close_position(
                open_price,
                bar_dt,
                exit_reason,
                bar_number,
                trigger_dt=self.position_state.get("queued_exit_trigger_dt"),
                trigger_price=trigger_price,
                reverse_signal=self.position_state.get("queued_exit_reverse_signal"),
                gap_exit=gap_exit,
            )
            return

        if self.position_state is None and self.pending_signal is not None:
            if bar_number >= self.datas[0].buflen():
                self.pending_signal = None
                return
            signal = self.pending_signal
            buy1_low = pd.to_numeric(pd.Series([signal.get("buy1_low")]), errors="coerce").iloc[0]
            atr_value = float(signal["atr_value"])
            stop_price = float(buy1_low) - self.p.stop_buffer_atr * atr_value if pd.notna(buy1_low) else float("nan")
            risk_distance = open_price - stop_price
            self.pending_signal = None
            if not pd.notna(stop_price) or risk_distance <= 0:
                return
            self.position_state = {
                "signal": signal,
                "entry_bar": bar_number,
                "last_eval_bar": bar_number + self.p.max_hold_bars - 1,
                "entry_dt": bar_dt,
                "entry_price": open_price,
                "entry_method": "next_open_market",
                "atr_value": atr_value,
                "stop_price": stop_price,
                "risk_distance": risk_distance,
                "queued_exit_reason": None,
                "queued_exit_trigger_dt": None,
                "queued_exit_trigger_price": None,
                "queued_exit_reverse_signal": None,
                "flat_days_before_entry": round(elapsed_days(self.last_exit_dt, bar_dt), 6) if self.last_exit_dt is not None else None,
            }

        if self.position_state is not None:
            stop_price = float(self.position_state["stop_price"])
            if low_price <= stop_price:
                self._queue_exit("STRUCT_STOP", bar_dt, stop_price)
            else:
                reverse_signal = self.reverse_signal_map.get(bar_dt)
                if reverse_signal is not None:
                    self._queue_exit(
                        f"REV_{reverse_signal['signal']}",
                        bar_dt,
                        float(reverse_signal["close"]),
                        reverse_signal=reverse_signal,
                    )
                elif bar_number >= int(self.position_state["last_eval_bar"]):
                    self._queue_exit("TIME", bar_dt, close_price)

        if self.position_state is None and self.pending_signal is None:
            signal = self.buy_signal_map.get(bar_dt)
            if signal is not None:
                self.pending_signal = signal

    def stop(self) -> None:
        if self.position_state is not None:
            bar_dt = to_bt_datetime(bt.num2date(self.datas[0].datetime[0]))
            close_price = float(self.datas[0].close[0])
            exit_reason = str(self.position_state.get("queued_exit_reason") or "TIME")
            self._close_position(
                close_price,
                bar_dt,
                exit_reason,
                len(self),
                trigger_dt=self.position_state.get("queued_exit_trigger_dt") or bar_dt,
                trigger_price=float(self.position_state.get("queued_exit_trigger_price") or close_price),
                reverse_signal=self.position_state.get("queued_exit_reverse_signal"),
                gap_exit=0,
            )

    def _queue_exit(
        self,
        reason: str,
        trigger_dt: pd.Timestamp,
        trigger_price: float,
        reverse_signal: dict[str, Any] | None = None,
    ) -> None:
        if self.position_state is None or self.position_state.get("queued_exit_reason"):
            return
        self.position_state["queued_exit_reason"] = reason
        self.position_state["queued_exit_trigger_dt"] = trigger_dt
        self.position_state["queued_exit_trigger_price"] = trigger_price
        self.position_state["queued_exit_reverse_signal"] = reverse_signal

    def _close_position(
        self,
        exit_price: float,
        exit_dt: pd.Timestamp,
        exit_reason: str,
        bar_number: int,
        trigger_dt: pd.Timestamp | None = None,
        trigger_price: float | None = None,
        reverse_signal: dict[str, Any] | None = None,
        gap_exit: int = 0,
    ) -> None:
        if self.position_state is None:
            return
        state = self.position_state
        signal = state["signal"]
        entry_price = float(state["entry_price"])
        trade_return = (float(exit_price) - entry_price) / entry_price
        hold_bars = int(bar_number - int(state["entry_bar"]) + 1)
        trade = {
            "engine": "backtrader",
            "level": signal.get("signal_level", "15m"),
            "strategy": "family1_research_enhanced_a_share",
            "symbol": str(signal["symbol"]),
            "name": str(signal["name"]),
            "signal": str(signal["signal"]),
            "signal_date": to_bt_datetime(signal["date"]),
            "signal_price": round(float(signal["close"]), 6),
            "score": round(float(signal["score"]), 6),
            "entry_date": state["entry_dt"],
            "entry_price": round(entry_price, 6),
            "entry_method": state["entry_method"],
            "stop_price": round(float(state["stop_price"]), 6),
            "exit_date": exit_dt,
            "exit_price": round(float(exit_price), 6),
            "exit_reason": exit_reason,
            "hold_bars": hold_bars,
            "hold_days": round(elapsed_days(pd.Timestamp(state["entry_dt"]), exit_dt), 6),
            "flat_days_before_entry": state["flat_days_before_entry"],
            "ret": round(float(trade_return), 6),
            "win": int(trade_return > 0),
            "atr_value": round(float(state["atr_value"]), 6),
            "risk_distance": round(float(state["risk_distance"]), 6),
            "trigger_date": trigger_dt,
            "trigger_price": round(float(trigger_price), 6) if trigger_price is not None else None,
            "reverse_signal_date": to_bt_datetime(reverse_signal["date"]) if reverse_signal is not None else None,
            "reverse_signal_price": round(float(reverse_signal["close"]), 6) if reverse_signal is not None else None,
            "reverse_daily_gate_label": reverse_signal.get("daily_gate_label") if reverse_signal is not None else None,
            "reverse_daily_gate_reason": reverse_signal.get("daily_gate_reason") if reverse_signal is not None else None,
            "reverse_transition_phase": reverse_signal.get("transition_phase") if reverse_signal is not None else None,
            "reverse_transition_hist": reverse_signal.get("transition_hist") if reverse_signal is not None else None,
            "reverse_transition_previous_hist": reverse_signal.get("transition_previous_hist") if reverse_signal is not None else None,
            "gap_exit": int(gap_exit),
            "daily_gate_label": signal.get("daily_gate_label"),
            "daily_gate_reason": signal.get("daily_gate_reason"),
            "transition_phase": signal.get("transition_phase", signal.get("four_hour_transition_phase")),
            "transition_hist": signal.get("transition_hist", signal.get("four_hour_hist")),
            "transition_previous_hist": signal.get("transition_previous_hist", signal.get("four_hour_previous_hist")),
            "four_hour_transition_phase": signal.get("four_hour_transition_phase", signal.get("transition_phase")),
            "four_hour_hist": signal.get("four_hour_hist", signal.get("transition_hist")),
            "four_hour_previous_hist": signal.get("four_hour_previous_hist", signal.get("transition_previous_hist")),
            "signal_level": signal.get("signal_level"),
            "gate_level": signal.get("gate_level"),
            "transition_level": signal.get("transition_level"),
        }
        self.trade_records.append(trade)
        self.last_exit_dt = exit_dt
        self.position_state = None


def build_bt_feed(price_df: pd.DataFrame) -> Family1ResearchEnhancedData:
    feed_df = price_df.copy()
    feed_df["date"] = pd.to_datetime(feed_df["date"])
    feed_df = feed_df.set_index("date").sort_index()
    return Family1ResearchEnhancedData(dataname=feed_df)


def run_backtrader_symbol(
    symbol: str,
    name: str,
    price_df: pd.DataFrame,
    candidates_df: pd.DataFrame,
    reverse_candidates_df: pd.DataFrame,
    args: argparse.Namespace,
) -> pd.DataFrame:
    if price_df.empty or candidates_df.empty:
        return pd.DataFrame()

    cerebro = bt.Cerebro(stdstats=False)
    cerebro.adddata(build_bt_feed(price_df), name=symbol)
    cerebro.addstrategy(
        Family1ResearchEnhancedStrategy,
        symbol=symbol,
        name=name,
        signal_rows=candidates_df.to_dict("records"),
        reverse_signal_rows=reverse_candidates_df.to_dict("records"),
        stop_buffer_atr=args.stop_buffer_atr,
        max_hold_bars=scaled_max_hold_bars(args.max_hold_bars, args.signal_level),
    )
    results = cerebro.run()
    strategy = results[0]
    return pd.DataFrame(strategy.trade_records)


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

    trade_records: list[dict[str, Any]] = []
    stats_rows: list[dict[str, Any]] = []
    total_symbols = len(universe)

    for idx, (_, row) in enumerate(universe.iterrows(), start=1):
        symbol = str(row["symbol"])
        name = str(row.get("name", ""))
        print(f"[{idx}/{total_symbols}] backtrader family1 -> {symbol}")
        price_df, candidates_df, reverse_candidates_df, stats = prepare_family1_symbol_candidates(
            symbol=symbol,
            name=name,
            market_store=market_store,
            cfg=cfg,
            signal_level=args.signal_level,
            gate_level=args.gate_level,
            transition_level=args.transition_level,
        )
        stats_rows.append(stats)
        trades_df = run_backtrader_symbol(symbol, name, price_df, candidates_df, reverse_candidates_df, args)
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
    if not summary.empty:
        summary["engine"] = "backtrader"
    if not symbol_summary.empty:
        symbol_summary["engine"] = "backtrader"
    if not all_trades.empty:
        all_trades["engine"] = "backtrader"

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
        exit_mode="family1_research_enhanced_a_share_backtrader_rev_sell1",
        minute_source="market_data_sqlite",
        output_path=str(output_path),
        summary_path=str(summary_path),
        trades_df=all_trades,
        summary_df=summary,
    )

    market_store.close()
    backtest_store.close()

    print(f"backtrader_family1_research_enhanced_backtest v{PROJECT_VERSION}")
    print(f"saved trades -> {output_path}")
    print(f"saved summary -> {summary_path}")
    print(f"saved symbol summary -> {symbol_summary_path}")
    if not summary.empty:
        with pd.option_context("display.max_columns", 50, "display.width", 200):
            print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
