#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib import parse, request
from zoneinfo import ZoneInfo

import pandas as pd

from market_data_store import MarketDataStore
from macd_time_signal_scanner import PROJECT_VERSION, ScanConfig, run_universe_scan, save_output
from macd_time_signal_scanner import MACDTimeSignalEngine


ASIA_SHANGHAI = ZoneInfo("Asia/Shanghai")
TELEGRAM_LIMIT = 3800

SIGNAL_LABELS = {
    "LEFT_BOTTOM": "左侧底",
    "LEFT_TOP": "左侧顶",
    "BUY1": "一买",
    "BUY2": "二买",
    "BUY3": "三买",
    "SELL1": "一卖",
    "SELL2": "二卖",
    "SELL3": "三卖",
}

GROUPS_ZH: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("左侧", ("LEFT_BOTTOM", "LEFT_TOP")),
    ("一买", ("BUY1",)),
    ("二买", ("BUY2",)),
    ("三买", ("BUY3",)),
    ("一卖", ("SELL1",)),
    ("二卖", ("SELL2",)),
    ("三卖", ("SELL3",)),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="MACD Telegram push system")
    parser.add_argument("--mode", choices=["scan", "replay", "test"], default="scan")
    parser.add_argument("--profile", choices=["alert", "summary", "manual"], default="alert")
    parser.add_argument("--data-source", choices=["scan", "market-db"], default="scan")
    parser.add_argument("--start-date", default="20220101")
    parser.add_argument("--end-date", default="20300101")
    parser.add_argument("--adjust", default="qfq", choices=["", "qfq", "hfq"])
    parser.add_argument("--period", default="daily", choices=["daily", "weekly", "monthly"])
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--min-amount", type=float, default=1.0e8)
    parser.add_argument("--min-price", type=float, default=2.0)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--recent-bars", type=int, default=5)
    parser.add_argument("--all-signals", action="store_true")
    parser.add_argument("--include-st", action="store_true")
    parser.add_argument("--include-delisting", action="store_true")
    parser.add_argument("--filter-date", default="")
    parser.add_argument("--telegram-token", default=os.getenv("TELEGRAM_BOT_TOKEN", ""))
    parser.add_argument("--telegram-chat-id", default=os.getenv("TELEGRAM_CHAT_ID", ""))
    parser.add_argument("--market-db-path", default=str(Path("data") / "market_data.sqlite"))
    parser.add_argument("--output", default=str(Path("outputs") / "latest_scan.csv"))
    parser.add_argument("--archive-dir", default=str(Path("outputs") / "archive"))
    parser.add_argument("--report-dir", default=str(Path("reports")))
    parser.add_argument("--state-file", default=str(Path("state") / "telegram_push_state.json"))
    parser.add_argument("--lock-file", default=str(Path("state") / "telegram_push.lock"))
    parser.add_argument("--max-per-group", type=int, default=20)
    parser.add_argument("--notify-empty", action="store_true")
    parser.add_argument("--no-dedup", action="store_true")
    parser.add_argument("--replay-file", default="")
    parser.add_argument("--replay-date", default="")
    parser.add_argument("--label", default="")
    parser.add_argument("--test-message", default="MACD 推送系统测试消息")
    return parser.parse_args()


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


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"sent_keys": [], "sent_batches": [], "runs": []}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"sent_keys": [], "sent_batches": [], "runs": []}


def save_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def ensure_dirs(*paths: Path) -> None:
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)


def now_local() -> datetime:
    return datetime.now(ASIA_SHANGHAI)


def now_stamp() -> str:
    return now_local().strftime("%Y%m%d_%H%M%S")


def resolve_date_value(raw: str) -> str:
    if not raw:
        return ""
    if raw == "today":
        return now_local().strftime("%Y-%m-%d")
    return pd.Timestamp(raw).strftime("%Y-%m-%d")


def append_run_history(state: dict[str, Any], run_record: dict[str, Any]) -> None:
    runs = list(state.get("runs", []))
    runs.append(run_record)
    state["runs"] = runs[-100:]


class LockGuard:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.fd: int | None = None

    def __enter__(self) -> "LockGuard":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            self.fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError as exc:
            raise SystemExit(f"推送任务正在运行，锁文件存在: {self.path}") from exc
        os.write(self.fd, str(os.getpid()).encode("utf-8"))
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if self.fd is not None:
            os.close(self.fd)
        if self.path.exists():
            self.path.unlink()


def signal_key(row: pd.Series) -> str:
    return f"{pd.Timestamp(row['date']).strftime('%Y-%m-%d')}|{row['symbol']}|{row['signal']}"


def archive_outputs(
    scan_df: pd.DataFrame,
    output_path: Path,
    archive_dir: Path,
    report_dir: Path,
    label: str,
) -> tuple[Path, Path]:
    ensure_dirs(output_path.parent, archive_dir, report_dir)
    save_output(scan_df, str(output_path))
    stamp = now_stamp()
    suffix = f"_{label}" if label else ""
    archive_csv = archive_dir / f"{stamp}{suffix}.csv"
    save_output(scan_df, str(archive_csv))
    report_json = report_dir / f"{stamp}{suffix}.json"
    return archive_csv, report_json


def summarise_df(df: pd.DataFrame) -> dict[str, Any]:
    if df.empty:
        return {"total": 0, "by_signal": {}, "latest_date": None, "top_scores": []}
    by_signal = {str(k): int(v) for k, v in df["signal"].value_counts().to_dict().items()}
    latest_date = pd.Timestamp(df["date"].max()).strftime("%Y-%m-%d")
    top_rows = (
        df.sort_values(["date", "score"], ascending=[False, False])
        .head(10)[["date", "symbol", "name", "signal", "close", "score"]]
        .to_dict("records")
    )
    for row in top_rows:
        row["date"] = pd.Timestamp(row["date"]).strftime("%Y-%m-%d")
        row["signal_label"] = SIGNAL_LABELS.get(str(row["signal"]), str(row["signal"]))
    return {
        "total": int(len(df)),
        "by_signal": by_signal,
        "latest_date": latest_date,
        "top_scores": top_rows,
    }


def write_report(report_path: Path, report: dict[str, Any]) -> None:
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")


def filter_by_date(df: pd.DataFrame, date_value: str) -> pd.DataFrame:
    if not date_value or df.empty:
        return df
    target = pd.Timestamp(date_value).normalize()
    return df[df["date"].dt.normalize() == target].copy()


def format_signal_line(row: pd.Series) -> str:
    date_text = pd.Timestamp(row["date"]).strftime("%Y-%m-%d")
    signal = SIGNAL_LABELS.get(str(row["signal"]), str(row["signal"]))
    name = str(row.get("name") or "")
    symbol = str(row["symbol"])
    close = float(row["close"])
    score = float(row["score"])
    label = f"{symbol} {name}".strip()
    return f"{date_text} | {signal} | {label} | 收盘={close:.2f} | 评分={score:.1f}"


def chunk_text(lines: list[str], limit: int = TELEGRAM_LIMIT) -> list[str]:
    parts: list[str] = []
    current = ""
    for line in lines:
        candidate = f"{current}\n{line}".strip() if current else line
        if len(candidate) > limit and current:
            parts.append(current)
            current = line
        else:
            current = candidate
    if current:
        parts.append(current)
    return parts


def build_messages(df: pd.DataFrame, label: str, max_per_group: int) -> list[str]:
    run_at = now_local().strftime("%Y-%m-%d %H:%M:%S")
    total = len(df)
    date_min = pd.Timestamp(df["date"].min()).strftime("%Y-%m-%d") if not df.empty else "-"
    date_max = pd.Timestamp(df["date"].max()).strftime("%Y-%m-%d") if not df.empty else "-"
    counts = df["signal"].value_counts().to_dict() if not df.empty else {}

    header = [
        f"MACD 信号系统 v{PROJECT_VERSION}",
        f"批次：{label}",
        f"时间：{run_at}",
        f"信号日期：{date_min} -> {date_max}",
        f"数量：{total}",
    ]
    if counts:
        header.append("分布：" + "，".join(f"{SIGNAL_LABELS.get(key, key)}={int(value)}" for key, value in counts.items()))

    messages = ["\n".join(header)]
    for title, signals in GROUPS_ZH:
        group_df = df[df["signal"].isin(signals)].sort_values(["date", "score"], ascending=[False, False]).head(max_per_group)
        if group_df.empty:
            continue
        lines = [title]
        lines.extend(format_signal_line(row) for _, row in group_df.iterrows())
        hidden = len(df[df["signal"].isin(signals)]) - len(group_df)
        if hidden > 0:
            lines.append(f"... 还有 {hidden} 条")
        messages.extend(chunk_text(lines))
    return messages


def send_telegram_message(token: str, chat_id: str, text: str) -> None:
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = parse.urlencode(
        {
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": "true",
        }
    ).encode("utf-8")
    req = request.Request(url, data=payload, method="POST")
    with request.urlopen(req, timeout=30) as resp:
        raw = resp.read().decode("utf-8")
    data = json.loads(raw)
    if not data.get("ok"):
        raise RuntimeError(f"Telegram sendMessage 失败: {raw}")


def validate_telegram(args: argparse.Namespace) -> None:
    if not args.telegram_token:
        raise SystemExit("缺少 Telegram bot token")
    if not args.telegram_chat_id:
        raise SystemExit("缺少 Telegram chat id")


def read_replay_df(args: argparse.Namespace) -> pd.DataFrame:
    replay_file = Path(args.replay_file).expanduser().resolve() if args.replay_file else Path(args.output).expanduser().resolve()
    if not replay_file.exists():
        raise SystemExit(f"回放文件不存在: {replay_file}")
    df = pd.read_csv(replay_file)
    if "date" not in df.columns:
        raise SystemExit(f"回放文件缺少 date 列: {replay_file}")
    df["date"] = pd.to_datetime(df["date"])
    replay_date = resolve_date_value(args.replay_date)
    return filter_by_date(df, replay_date)


def market_db_source_name(period: str) -> str:
    if period in {"daily", "weekly", "monthly"}:
        return "akshare"
    return "baostock"


def market_db_bounds(args: argparse.Namespace) -> tuple[str, str]:
    start = pd.Timestamp(args.start_date).strftime("%Y-%m-%d 00:00:00")
    end = pd.Timestamp(args.end_date).strftime("%Y-%m-%d 23:59:59")
    return start, end


def scan_from_market_db(args: argparse.Namespace) -> pd.DataFrame:
    store = MarketDataStore(args.market_db_path)
    try:
        universe = store.load_latest_universe(limit=args.limit)
        if universe.empty:
            return pd.DataFrame()

        if not args.include_st and "is_st" in universe.columns:
            universe = universe[universe["is_st"].fillna(0).astype(int) == 0].copy()
        if not args.include_delisting and "is_delisting" in universe.columns:
            universe = universe[universe["is_delisting"].fillna(0).astype(int) == 0].copy()
        if "amount" in universe.columns:
            universe = universe[pd.to_numeric(universe["amount"], errors="coerce").fillna(0) >= args.min_amount].copy()
        if "last" in universe.columns:
            universe = universe[pd.to_numeric(universe["last"], errors="coerce").fillna(0) >= args.min_price].copy()
        if universe.empty:
            return pd.DataFrame()

        engine = MACDTimeSignalEngine(build_config(args))
        source_name = market_db_source_name(args.period)
        start_bound, end_bound = market_db_bounds(args)
        chunks: list[pd.DataFrame] = []

        for _, row in universe.iterrows():
            symbol = str(row["symbol"])
            name = str(row.get("name", "") or "")
            hist = store.load_kline(symbol, args.period, args.adjust, source_name, start_bound, end_bound)
            if hist.empty:
                continue
            signal_df = engine.scan_dataframe(hist, symbol=symbol, name=name)
            if signal_df is not None and not signal_df.empty:
                chunks.append(signal_df)

        if not chunks:
            return pd.DataFrame()
        out = pd.concat(chunks, ignore_index=True)
        return out.sort_values(["date", "score", "symbol"], ascending=[False, False, True]).reset_index(drop=True)
    finally:
        store.close()


def dedup_for_profile(df: pd.DataFrame, args: argparse.Namespace, state: dict[str, Any], effective_date: str) -> tuple[pd.DataFrame, set[str], set[str]]:
    sent_keys = set(state.get("sent_keys", []))
    sent_batches = set(state.get("sent_batches", []))
    if args.no_dedup:
        df = df.copy()
        df["signal_key"] = df.apply(signal_key, axis=1)
        return df, sent_keys, sent_batches

    if args.profile == "summary":
        batch_key = f"summary|{effective_date or 'all'}|{args.label or '收盘汇总'}"
        if batch_key in sent_batches:
            return pd.DataFrame(columns=list(df.columns) + ["signal_key"]), sent_keys, sent_batches
        df = df.copy()
        df["signal_key"] = df.apply(signal_key, axis=1)
        df.attrs["batch_key"] = batch_key
        return df, sent_keys, sent_batches

    df = df.copy()
    df["signal_key"] = df.apply(signal_key, axis=1)
    return df[~df["signal_key"].isin(sent_keys)].copy(), sent_keys, sent_batches


def update_state_after_push(state: dict[str, Any], df: pd.DataFrame, sent_keys: set[str], sent_batches: set[str]) -> None:
    ordered_keys = list(state.get("sent_keys", []))
    for key in df["signal_key"].tolist():
        if key not in sent_keys:
            ordered_keys.append(key)
            sent_keys.add(key)
    state["sent_keys"] = ordered_keys[-10000:]

    batch_key = df.attrs.get("batch_key")
    if batch_key and batch_key not in sent_batches:
        ordered_batches = list(state.get("sent_batches", []))
        ordered_batches.append(batch_key)
        state["sent_batches"] = ordered_batches[-1000:]


def main() -> None:
    args = parse_args()
    validate_telegram(args)

    output_path = Path(args.output).expanduser().resolve()
    archive_dir = Path(args.archive_dir).expanduser().resolve()
    report_dir = Path(args.report_dir).expanduser().resolve()
    state_path = Path(args.state_file).expanduser().resolve()
    lock_path = Path(args.lock_file).expanduser().resolve()
    ensure_dirs(output_path.parent, archive_dir, report_dir, state_path.parent)

    with LockGuard(lock_path):
        state = load_state(state_path)
        started_at = now_local().isoformat(timespec="seconds")
        effective_date = resolve_date_value(args.filter_date)
        label = args.label or ("盘中预警" if args.profile == "alert" else "收盘汇总" if args.profile == "summary" else args.mode)

        try:
            if args.mode == "test":
                send_telegram_message(args.telegram_token, args.telegram_chat_id, f"MACD 信号系统 v{PROJECT_VERSION}\n{args.test_message}")
                state["last_success_at"] = now_local().isoformat(timespec="seconds")
                append_run_history(
                    state,
                    {"started_at": started_at, "finished_at": state["last_success_at"], "mode": "test", "status": "ok"},
                )
                save_state(state_path, state)
                print("test message sent")
                return

            if args.mode == "scan":
                if args.data_source == "market-db":
                    scan_df = scan_from_market_db(args)
                else:
                    scan_df = run_universe_scan(build_config(args))
            else:
                scan_df = read_replay_df(args)

            if not scan_df.empty:
                scan_df["date"] = pd.to_datetime(scan_df["date"])
            scan_df = filter_by_date(scan_df, effective_date)
            archive_csv, report_json = archive_outputs(scan_df, output_path, archive_dir, report_dir, label)

            summary = summarise_df(scan_df)
            write_report(
                report_json,
                {
                    "version": PROJECT_VERSION,
                    "mode": args.mode,
                    "profile": args.profile,
                    "data_source": args.data_source,
                    "label": label,
                    "filter_date": effective_date,
                    "started_at": started_at,
                    "finished_at": now_local().isoformat(timespec="seconds"),
                    "summary": summary,
                    "output_csv": str(output_path),
                    "archive_csv": str(archive_csv),
                },
            )

            if scan_df.empty:
                if args.notify_empty:
                    send_telegram_message(args.telegram_token, args.telegram_chat_id, f"{label}\n日期：{effective_date or '全部'}\n没有符合条件的信号。")
                state["last_success_at"] = now_local().isoformat(timespec="seconds")
                append_run_history(
                    state,
                    {
                        "started_at": started_at,
                        "finished_at": state["last_success_at"],
                        "mode": args.mode,
                        "profile": args.profile,
                        "data_source": args.data_source,
                        "label": label,
                        "status": "ok",
                        "rows": 0,
                        "pushed": 0,
                        "archive_csv": str(archive_csv),
                    },
                )
                save_state(state_path, state)
                print("scan finished with no rows")
                return

            to_push_df, sent_keys, sent_batches = dedup_for_profile(scan_df, args, state, effective_date)
            if to_push_df.empty:
                if args.notify_empty:
                    send_telegram_message(args.telegram_token, args.telegram_chat_id, f"{label}\n日期：{effective_date or '全部'}\n没有新的可推送内容。")
                state["last_success_at"] = now_local().isoformat(timespec="seconds")
                append_run_history(
                    state,
                    {
                        "started_at": started_at,
                        "finished_at": state["last_success_at"],
                        "mode": args.mode,
                        "profile": args.profile,
                        "data_source": args.data_source,
                        "label": label,
                        "status": "ok",
                        "rows": int(len(scan_df)),
                        "pushed": 0,
                        "archive_csv": str(archive_csv),
                    },
                )
                save_state(state_path, state)
                print("scan finished with no new signals")
                return

            for message in build_messages(to_push_df, label, args.max_per_group):
                send_telegram_message(args.telegram_token, args.telegram_chat_id, message)

            update_state_after_push(state, to_push_df, sent_keys, sent_batches)
            state["last_success_at"] = now_local().isoformat(timespec="seconds")
            state["last_sent_count"] = int(len(to_push_df))
            append_run_history(
                state,
                {
                    "started_at": started_at,
                    "finished_at": state["last_success_at"],
                    "mode": args.mode,
                    "profile": args.profile,
                    "data_source": args.data_source,
                    "label": label,
                    "status": "ok",
                    "filter_date": effective_date,
                    "rows": int(len(scan_df)),
                    "pushed": int(len(to_push_df)),
                    "archive_csv": str(archive_csv),
                    "report_json": str(report_json),
                },
            )
            save_state(state_path, state)
            print(f"pushed {len(to_push_df)} signals to Telegram")
        except Exception as exc:
            state["last_error_at"] = now_local().isoformat(timespec="seconds")
            state["last_error"] = str(exc)
            append_run_history(
                state,
                {
                    "started_at": started_at,
                    "finished_at": state["last_error_at"],
                    "mode": args.mode,
                    "profile": args.profile,
                    "data_source": args.data_source,
                    "label": label,
                    "status": "error",
                    "error": str(exc),
                },
            )
            save_state(state_path, state)
            raise


if __name__ == "__main__":
    main()
