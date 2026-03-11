#!/usr/bin/env python3
from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

import pandas as pd


PRICE_COLUMNS = ["date", "open", "high", "low", "close", "volume", "amount", "turnover"]


class BacktestStore:
    def __init__(self, db_path: str) -> None:
        self.db_path = Path(db_path).expanduser().resolve()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path, timeout=30.0)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self._init_schema()

    def close(self) -> None:
        self.conn.close()

    def _init_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS price_history (
                symbol TEXT NOT NULL,
                level TEXT NOT NULL,
                adjust TEXT NOT NULL,
                source TEXT NOT NULL,
                date TEXT NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume REAL,
                amount REAL,
                turnover REAL,
                PRIMARY KEY (symbol, level, adjust, source, date)
            );

            CREATE TABLE IF NOT EXISTS backtest_runs (
                run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                start_date TEXT NOT NULL,
                end_date TEXT NOT NULL,
                levels_json TEXT NOT NULL,
                symbols_json TEXT NOT NULL,
                exit_mode TEXT NOT NULL,
                minute_source TEXT NOT NULL,
                output_path TEXT NOT NULL,
                summary_path TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS backtest_summary (
                run_id INTEGER NOT NULL,
                level TEXT NOT NULL,
                metrics_json TEXT NOT NULL,
                PRIMARY KEY (run_id, level)
            );

            CREATE TABLE IF NOT EXISTS backtest_trades (
                run_id INTEGER NOT NULL,
                level TEXT NOT NULL,
                symbol TEXT NOT NULL,
                trade_key TEXT NOT NULL,
                trade_json TEXT NOT NULL,
                PRIMARY KEY (run_id, trade_key)
            );
            """
        )
        self.conn.commit()

    def load_price_history(
        self,
        symbol: str,
        level: str,
        adjust: str,
        source: str,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        query = """
            SELECT date, open, high, low, close, volume, amount, turnover
            FROM price_history
            WHERE symbol = ?
              AND level = ?
              AND adjust = ?
              AND source = ?
              AND date >= ?
              AND date <= ?
            ORDER BY date
        """
        df = pd.read_sql_query(
            query,
            self.conn,
            params=(symbol, level, adjust, source, start_date, end_date),
        )
        if df.empty:
            return df
        df["date"] = pd.to_datetime(df["date"])
        return df

    def has_price_coverage(
        self,
        symbol: str,
        level: str,
        adjust: str,
        source: str,
        start_date: str,
        end_date: str,
    ) -> bool:
        row = self.conn.execute(
            """
            SELECT MIN(date), MAX(date), COUNT(*)
            FROM price_history
            WHERE symbol = ?
              AND level = ?
              AND adjust = ?
              AND source = ?
            """,
            (symbol, level, adjust, source),
        ).fetchone()
        if not row or not row[0] or not row[1] or row[2] == 0:
            return False
        return row[0] <= start_date and row[1] >= end_date

    def save_price_history(
        self,
        symbol: str,
        level: str,
        adjust: str,
        source: str,
        df: pd.DataFrame,
    ) -> None:
        if df.empty:
            return
        records = [
            (
                symbol,
                level,
                adjust,
                source,
                pd.Timestamp(row["date"]).isoformat(sep=" "),
                float(row["open"]),
                float(row["high"]),
                float(row["low"]),
                float(row["close"]),
                float(row["volume"]) if pd.notna(row["volume"]) else None,
                float(row["amount"]) if pd.notna(row["amount"]) else None,
                float(row["turnover"]) if pd.notna(row["turnover"]) else None,
            )
            for _, row in df.iterrows()
        ]
        self.conn.executemany(
            """
            INSERT OR REPLACE INTO price_history
            (symbol, level, adjust, source, date, open, high, low, close, volume, amount, turnover)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            records,
        )
        self.conn.commit()

    def save_backtest_run(
        self,
        *,
        created_at: str,
        start_date: str,
        end_date: str,
        levels: list[str],
        symbols: list[str],
        exit_mode: str,
        minute_source: str,
        output_path: str,
        summary_path: str,
        trades_df: pd.DataFrame,
        summary_df: pd.DataFrame,
    ) -> int:
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO backtest_runs
            (created_at, start_date, end_date, levels_json, symbols_json, exit_mode, minute_source, output_path, summary_path)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                created_at,
                start_date,
                end_date,
                json.dumps(levels, ensure_ascii=False),
                json.dumps(symbols, ensure_ascii=False),
                exit_mode,
                minute_source,
                output_path,
                summary_path,
            ),
        )
        run_id = int(cur.lastrowid)

        if not summary_df.empty:
            summary_records = [
                (run_id, str(row["level"]), json.dumps(row.dropna().to_dict(), ensure_ascii=False, default=str))
                for _, row in summary_df.iterrows()
            ]
            cur.executemany(
                "INSERT OR REPLACE INTO backtest_summary (run_id, level, metrics_json) VALUES (?, ?, ?)",
                summary_records,
            )

        if not trades_df.empty:
            trade_records = []
            for _, row in trades_df.iterrows():
                trade_key = "|".join(
                    [
                        str(row.get("level", "")),
                        str(row.get("symbol", "")),
                        str(row.get("signal", "")),
                        str(row.get("entry_date", "")),
                        str(row.get("exit_date", "")),
                    ]
                )
                trade_records.append(
                    (
                        run_id,
                        str(row.get("level", "")),
                        str(row.get("symbol", "")),
                        trade_key,
                        json.dumps(row.dropna().to_dict(), ensure_ascii=False, default=str),
                    )
                )
            cur.executemany(
                "INSERT OR REPLACE INTO backtest_trades (run_id, level, symbol, trade_key, trade_json) VALUES (?, ?, ?, ?, ?)",
                trade_records,
            )

        self.conn.commit()
        return run_id
