#!/usr/bin/env python3
from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

import pandas as pd


KLINE_COLUMNS = ["date", "open", "high", "low", "close", "volume", "amount", "turnover"]


def infer_exchange(symbol: str) -> str:
    if symbol.startswith(("6", "5")):
        return "SH"
    if symbol.startswith(("4", "8", "9")):
        return "BJ"
    return "SZ"


def infer_board(symbol: str) -> str:
    if symbol.startswith("688"):
        return "STAR"
    if symbol.startswith(("300", "301")):
        return "ChiNext"
    if symbol.startswith(("4", "8", "9")):
        return "BSE"
    return "Main"


class MarketDataStore:
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
            CREATE TABLE IF NOT EXISTS symbols (
                symbol TEXT PRIMARY KEY,
                name TEXT,
                exchange TEXT NOT NULL,
                board TEXT NOT NULL,
                last REAL,
                amount REAL,
                float_mkt_cap REAL,
                turnover REAL,
                is_st INTEGER NOT NULL DEFAULT 0,
                is_delisting INTEGER NOT NULL DEFAULT 0,
                source TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS universe_snapshots (
                snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                source TEXT NOT NULL,
                total_symbols INTEGER NOT NULL,
                filters_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS universe_snapshot_items (
                snapshot_id INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                name TEXT,
                last REAL,
                amount REAL,
                float_mkt_cap REAL,
                turnover REAL,
                rank_amount INTEGER,
                PRIMARY KEY (snapshot_id, symbol)
            );

            CREATE TABLE IF NOT EXISTS kline (
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
                is_derived INTEGER NOT NULL DEFAULT 0,
                derived_from TEXT,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (symbol, level, adjust, source, date)
            );

            CREATE TABLE IF NOT EXISTS data_coverage (
                symbol TEXT NOT NULL,
                level TEXT NOT NULL,
                adjust TEXT NOT NULL,
                source TEXT NOT NULL,
                min_date TEXT NOT NULL,
                max_date TEXT NOT NULL,
                rows_count INTEGER NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (symbol, level, adjust, source)
            );

            CREATE TABLE IF NOT EXISTS sync_runs (
                run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                start_date TEXT NOT NULL,
                end_date TEXT NOT NULL,
                levels_json TEXT NOT NULL,
                symbols_json TEXT NOT NULL,
                source TEXT NOT NULL,
                mode TEXT NOT NULL,
                status TEXT NOT NULL,
                notes TEXT
            );

            CREATE TABLE IF NOT EXISTS sync_run_items (
                run_id INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                level TEXT NOT NULL,
                status TEXT NOT NULL,
                rows_written INTEGER NOT NULL,
                min_date TEXT,
                max_date TEXT,
                started_at TEXT NOT NULL,
                finished_at TEXT NOT NULL,
                error TEXT,
                PRIMARY KEY (run_id, symbol, level)
            );

            CREATE INDEX IF NOT EXISTS idx_kline_symbol_level_date
            ON kline (symbol, level, date);

            CREATE INDEX IF NOT EXISTS idx_symbols_exchange_board
            ON symbols (exchange, board);

            CREATE INDEX IF NOT EXISTS idx_snapshot_items_symbol
            ON universe_snapshot_items (symbol);
            """
        )
        self.conn.commit()

    def start_sync_run(
        self,
        *,
        start_date: str,
        end_date: str,
        levels: list[str],
        symbols: list[str],
        source: str,
        mode: str,
        notes: str = "",
    ) -> int:
        now = pd.Timestamp.now().isoformat()
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO sync_runs
            (created_at, started_at, start_date, end_date, levels_json, symbols_json, source, mode, status, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                now,
                now,
                start_date,
                end_date,
                json.dumps(levels, ensure_ascii=False),
                json.dumps(symbols, ensure_ascii=False),
                source,
                mode,
                "running",
                notes,
            ),
        )
        self.conn.commit()
        return int(cur.lastrowid)

    def finish_sync_run(self, run_id: int, status: str, notes: str = "") -> None:
        self.conn.execute(
            """
            UPDATE sync_runs
            SET finished_at = ?, status = ?, notes = ?
            WHERE run_id = ?
            """,
            (pd.Timestamp.now().isoformat(), status, notes, run_id),
        )
        self.conn.commit()

    def record_sync_item(
        self,
        *,
        run_id: int,
        symbol: str,
        level: str,
        status: str,
        rows_written: int,
        started_at: str,
        finished_at: str,
        min_date: str | None = None,
        max_date: str | None = None,
        error: str | None = None,
    ) -> None:
        self.conn.execute(
            """
            INSERT OR REPLACE INTO sync_run_items
            (run_id, symbol, level, status, rows_written, min_date, max_date, started_at, finished_at, error)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                symbol,
                level,
                status,
                rows_written,
                min_date,
                max_date,
                started_at,
                finished_at,
                error,
            ),
        )
        self.conn.commit()

    def upsert_symbols(self, universe_df: pd.DataFrame, source: str) -> int:
        if universe_df.empty:
            return 0
        now = pd.Timestamp.now().isoformat()
        records = []
        for _, row in universe_df.iterrows():
            symbol = str(row["symbol"])
            name = str(row.get("name", ""))
            records.append(
                (
                    symbol,
                    name,
                    infer_exchange(symbol),
                    infer_board(symbol),
                    self._to_float(row.get("last")),
                    self._to_float(row.get("amount")),
                    self._to_float(row.get("float_mkt_cap")),
                    self._to_float(row.get("turnover")),
                    int("ST" in name.upper()),
                    int("退" in name),
                    source,
                    now,
                )
            )
        self.conn.executemany(
            """
            INSERT OR REPLACE INTO symbols
            (symbol, name, exchange, board, last, amount, float_mkt_cap, turnover, is_st, is_delisting, source, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            records,
        )
        self.conn.commit()
        return len(records)

    def create_universe_snapshot(self, universe_df: pd.DataFrame, source: str, filters: dict[str, Any]) -> int:
        now = pd.Timestamp.now().isoformat()
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO universe_snapshots
            (created_at, source, total_symbols, filters_json)
            VALUES (?, ?, ?, ?)
            """,
            (now, source, int(len(universe_df)), json.dumps(filters, ensure_ascii=False)),
        )
        snapshot_id = int(cur.lastrowid)
        if universe_df.empty:
            self.conn.commit()
            return snapshot_id

        work = universe_df.copy()
        if "amount" in work.columns:
            work["amount_num"] = pd.to_numeric(work["amount"], errors="coerce")
            work = work.sort_values("amount_num", ascending=False, na_position="last").reset_index(drop=True)
        else:
            work = work.reset_index(drop=True)

        records = []
        for idx, row in work.iterrows():
            records.append(
                (
                    snapshot_id,
                    str(row["symbol"]),
                    str(row.get("name", "")),
                    self._to_float(row.get("last")),
                    self._to_float(row.get("amount")),
                    self._to_float(row.get("float_mkt_cap")),
                    self._to_float(row.get("turnover")),
                    idx + 1,
                )
            )
        cur.executemany(
            """
            INSERT OR REPLACE INTO universe_snapshot_items
            (snapshot_id, symbol, name, last, amount, float_mkt_cap, turnover, rank_amount)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            records,
        )
        self.conn.commit()
        return snapshot_id

    def load_latest_universe(self, limit: int | None = None) -> pd.DataFrame:
        snapshot = self.conn.execute(
            """
            SELECT snapshot_id
            FROM universe_snapshots
            ORDER BY snapshot_id DESC
            LIMIT 1
            """
        ).fetchone()
        if snapshot:
            query = """
                SELECT
                    u.symbol,
                    COALESCE(NULLIF(u.name, ''), s.name, '') AS name,
                    COALESCE(u.last, s.last) AS last,
                    COALESCE(u.amount, s.amount) AS amount,
                    COALESCE(u.float_mkt_cap, s.float_mkt_cap) AS float_mkt_cap,
                    COALESCE(u.turnover, s.turnover) AS turnover,
                    COALESCE(s.is_st, 0) AS is_st,
                    COALESCE(s.is_delisting, 0) AS is_delisting,
                    u.rank_amount
                FROM universe_snapshot_items u
                LEFT JOIN symbols s ON s.symbol = u.symbol
                WHERE u.snapshot_id = ?
                ORDER BY u.rank_amount ASC, u.symbol ASC
            """
            df = pd.read_sql_query(query, self.conn, params=(int(snapshot[0]),))
        else:
            df = pd.read_sql_query(
                """
                SELECT
                    symbol,
                    name,
                    last,
                    amount,
                    float_mkt_cap,
                    turnover,
                    is_st,
                    is_delisting,
                    NULL AS rank_amount
                FROM symbols
                ORDER BY symbol ASC
                """,
                self.conn,
            )

        if df.empty:
            return df
        df["symbol"] = df["symbol"].astype(str)
        if limit is not None:
            df = df.head(limit).copy()
        return df.reset_index(drop=True)

    def latest_covered_date(self, level: str, adjust: str, source: str) -> pd.Timestamp | None:
        row = self.conn.execute(
            """
            SELECT MAX(max_date)
            FROM data_coverage
            WHERE level = ?
              AND adjust = ?
              AND source = ?
            """,
            (level, adjust, source),
        ).fetchone()
        if not row or not row[0]:
            return None
        return pd.Timestamp(row[0])

    def count_symbols_covered_through(self, level: str, adjust: str, source: str, target_date: str) -> int:
        row = self.conn.execute(
            """
            SELECT COUNT(*)
            FROM data_coverage
            WHERE level = ?
              AND adjust = ?
              AND source = ?
              AND date(max_date) >= date(?)
            """,
            (level, adjust, source, target_date),
        ).fetchone()
        return int(row[0]) if row and row[0] is not None else 0

    def has_kline_coverage(
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
            SELECT min_date, max_date, rows_count
            FROM data_coverage
            WHERE symbol = ?
              AND level = ?
              AND adjust = ?
              AND source = ?
            """,
            (symbol, level, adjust, source),
        ).fetchone()
        if not row:
            return False
        min_date, max_date, rows_count = row
        return bool(min_date and max_date and rows_count and min_date <= start_date and max_date >= end_date)

    def load_kline(
        self,
        symbol: str,
        level: str,
        adjust: str,
        source: str,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        df = pd.read_sql_query(
            """
            SELECT date, open, high, low, close, volume, amount, turnover
            FROM kline
            WHERE symbol = ?
              AND level = ?
              AND adjust = ?
              AND source = ?
              AND date >= ?
              AND date <= ?
            ORDER BY date
            """,
            self.conn,
            params=(symbol, level, adjust, source, start_date, end_date),
        )
        if df.empty:
            return df
        df["date"] = pd.to_datetime(df["date"])
        return df

    def save_kline(
        self,
        symbol: str,
        level: str,
        adjust: str,
        source: str,
        df: pd.DataFrame,
        *,
        is_derived: bool = False,
        derived_from: str | None = None,
    ) -> int:
        if df.empty:
            return 0
        work = df.copy()
        for col in KLINE_COLUMNS:
            if col not in work.columns:
                work[col] = None
        work["date"] = pd.to_datetime(work["date"])
        now = pd.Timestamp.now().isoformat()
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
                self._to_float(row.get("volume")),
                self._to_float(row.get("amount")),
                self._to_float(row.get("turnover")),
                int(is_derived),
                derived_from,
                now,
            )
            for _, row in work.iterrows()
        ]
        self.conn.executemany(
            """
            INSERT OR REPLACE INTO kline
            (symbol, level, adjust, source, date, open, high, low, close, volume, amount, turnover, is_derived, derived_from, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            records,
        )
        self._refresh_coverage(symbol, level, adjust, source)
        self.conn.commit()
        return len(records)

    def import_backtest_price_history(self, backtest_db_path: str) -> int:
        source_db = Path(backtest_db_path).expanduser().resolve()
        if not source_db.exists():
            return 0
        ext = sqlite3.connect(source_db)
        try:
            cur = ext.execute(
                """
                SELECT symbol, level, adjust, source, date, open, high, low, close, volume, amount, turnover
                FROM price_history
                ORDER BY symbol, level, adjust, source, date
                """
            )
            batch: list[tuple[Any, ...]] = []
            coverage_keys: set[tuple[str, str, str, str]] = set()
            imported_symbols: set[str] = set()
            total = 0
            now = pd.Timestamp.now().isoformat()
            for row in cur:
                symbol, level, adjust, source, date, open_, high_, low_, close_, volume, amount, turnover = row
                imported_symbols.add(symbol)
                batch.append(
                    (
                        symbol,
                        level,
                        adjust,
                        source,
                        date,
                        open_,
                        high_,
                        low_,
                        close_,
                        volume,
                        amount,
                        turnover,
                        0,
                        None,
                        now,
                    )
                )
                coverage_keys.add((symbol, level, adjust, source))
                if len(batch) >= 5000:
                    self.conn.executemany(
                        """
                        INSERT OR REPLACE INTO kline
                        (symbol, level, adjust, source, date, open, high, low, close, volume, amount, turnover, is_derived, derived_from, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        batch,
                    )
                    total += len(batch)
                    batch = []
            if batch:
                self.conn.executemany(
                    """
                    INSERT OR REPLACE INTO kline
                    (symbol, level, adjust, source, date, open, high, low, close, volume, amount, turnover, is_derived, derived_from, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    batch,
                )
                total += len(batch)
            for key in sorted(coverage_keys):
                self._refresh_coverage(*key)
            self._ensure_symbol_placeholders(imported_symbols, source="backtest_cache_import")
            self.conn.commit()
            return total
        finally:
            ext.close()

    def backfill_symbols_from_kline(self, source: str = "kline_backfill") -> int:
        rows = self.conn.execute("SELECT DISTINCT symbol FROM kline").fetchall()
        symbols = {str(row[0]) for row in rows if row and row[0]}
        self._ensure_symbol_placeholders(symbols, source=source)
        self.conn.commit()
        return len(symbols)

    def _refresh_coverage(self, symbol: str, level: str, adjust: str, source: str) -> None:
        row = self.conn.execute(
            """
            SELECT MIN(date), MAX(date), COUNT(*)
            FROM kline
            WHERE symbol = ?
              AND level = ?
              AND adjust = ?
              AND source = ?
            """,
            (symbol, level, adjust, source),
        ).fetchone()
        if not row or not row[0] or not row[1]:
            return
        self.conn.execute(
            """
            INSERT OR REPLACE INTO data_coverage
            (symbol, level, adjust, source, min_date, max_date, rows_count, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                symbol,
                level,
                adjust,
                source,
                row[0],
                row[1],
                int(row[2]),
                pd.Timestamp.now().isoformat(),
            ),
        )

    def _ensure_symbol_placeholders(self, symbols: set[str], source: str) -> None:
        if not symbols:
            return
        now = pd.Timestamp.now().isoformat()
        records = [
            (
                symbol,
                "",
                infer_exchange(symbol),
                infer_board(symbol),
                source,
                now,
            )
            for symbol in sorted(symbols)
        ]
        self.conn.executemany(
            """
            INSERT OR IGNORE INTO symbols
            (symbol, name, exchange, board, source, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            records,
        )

    @staticmethod
    def _to_float(value: Any) -> float | None:
        if value is None or (isinstance(value, float) and pd.isna(value)) or pd.isna(value):
            return None
        try:
            return float(value)
        except Exception:
            return None
