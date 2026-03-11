#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path


STATE_PATH = Path(__file__).resolve().parents[1] / "state" / "telegram_push_state.json"


def main() -> None:
    if not STATE_PATH.exists():
        print("state file not found")
        return
    state = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    print(f"last_success_at: {state.get('last_success_at', '-')}")
    print(f"last_error_at: {state.get('last_error_at', '-')}")
    print(f"last_error: {state.get('last_error', '-')}")
    print(f"last_sent_count: {state.get('last_sent_count', 0)}")
    runs = state.get("runs", [])
    print(f"run_count: {len(runs)}")
    for run in runs[-5:]:
        print(json.dumps(run, ensure_ascii=False))


if __name__ == "__main__":
    main()
