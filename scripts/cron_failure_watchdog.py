#!/usr/bin/env python3
"""Cron watchdog: speak only when scheduled runs are not completing.

Every run of every job in every cron store is supposed to reach a terminal state
(``completed``/``failed``). When something breaks the dispatch path instead, the
run is recorded ``unknown`` -- no output, no delivery, no retry -- and on a store
where the healthy jobs are silent and the failing ones are agent jobs, the board
looks green while the briefings and reports quietly stop arriving. That is the
class this watch exists for: on 2026-09-29 four runs died this way and nobody
noticed until a briefing was missing.

Silent by default. Prints nothing and exits 0 when the window is clean; prints a
terse report and exits 0 when it finds non-terminal runs, because the report IS
the alert and a non-zero exit would add a duplicate engine failure notice. Exit 1
is reserved for a broken checker (unreadable store), so a real defect in this
script can never pass as a quiet day.

Alerts are deduplicated by execution id, so a persistent failure is reported once,
not on every tick.

Usage:
    cron_failure_watchdog.py [--window-minutes N] [--min-failures N] [--state PATH]
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

NON_TERMINAL_BAD = ("failed", "unknown")


def store_paths() -> list[Path]:
    """The launch profile's store plus every named profile's, in a stable order."""
    home = Path(os.environ.get("HERMES_HOME") or (Path.home() / ".hermes"))
    paths = [home / "cron" / "executions.db"]
    profiles = home / "profiles"
    if profiles.is_dir():
        paths += sorted(p / "cron" / "executions.db" for p in profiles.iterdir() if p.is_dir())
    return [p for p in paths if p.is_file()]


def collect(path: Path, since_iso: str) -> list[dict]:
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        try:
            rows = conn.execute(
                """SELECT id, job_id, status, started_at, error FROM executions
                   WHERE status IN (?, ?) AND started_at >= ?
                   ORDER BY started_at DESC""",
                (*NON_TERMINAL_BAD, since_iso),
            ).fetchall()
        finally:
            conn.close()
    except sqlite3.Error as exc:
        print(f"cron-failure-watchdog: cannot read {path}: {exc}", file=sys.stderr)
        raise SystemExit(1)
    return [dict(r) for r in rows]


def job_names(path: Path) -> dict[str, str]:
    """Best effort: a job's human name, so the alert is actionable without a lookup."""
    jobs_file = path.parent / "jobs.json"
    try:
        data = json.loads(jobs_file.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    jobs = data.get("jobs", data) if isinstance(data, dict) else data
    names: dict[str, str] = {}
    for job in jobs or []:
        if isinstance(job, dict) and job.get("id"):
            names[str(job["id"])] = str(job.get("name") or job["id"])
    return names


def load_state(path: Path) -> set[str]:
    try:
        return set(json.loads(path.read_text(encoding="utf-8")).get("alerted", []))
    except (OSError, ValueError):
        return set()


def save_state(path: Path, alerted: set[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps({"alerted": sorted(alerted)[-500:]}, indent=1), encoding="utf-8")
    os.replace(tmp, path)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--window-minutes", type=int, default=60)
    parser.add_argument("--min-failures", type=int, default=2)
    parser.add_argument("--state", default=None)
    args = parser.parse_args()

    state_path = Path(args.state) if args.state else (
        Path(os.environ.get("HERMES_HOME") or (Path.home() / ".hermes"))
        / "cron" / "failure-watchdog-state.json"
    )
    since = (datetime.now(timezone.utc) - timedelta(minutes=args.window_minutes)).isoformat()

    failures: list[tuple[Path, dict, dict[str, str]]] = []
    for store in store_paths():
        names = job_names(store)
        for row in collect(store, since):
            failures.append((store, row, names))
    failures.sort(key=lambda item: item[1]["started_at"] or "")

    alerted = load_state(state_path)
    fresh = [f for f in failures if f[1]["id"] not in alerted]
    if len(failures) < args.min_failures or not fresh:
        return 0

    print(
        f"CRON FAILURES — {len(failures)} non-ok run(s) in the last "
        f"{args.window_minutes} min"
    )
    per_job: dict[str, list[dict]] = {}
    for store, row, names in failures:
        per_job.setdefault(f"{store}\0{row['job_id']}", []).append(row)
    for key, rows in per_job.items():
        store_str, _, job_id = key.partition("\0")
        names = job_names(Path(store_str))
        name = names.get(job_id, job_id)
        latest = rows[0]
        reason = " ".join((latest.get("error") or "").split())[:110]
        when = (latest.get("started_at") or "")[11:16]
        print(f"  {name} [{job_id}] — {len(rows)} run(s), latest {latest['status']} {when} — {reason}")

    save_state(state_path, alerted | {f[1]["id"] for f in failures})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
