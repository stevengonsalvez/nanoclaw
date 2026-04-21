#!/usr/bin/env python3
"""session-usage-collector — ingest per-message Anthropic usage to mission-control.

Cron cadence: every 1 minute.

NanoClaw sources usage from the Claude Agent SDK transcript, not a SQLite
sessions table. For each agent in {geordi (groups/main), data (groups/data)}:
  1. Read ~/.nanoclaw/groups/<group>/logs/chat.json (JSON array)
  2. Aggregate per sessionId: sum tokens, find started_at/ended_at, model,
     message count, tool_use count
  3. Only emit sessions whose last message timestamp > cursor
  4. Map to /api/sync/session-usage payload shape (same as Hermes)
  5. POST each row (3s timeout, fire-and-forget)
  6. Advance cursor only on POST success

Idempotency: cursor is (agent -> max last-message-ts seen). Server-side
dedup on (sessionId, agentId) handles restart replay.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Optional

_SCRIPT_DIR = Path(__file__).resolve().parent
if str(_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_DIR))

import mc_client  # noqa: E402

LOG = mc_client.setup_logger("session-usage")

# Agent name → group folder under ~/.nanoclaw/groups/
AGENTS = {
    "geordi": "main",
    "data": "data",
}

GROUPS_ROOT = Path.home() / ".nanoclaw" / "groups"
ENDPOINT = "/api/sync/session-usage"
PROJECT_PATH = "nanoclaw/lambda"

# Cost model — Anthropic public pricing (USD per 1M tokens, as of 2026-04).
# Used for cost estimation since chat.json doesn't record billed amounts.
MODEL_PRICING = {
    "claude-opus-4-6":    {"input": 15.00, "output": 75.00, "cacheRead": 1.50,  "cacheWrite": 18.75},
    "claude-opus-4-7":    {"input": 15.00, "output": 75.00, "cacheRead": 1.50,  "cacheWrite": 18.75},
    "claude-sonnet-4-5":  {"input":  3.00, "output": 15.00, "cacheRead": 0.30,  "cacheWrite":  3.75},
    "claude-sonnet-4-6":  {"input":  3.00, "output": 15.00, "cacheRead": 0.30,  "cacheWrite":  3.75},
    "claude-haiku-4-5":   {"input":  1.00, "output":  5.00, "cacheRead": 0.10,  "cacheWrite":  1.25},
}
DEFAULT_PRICING = {"input": 3.00, "output": 15.00, "cacheRead": 0.30, "cacheWrite": 3.75}


def _load_sync_state() -> dict:
    path = mc_client.state_path("session-usage-sync-state.json")
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception as exc:
        LOG.warning("sync-state unreadable (%s); treating as empty", exc)
        return {}


def _save_sync_state(state: dict) -> None:
    path = mc_client.state_path("session-usage-sync-state.json")
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(state, indent=2, sort_keys=True))
    tmp.replace(path)


def _parse_iso_ms(ts: str) -> int:
    """Parse an ISO 8601 timestamp (Z-suffixed) to epoch milliseconds."""
    # strptime can't handle fractional seconds uniformly; split + fromisoformat-ish.
    try:
        base, _, frac = ts.rstrip("Z").partition(".")
        st = time.strptime(base, "%Y-%m-%dT%H:%M:%S")
        secs = int(time.mktime(st)) - time.timezone  # treat as UTC
        ms = 0
        if frac:
            ms = int(frac[:3].ljust(3, "0"))
        return secs * 1000 + ms
    except ValueError:
        return 0


def _read_chat_entries(group_folder: str) -> list:
    path = GROUPS_ROOT / group_folder / "logs" / "chat.json"
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text())
        return data if isinstance(data, list) else []
    except (OSError, json.JSONDecodeError) as exc:
        LOG.warning("%s chat.json read failed: %s", group_folder, exc)
        return []


def _aggregate_sessions(entries: list) -> dict:
    """Group chat.json entries by sessionId. Return dict[sessionId] -> aggregate.

    Aggregate fields: started_at_ms, ended_at_ms, model, input_tokens,
    output_tokens, cache_read, cache_write, message_count, user_message_count,
    tool_use_count, last_entry_ts_ms.
    """
    sessions: dict[str, dict] = {}

    for entry in entries:
        if not isinstance(entry, dict):
            continue
        sid = entry.get("sessionId")
        if not sid:
            continue
        ts_str = entry.get("timestamp")
        ts_ms = _parse_iso_ms(ts_str) if ts_str else 0

        agg = sessions.setdefault(
            sid,
            {
                "sessionId": sid,
                "started_at_ms": ts_ms or None,
                "ended_at_ms": ts_ms or 0,
                "model": None,
                "input_tokens": 0,
                "output_tokens": 0,
                "cache_read": 0,
                "cache_write": 0,
                "message_count": 0,
                "user_message_count": 0,
                "tool_use_count": 0,
                "last_entry_ts_ms": 0,
            },
        )

        if ts_ms:
            if agg["started_at_ms"] is None or ts_ms < agg["started_at_ms"]:
                agg["started_at_ms"] = ts_ms
            if ts_ms > agg["ended_at_ms"]:
                agg["ended_at_ms"] = ts_ms
            if ts_ms > agg["last_entry_ts_ms"]:
                agg["last_entry_ts_ms"] = ts_ms

        entry_type = entry.get("type")
        if entry_type == "user":
            agg["message_count"] += 1
            agg["user_message_count"] += 1
        elif entry_type == "assistant":
            agg["message_count"] += 1
            msg = entry.get("message") or {}
            if msg.get("model") and not agg["model"]:
                agg["model"] = msg["model"]

            # tool_use blocks
            content = msg.get("content") or []
            if isinstance(content, list):
                for block in content:
                    if isinstance(block, dict) and block.get("type") == "tool_use":
                        agg["tool_use_count"] += 1

            usage = msg.get("usage") or {}
            agg["input_tokens"] += int(usage.get("input_tokens") or 0)
            agg["output_tokens"] += int(usage.get("output_tokens") or 0)
            agg["cache_read"] += int(usage.get("cache_read_input_tokens") or 0)
            agg["cache_write"] += int(usage.get("cache_creation_input_tokens") or 0)

    return sessions


def _cost_for(model: str, aggregate: dict) -> tuple[float, dict]:
    pricing = MODEL_PRICING.get(model or "", DEFAULT_PRICING)
    input_cost = aggregate["input_tokens"] * pricing["input"] / 1_000_000
    output_cost = aggregate["output_tokens"] * pricing["output"] / 1_000_000
    cache_read_cost = aggregate["cache_read"] * pricing["cacheRead"] / 1_000_000
    cache_write_cost = aggregate["cache_write"] * pricing["cacheWrite"] / 1_000_000
    total = input_cost + output_cost + cache_read_cost + cache_write_cost
    breakdown = {
        "input": round(input_cost, 6),
        "output": round(output_cost, 6),
        "cacheRead": round(cache_read_cost, 6),
        "cacheWrite": round(cache_write_cost, 6),
    }
    return round(total, 6), breakdown


def _build_payload(agent_id: str, agg: dict) -> dict:
    model = agg["model"] or "unknown"
    started_ms = agg["started_at_ms"] or 0
    ended_ms = agg["ended_at_ms"] or started_ms
    duration_ms = max(0, ended_ms - started_ms)

    cost_total, breakdown = _cost_for(model, agg)

    return {
        "sessionId": agg["sessionId"],
        "agentId": agent_id,
        "projectPath": PROJECT_PATH,
        "model": model,
        "tokensIn": agg["input_tokens"],
        "tokensOut": agg["output_tokens"],
        "cacheRead": agg["cache_read"],
        "cacheWrite": agg["cache_write"],
        "costTotal": cost_total,
        "costBreakdown": breakdown,
        "messageCount": agg["message_count"],
        "userMessageCount": agg["user_message_count"],
        "toolUseCount": agg["tool_use_count"],
        "durationMs": duration_ms,
        "startedAt": started_ms,
        "endedAt": ended_ms,
        "isSubagent": False,
    }


def _collect_for_agent(
    agent_id: str, group_folder: str, sync_state: dict, *, dry_run: bool
) -> tuple[int, int]:
    entries = _read_chat_entries(group_folder)
    if not entries:
        return (0, 0)

    sessions = _aggregate_sessions(entries)
    cursor_ms = int(sync_state.get(agent_id, {}).get("last_entry_ts_ms", 0))

    # Sort sessions by last-entry timestamp; only ship those beyond cursor.
    ordered = sorted(sessions.values(), key=lambda a: a["last_entry_ts_ms"])

    attempted = 0
    succeeded = 0
    last_success_cursor = cursor_ms

    for agg in ordered:
        if agg["last_entry_ts_ms"] <= cursor_ms:
            continue
        if agg["input_tokens"] == 0 and agg["output_tokens"] == 0:
            # No usage data (likely the initial user turn before assistant).
            continue

        attempted += 1
        payload = _build_payload(agent_id, agg)

        if dry_run:
            print(json.dumps(payload, indent=2, sort_keys=True))
            succeeded += 1
            continue

        ok = mc_client.post(ENDPOINT, payload)
        if ok:
            succeeded += 1
            last_success_cursor = max(last_success_cursor, agg["last_entry_ts_ms"])
        else:
            LOG.warning(
                "POST failed for %s session %s; cursor held at %d",
                agent_id, agg["sessionId"], last_success_cursor,
            )
            break

    if not dry_run and last_success_cursor > cursor_ms:
        sync_state[agent_id] = {
            "last_entry_ts_ms": last_success_cursor,
            "last_synced_wall": time.time(),
        }

    return (attempted, succeeded)


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__ or "")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--agent", choices=list(AGENTS.keys()))
    args = parser.parse_args(argv)

    if args.self_test:
        rc = mc_client.self_test()
        for agent, folder in AGENTS.items():
            p = GROUPS_ROOT / folder / "logs" / "chat.json"
            print(f"[session-usage] {agent} (group={folder}) chat.json exists: {p.exists()}")
        return rc

    targets = {args.agent: AGENTS[args.agent]} if args.agent else AGENTS
    sync_state = _load_sync_state()

    total_attempted = 0
    total_succeeded = 0
    for agent_id, group_folder in targets.items():
        a, s = _collect_for_agent(
            agent_id, group_folder, sync_state, dry_run=args.dry_run
        )
        total_attempted += a
        total_succeeded += s

    if not args.dry_run and total_succeeded > 0:
        _save_sync_state(sync_state)

    if total_attempted and total_succeeded < total_attempted:
        LOG.warning(
            "session-usage: %d/%d sessions posted", total_succeeded, total_attempted
        )

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        LOG.error("fatal: %s", exc)
        sys.exit(0)
