#!/usr/bin/env python3
"""agent-state-collector — publish live fleet-grid state to mission-control.

Cron cadence: every 15 minutes.

For each NanoClaw agent in {geordi, data}:
  - Heartbeat age: last_run on the hb-<agent> scheduled_task row (SQLite)
  - 24h rollup: aggregate chat.json entries by timestamp window (count
    sessions, messages, tokens, estimated cost)
  - Context %: message_count_last_1h * 400 tokens / 200k window
  - Inbox depth: GET /api/inbox/poll?agent=<id>
  - Status rule (peer schema: no 'stale' — map to idle or offline):
      active  — last heartbeat < 10 min ago
      idle    — last heartbeat < 30 min ago (covers the old 'stale' window)
      offline — unknown / > 30 min
  - POST /api/sync/agent-status with REQUIRED: theme, heartbeatInterval,
    model, workspace, channels[]
  - REJECTED by server: metadata, updatedAt (do not include)

Always exit 0. Silent on success; warnings to
~/.nanoclaw/observability/logs/agent-state.log.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from pathlib import Path
from typing import Optional
from urllib.parse import urlencode

_SCRIPT_DIR = Path(__file__).resolve().parent
if str(_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_DIR))

import mc_client  # noqa: E402

LOG = mc_client.setup_logger("agent-state")

GROUPS_ROOT = Path.home() / ".nanoclaw" / "groups"
MESSAGES_DB = Path.home() / ".nanoclaw" / "store" / "messages.db"
ENDPOINT = "/api/sync/agent-status"
INBOX_ENDPOINT_TEMPLATE = "/api/inbox/poll?{qs}"

# Agent display metadata. Required by server schema: theme, model, workspace, channels.
AGENT_META = {
    "geordi": {
        "name": "Geordi",
        "emoji": "🔧",
        "theme": "yellow",         # gold engineering
        "model": "claude-opus-4-6",
        "workspace": "fleet-lambda",
        "channels": ["discord"],
        "group": "main",
    },
    "data": {
        "name": "Data",
        "emoji": "👁️",
        "theme": "slate",          # android cool
        "model": "claude-opus-4-6",
        "workspace": "fleet-lambda",
        "channels": ["discord"],
        "group": "data",
    },
}

HEARTBEAT_INTERVAL = "1m"

TOKENS_PER_MSG_ESTIMATE = 400
CONTEXT_WINDOW_TOKENS = 200_000

STATUS_ACTIVE_MAX_SECONDS = 10 * 60
STATUS_IDLE_MAX_SECONDS = 30 * 60

# Anthropic pricing for cost estimate (USD per 1M tokens). Mirror collector.
MODEL_PRICING = {
    "claude-opus-4-6":   {"input": 15.00, "output": 75.00, "cacheRead": 1.50, "cacheWrite": 18.75},
    "claude-opus-4-7":   {"input": 15.00, "output": 75.00, "cacheRead": 1.50, "cacheWrite": 18.75},
    "claude-sonnet-4-5": {"input":  3.00, "output": 15.00, "cacheRead": 0.30, "cacheWrite":  3.75},
    "claude-sonnet-4-6": {"input":  3.00, "output": 15.00, "cacheRead": 0.30, "cacheWrite":  3.75},
    "claude-haiku-4-5":  {"input":  1.00, "output":  5.00, "cacheRead": 0.10, "cacheWrite":  1.25},
}
DEFAULT_PRICING = {"input": 3.00, "output": 15.00, "cacheRead": 0.30, "cacheWrite": 3.75}


def _heartbeat_age(agent: str) -> Optional[float]:
    """Seconds since the last heartbeat scheduled_task ran.

    Looks up scheduled_tasks.last_run for id = 'hb-<agent>'. Returns None
    if the row is missing or last_run is NULL.
    """
    if not MESSAGES_DB.exists():
        return None
    try:
        conn = sqlite3.connect(f"file:{MESSAGES_DB}?mode=ro", uri=True, timeout=5.0)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT last_run FROM scheduled_tasks WHERE id = ?",
            (f"hb-{agent}",),
        ).fetchone()
        conn.close()
        if not row or not row["last_run"]:
            return None
        last_run_ms = _parse_iso_ms(row["last_run"])
        if last_run_ms == 0:
            return None
        return max(0.0, time.time() - last_run_ms / 1000.0)
    except sqlite3.Error as exc:
        LOG.warning("heartbeat lookup failed for %s: %s", agent, exc)
        return None


def _status_label(heartbeat_age_seconds: Optional[float]) -> str:
    """Map heartbeat age to server-accepted enum.

    Peer schema note: server enum is active|idle|error|blocked|offline.
    No 'stale' — map the old stale window (10–30min) to 'idle' and
    everything beyond to 'offline'.
    """
    if heartbeat_age_seconds is None:
        return "offline"
    if heartbeat_age_seconds <= STATUS_ACTIVE_MAX_SECONDS:
        return "active"
    if heartbeat_age_seconds <= STATUS_IDLE_MAX_SECONDS:
        return "idle"
    return "offline"


def _parse_iso_ms(ts: str) -> int:
    try:
        base, _, frac = ts.rstrip("Z").partition(".")
        st = time.strptime(base, "%Y-%m-%dT%H:%M:%S")
        secs = int(time.mktime(st)) - time.timezone
        ms = int(frac[:3].ljust(3, "0")) if frac else 0
        return secs * 1000 + ms
    except ValueError:
        return 0


def _read_chat(group_folder: str) -> list:
    path = GROUPS_ROOT / group_folder / "logs" / "chat.json"
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text())
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _count_violations_last_24h(group_folder: str) -> int:
    path = GROUPS_ROOT / group_folder / "self-improving" / "violations.jsonl"
    if not path.exists():
        return 0
    cutoff_ms = int((time.time() - 24 * 3600) * 1000)
    count = 0
    try:
        with path.open("r", encoding="utf-8", errors="replace") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    continue
                ts = entry.get("ts") or entry.get("timestamp")
                if not ts:
                    continue
                ts_ms = _parse_iso_ms(ts)
                if ts_ms >= cutoff_ms:
                    count += 1
    except OSError as exc:
        LOG.warning("violations read failed for %s: %s", group_folder, exc)
    return count


def _chat_rollup(group_folder: str) -> dict:
    """24h + 1h aggregates from chat.json. Same math as session-usage cost model."""
    out = {
        "sessionCount": 0,
        "cost24h": 0.0,
        "costLast1h": 0.0,
        "messageCount24h": 0,
        "messageCountLast1h": 0,
        "tokensUsed24h": 0,
    }

    entries = _read_chat(group_folder)
    if not entries:
        return out

    now_ms = int(time.time() * 1000)
    cutoff_24h = now_ms - 24 * 3600 * 1000
    cutoff_1h = now_ms - 1 * 3600 * 1000

    sessions_in_24h: set[str] = set()

    for entry in entries:
        if not isinstance(entry, dict):
            continue
        ts = entry.get("timestamp")
        if not ts:
            continue
        ts_ms = _parse_iso_ms(ts)
        if ts_ms < cutoff_24h:
            continue

        sid = entry.get("sessionId")
        if sid:
            sessions_in_24h.add(sid)

        etype = entry.get("type")
        if etype not in ("user", "assistant"):
            continue

        out["messageCount24h"] += 1
        if ts_ms >= cutoff_1h:
            out["messageCountLast1h"] += 1

        if etype == "assistant":
            msg = entry.get("message") or {}
            usage = msg.get("usage") or {}
            model = msg.get("model") or "unknown"
            tok_in = int(usage.get("input_tokens") or 0)
            tok_out = int(usage.get("output_tokens") or 0)
            tok_cache_r = int(usage.get("cache_read_input_tokens") or 0)
            tok_cache_w = int(usage.get("cache_creation_input_tokens") or 0)
            out["tokensUsed24h"] += tok_in + tok_out + tok_cache_r + tok_cache_w

            pricing = MODEL_PRICING.get(model, DEFAULT_PRICING)
            msg_cost = (
                tok_in * pricing["input"]
                + tok_out * pricing["output"]
                + tok_cache_r * pricing["cacheRead"]
                + tok_cache_w * pricing["cacheWrite"]
            ) / 1_000_000
            out["cost24h"] += msg_cost
            if ts_ms >= cutoff_1h:
                out["costLast1h"] += msg_cost

    out["sessionCount"] = len(sessions_in_24h)
    out["cost24h"] = round(out["cost24h"], 6)
    out["costLast1h"] = round(out["costLast1h"], 6)
    return out


def _inbox_depth(agent: str) -> int:
    qs = urlencode({"agent": agent, "project": "lambda", "clan": "lambda"})
    data = mc_client.get(INBOX_ENDPOINT_TEMPLATE.format(qs=qs))
    if data is None:
        return 0
    items = data.get("items") if isinstance(data, dict) else data
    return len(items) if isinstance(items, list) else 0


def _context_pct(message_count_last_1h: int) -> float:
    approx_tokens = message_count_last_1h * TOKENS_PER_MSG_ESTIMATE
    return round(min(100.0, (approx_tokens / CONTEXT_WINDOW_TOKENS) * 100.0), 2)


def _current_task(group_folder: str) -> str:
    pending = GROUPS_ROOT / group_folder / "self-improving" / "corrections.md"
    if pending.exists():
        try:
            content = pending.read_text()
            if "pending-review" in content:
                return "reviewing pending corrections"
        except OSError:
            pass
    return "idle"


def _build_payload(agent_id: str) -> dict:
    meta = AGENT_META[agent_id]
    group = meta["group"]

    heartbeat_age = _heartbeat_age(agent_id)
    status = _status_label(heartbeat_age)
    last_heartbeat_ms = (
        int((time.time() - heartbeat_age) * 1000) if heartbeat_age is not None else 0
    )

    rollup = _chat_rollup(group)
    inbox_depth = _inbox_depth(agent_id)
    context_pct = _context_pct(rollup["messageCountLast1h"])

    # REQUIRED fields per peer schema: theme, heartbeatInterval, model, workspace, channels[]
    # REJECTED: metadata, updatedAt (omitted)
    payload = {
        "agentId": agent_id,
        "name": meta["name"],
        "emoji": meta["emoji"],
        "theme": meta["theme"],
        "status": status,
        "currentTask": _current_task(group),
        "lastHeartbeat": last_heartbeat_ms,
        "heartbeatInterval": HEARTBEAT_INTERVAL,
        "model": meta["model"],
        "workspace": meta["workspace"],
        "channels": meta["channels"],
        "sessionCount": rollup["sessionCount"],
        "messageCount24h": rollup["messageCount24h"],
        "tokensUsed24h": rollup["tokensUsed24h"],
        "costTotal24h": rollup["cost24h"],
        "contextPct": context_pct,
        "inboxDepth": inbox_depth,
        "costLast1h": rollup["costLast1h"],
    }
    return payload


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__ or "")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--agent", choices=list(AGENT_META.keys()))
    args = parser.parse_args(argv)

    if args.self_test:
        rc = mc_client.self_test()
        for agent, meta in AGENT_META.items():
            chat = GROUPS_ROOT / meta["group"] / "logs" / "chat.json"
            print(f"[agent-state] {agent} chat.json: {chat.exists()}")
        print(f"[agent-state] messages.db: {MESSAGES_DB.exists()}")
        return rc

    targets = [args.agent] if args.agent else list(AGENT_META.keys())
    posted = 0
    for agent in targets:
        try:
            payload = _build_payload(agent)
        except Exception as exc:
            LOG.error("build payload failed for %s: %s", agent, exc)
            continue

        if args.dry_run:
            print(json.dumps(payload, indent=2, sort_keys=True))
            posted += 1
            continue

        if mc_client.post(ENDPOINT, payload):
            posted += 1
        else:
            LOG.warning("agent-status POST failed for %s", agent)

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        LOG.error("fatal: %s", exc)
        sys.exit(0)
