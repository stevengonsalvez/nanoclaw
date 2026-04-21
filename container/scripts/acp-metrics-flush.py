#!/usr/bin/env python3
"""acp-metrics-flush — stream ACP thread closure metrics to mission-control.

Cron cadence: every 10 minutes. Shared (not per-agent) — reads the single
fleet-wide JSONL emitted by the createACPMetricsHook Stop hook.

Source file: ~/.clan/learnings/acp-metrics.jsonl
Sync state:  ~/.nanoclaw/observability/state/acp-flush-state.json
Target:      POST /api/events  with  type="acp.thread-closed"

Payload shape (matches the /api/events validator used by Hermes):
  {
    "tenantId": <env WOLOLO_TENANT_ID || "dev-tenant-000">,
    "agentId":  <first agent from agentsInvolved, or "fleet">,
    "source":   "nanoclaw",
    "type":     "acp.thread-closed",
    "message":  "ACP thread <id> closed (<N> msgs, <M> agents)",
    "timestamp": <ISO 8601 Z>,
    "metadata": {
        threadId, protocol, messageCount, agentsInvolved,
        timeToResolutionMin, estimatedTokens, loopsDetected,
        humanInterventions, outcome, harness="nanoclaw", clan="lambda"
    }
  }

Idempotency: byte-offset cursor; truncation resets to 0 and replays.
Exit 0 regardless of outcome.
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

LOG = mc_client.setup_logger("acp-flush")

SOURCE_PATH = Path.home() / ".clan" / "learnings" / "acp-metrics.jsonl"
ENDPOINT = "/api/events"
EVENT_TYPE = "acp.thread-closed"

METADATA_FIELDS = (
    "threadId",
    "protocol",
    "messageCount",
    "agentsInvolved",
    "timeToResolutionMin",
    "estimatedTokens",
    "loopsDetected",
    "humanInterventions",
    "outcome",
)


def _load_sync_state() -> dict:
    path = mc_client.state_path("acp-flush-state.json")
    if not path.exists():
        return {"offset": 0}
    try:
        return json.loads(path.read_text())
    except Exception as exc:
        LOG.warning("sync-state unreadable: %s", exc)
        return {"offset": 0}


def _save_sync_state(state: dict) -> None:
    path = mc_client.state_path("acp-flush-state.json")
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(state, indent=2, sort_keys=True))
    tmp.replace(path)


def _pick_agent_id(entry: dict) -> str:
    agents = entry.get("agentsInvolved") or []
    if isinstance(agents, list) and agents:
        first = agents[0]
        if isinstance(first, str) and first:
            return first
    return entry.get("initiator") or "fleet"


def _build_payload(entry: dict) -> dict:
    metadata = {k: entry[k] for k in METADATA_FIELDS if k in entry}
    metadata.setdefault("threadId", entry.get("thread_id", ""))
    metadata["harness"] = "nanoclaw"
    metadata["clan"] = entry.get("clan", "lambda")

    message_count = entry.get("messageCount", "?")
    agents_involved = entry.get("agentsInvolved") or []
    agent_count = len(agents_involved) if isinstance(agents_involved, list) else "?"
    thread_id = metadata.get("threadId") or entry.get("thread_id", "?")

    ts = entry.get("closedAt") or entry.get("ts") or entry.get("timestamp") or time.strftime(
        "%Y-%m-%dT%H:%M:%SZ", time.gmtime()
    )

    return {
        "tenantId": mc_client.tenant_id(),
        "agentId": _pick_agent_id(entry),
        "source": "nanoclaw",
        "type": EVENT_TYPE,
        "message": f"ACP thread {thread_id} closed ({message_count} msgs, {agent_count} agents)",
        "timestamp": ts,
        "metadata": metadata,
    }


def _flush(state: dict, *, dry_run: bool) -> tuple[int, int]:
    if not SOURCE_PATH.exists():
        return (0, 0)

    try:
        size = SOURCE_PATH.stat().st_size
    except OSError as exc:
        LOG.warning("acp-metrics stat failed: %s", exc)
        return (0, 0)

    last_offset = int(state.get("offset", 0))

    if last_offset > size:
        LOG.warning(
            "acp-metrics shrank (%d -> %d); resetting cursor", last_offset, size
        )
        last_offset = 0

    if last_offset == size:
        return (0, 0)

    attempted = 0
    succeeded = 0
    new_offset = last_offset

    try:
        with SOURCE_PATH.open("rb") as fh:
            fh.seek(last_offset)
            while True:
                raw = fh.readline()
                if not raw:
                    break
                line = raw.decode("utf-8", "replace").strip()
                if not line:
                    new_offset = fh.tell()
                    continue
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    LOG.warning("invalid ACP JSON at offset %d", new_offset)
                    new_offset = fh.tell()
                    continue

                attempted += 1
                payload = _build_payload(entry)

                if dry_run:
                    print(json.dumps(payload, indent=2, sort_keys=True))
                    succeeded += 1
                    new_offset = fh.tell()
                    continue

                if mc_client.post(ENDPOINT, payload):
                    succeeded += 1
                    new_offset = fh.tell()
                else:
                    LOG.warning("acp POST failed; holding cursor at %d", new_offset)
                    break
    except OSError as exc:
        LOG.warning("acp-metrics read failed: %s", exc)

    if not dry_run and new_offset != last_offset:
        state["offset"] = new_offset
        state["last_synced_wall"] = time.time()

    return (attempted, succeeded)


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__ or "")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args(argv)

    if args.self_test:
        rc = mc_client.self_test()
        print(f"[acp-flush] source exists: {SOURCE_PATH.exists()} ({SOURCE_PATH})")
        return rc

    state = _load_sync_state()
    attempted, succeeded = _flush(state, dry_run=args.dry_run)

    if not args.dry_run and succeeded > 0:
        _save_sync_state(state)

    if attempted and succeeded < attempted:
        LOG.warning("acp-flush: %d/%d posted", succeeded, attempted)

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        LOG.error("fatal: %s", exc)
        sys.exit(0)
