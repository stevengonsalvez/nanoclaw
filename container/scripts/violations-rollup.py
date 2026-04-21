#!/usr/bin/env python3
"""violations-rollup — stream new violations.jsonl entries to mission-control.

Cron cadence: every 30 minutes.

For each NanoClaw agent in {geordi (groups/main), data (groups/data)}:
  - Tail ~/.nanoclaw/groups/<group>/self-improving/violations.jsonl using
    a byte-offset cursor at
    ~/.nanoclaw/observability/state/violations-rollup-sync-state.json
  - For each new JSON line, POST /api/violations/report

Payload shape per peer schema:
  REQUIRED: agentId, reportedBy, rule, description
  severity only: "warning" | "violation"   (no other literals accepted)
  project: "lambda"
  messageSnippet (optional, <=100 chars)

REJECTED by server (omit): createdAt, metadata, timestamp.

Severity inference: rule substring match table, with explicit severity
field passed through when valid. Harness 'error' or 'critical' labels
coerce to 'violation'.

Truncation/rotation: if file is smaller than cursor, cursor resets to 0
and replays. Server dedups by (agentId, rule, description).

Exit 0 regardless; collectors must never kill cron.
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

LOG = mc_client.setup_logger("violations-rollup")

AGENTS = {
    "geordi": "main",
    "data": "data",
}

GROUPS_ROOT = Path.home() / ".nanoclaw" / "groups"
ENDPOINT = "/api/violations/report"

# Rule substring → severity. Matched case-insensitively, first hit wins.
# Server accepts only 'warning' | 'violation' — map hard rules to 'violation'.
_SEVERITY_RULES = (
    ("orphan-mention", "warning"),
    ("missing-inbox", "violation"),
    ("unauthorized", "violation"),
    ("security", "violation"),
    ("invalid-inbox", "violation"),
    ("incomplete-inbox", "warning"),
    ("rate-limit", "warning"),
    ("inbox_enforcer", "warning"),
    ("corrections-drift", "warning"),
    ("missing-correction", "warning"),
)

_VALID_SEVERITIES = {"warning", "violation"}


def _infer_severity(entry: dict) -> str:
    rule = (entry.get("rule") or entry.get("type") or entry.get("violation_type") or "").lower()
    raw = (entry.get("severity") or "").lower()
    if raw in _VALID_SEVERITIES:
        return raw
    if raw in {"error", "critical", "high"}:
        return "violation"
    for needle, sev in _SEVERITY_RULES:
        if needle in rule:
            return sev
    return "warning"


def _load_sync_state() -> dict:
    path = mc_client.state_path("violations-rollup-sync-state.json")
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception as exc:
        LOG.warning("sync-state unreadable: %s", exc)
        return {}


def _save_sync_state(state: dict) -> None:
    path = mc_client.state_path("violations-rollup-sync-state.json")
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(state, indent=2, sort_keys=True))
    tmp.replace(path)


def _build_payload(agent_id: str, entry: dict) -> dict:
    # NanoClaw inbox-enforcer emits entries with {ts, sessionId, mentions[],
    # messagePreview, type, platform} where type is the rule name.
    rule = entry.get("rule") or entry.get("type") or entry.get("violation_type") or "unknown"
    preview = (entry.get("messagePreview") or entry.get("message_preview") or "")[:200]

    # Description is REQUIRED — synthesize from the best available fields.
    detail = entry.get("detail") or entry.get("description")
    if detail:
        description = detail
    else:
        # Build a readable summary from rule + mentions + preview.
        mentions = entry.get("mentions") or []
        if mentions:
            description = f"{rule}: mentioned {', '.join(mentions[:3])} without [inbox:ID]"
        elif entry.get("missingFields"):
            description = f"{rule}: missing fields {', '.join(entry['missingFields'])}"
        elif preview:
            description = f"{rule}: {preview}"
        else:
            description = rule

    # REJECTED by server: metadata, createdAt, timestamp. Omit.
    # Include only: agentId, reportedBy, rule, description, severity, project, messageSnippet.
    return {
        "agentId": agent_id,
        "rule": rule,
        "reportedBy": "plugin",
        "description": description,
        "messageSnippet": preview[:100],
        "severity": _infer_severity(entry),
        "project": "lambda",
    }


def _process_agent(
    agent_id: str, group_folder: str, sync_state: dict, *, dry_run: bool
) -> tuple[int, int]:
    path = GROUPS_ROOT / group_folder / "self-improving" / "violations.jsonl"
    if not path.exists():
        return (0, 0)

    profile_state = sync_state.get(agent_id, {})
    last_offset = int(profile_state.get("offset", 0))

    try:
        size = path.stat().st_size
    except OSError as exc:
        LOG.warning("%s violations stat failed: %s", agent_id, exc)
        return (0, 0)

    if last_offset > size:
        LOG.warning(
            "%s violations shrank (%d -> %d); resetting cursor", agent_id, last_offset, size
        )
        last_offset = 0

    if last_offset == size:
        return (0, 0)

    attempted = 0
    succeeded = 0
    new_offset = last_offset

    try:
        with path.open("rb") as fh:
            fh.seek(last_offset)
            while True:
                line_start = fh.tell()
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
                    LOG.warning("%s invalid JSON at offset %d", agent_id, line_start)
                    new_offset = fh.tell()
                    continue

                attempted += 1
                payload = _build_payload(agent_id, entry)

                if dry_run:
                    print(json.dumps(payload, indent=2, sort_keys=True))
                    succeeded += 1
                    new_offset = fh.tell()
                    continue

                if mc_client.post(ENDPOINT, payload):
                    succeeded += 1
                    new_offset = fh.tell()
                else:
                    LOG.warning(
                        "%s violations POST failed; holding cursor at %d",
                        agent_id, new_offset,
                    )
                    break
    except OSError as exc:
        LOG.warning("%s violations read failed: %s", agent_id, exc)

    if not dry_run and new_offset != last_offset:
        sync_state[agent_id] = {"offset": new_offset, "last_synced_wall": time.time()}

    return (attempted, succeeded)


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__ or "")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--agent", choices=list(AGENTS.keys()))
    args = parser.parse_args(argv)

    if args.self_test:
        rc = mc_client.self_test()
        for agent, group in AGENTS.items():
            v = GROUPS_ROOT / group / "self-improving" / "violations.jsonl"
            print(f"[violations-rollup] {agent} violations.jsonl: {v.exists()}")
        return rc

    targets = {args.agent: AGENTS[args.agent]} if args.agent else AGENTS
    sync_state = _load_sync_state()

    total_attempted = 0
    total_succeeded = 0
    for agent_id, group in targets.items():
        a, s = _process_agent(agent_id, group, sync_state, dry_run=args.dry_run)
        total_attempted += a
        total_succeeded += s

    if not args.dry_run and total_succeeded > 0:
        _save_sync_state(sync_state)

    if total_attempted and total_succeeded < total_attempted:
        LOG.warning(
            "violations-rollup: %d/%d posted", total_succeeded, total_attempted
        )

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        LOG.error("fatal: %s", exc)
        sys.exit(0)
