#!/usr/bin/env bash
# sleep-cycle.sh — Nightly consolidation for a fleet agent.
#
# Runs once per night per agent (staggered across the fleet). Performs:
#   1. Correction consolidation: promote repeated patterns to MEMORY.md
#   2. Memory hygiene: archive least-used if MEMORY.md > 100 lines
#   3. Discovery sleep audit: validate + promote high-confidence discoveries
#   4. Cross-agent learnings check: surface new shared patterns
#   5. Index update: refresh self-improving/index.md with current counts
#
# Called by NanoClaw scheduled_task as a script-phase job. Outputs JSON
# with wakeAgent=true when the agent should receive a summary on wake,
# or wakeAgent=false when nothing notable happened.

set -euo pipefail

GROUP_DIR="${NANOCLAW_WORKSPACE_GROUP:-}"
if [ -z "$GROUP_DIR" ] || [ ! -d "$GROUP_DIR" ]; then
  echo '{"wakeAgent": false}'
  exit 0
fi

AGENT_NAME=$(basename "$GROUP_DIR")
CORRECTIONS="$GROUP_DIR/self-improving/corrections.md"
MEMORY="$GROUP_DIR/memories/MEMORY.md"
ARCHIVE_DIR="$GROUP_DIR/self-improving/archive"
INDEX_FILE="$GROUP_DIR/self-improving/index.md"

mkdir -p "$ARCHIVE_DIR" "$GROUP_DIR/memories" "$GROUP_DIR/self-improving"

# 1. Correction consolidation — count pending-review patterns, find 3+ repeats
PENDING_COUNT=0
PROMOTED_COUNT=0
if [ -f "$CORRECTIONS" ]; then
  PENDING_COUNT=$(grep -c 'Status:\*\* pending-review' "$CORRECTIONS" 2>/dev/null || echo "0")
fi

# 2. Memory hygiene — count MEMORY.md lines
MEMORY_LINES=0
if [ -f "$MEMORY" ]; then
  MEMORY_LINES=$(wc -l <"$MEMORY" | tr -d ' ')
fi

ARCHIVED=false
if [ "$MEMORY_LINES" -gt 100 ]; then
  # Archive oldest half of memory file (preserve recent entries at the bottom)
  HALF=$((MEMORY_LINES / 2))
  TIMESTAMP=$(date -u +%Y%m%d-%H%M%S)
  head -n "$HALF" "$MEMORY" >"$ARCHIVE_DIR/MEMORY-$TIMESTAMP.md"
  tail -n "$HALF" "$MEMORY" >"$MEMORY.tmp" && mv "$MEMORY.tmp" "$MEMORY"
  ARCHIVED=true
fi

# 3. Discovery sleep audit — run shared script scoped to this agent
AUDIT_OUTPUT=""
AUDIT_SCRIPT="$HOME/.clan/learnings/scripts/discovery-sleep-audit.py"
if [ -x "$AUDIT_SCRIPT" ] || [ -f "$AUDIT_SCRIPT" ]; then
  AUDIT_OUTPUT=$(python3 "$AUDIT_SCRIPT" "$AGENT_NAME" 2>&1 || echo "audit-failed")
fi

# 4. Cross-agent learnings — count patterns by harness
PATTERNS_FILE="$HOME/.clan/learnings/patterns.jsonl"
PATTERN_COUNTS=""
if [ -f "$PATTERNS_FILE" ]; then
  TOTAL_PATTERNS=$(wc -l <"$PATTERNS_FILE" | tr -d ' ')
  NANOCLAW_PATTERNS=$(grep -c '"harness":"nanoclaw"' "$PATTERNS_FILE" 2>/dev/null || echo "0")
  HERMES_PATTERNS=$(grep -c '"harness":"hermes"' "$PATTERNS_FILE" 2>/dev/null || echo "0")
  PATTERN_COUNTS="total=$TOTAL_PATTERNS nanoclaw=$NANOCLAW_PATTERNS hermes=$HERMES_PATTERNS"
fi

# 5. Update index.md with current counts
DATE=$(date -u +%Y-%m-%d)
TIME=$(date -u +%H:%M:%S)
cat >"$INDEX_FILE" <<EOF
# Self-Improving Index — $AGENT_NAME

Last sleep cycle: $DATE $TIME UTC

## Counts
- Pending corrections: $PENDING_COUNT
- MEMORY.md lines: $MEMORY_LINES
- Memory archived this cycle: $ARCHIVED
- Cross-agent patterns: $PATTERN_COUNTS

## Discovery Audit
\`\`\`
$AUDIT_OUTPUT
\`\`\`

## Files
- corrections.md: $CORRECTIONS
- MEMORY.md: $MEMORY
- archive: $ARCHIVE_DIR
EOF

# Decide whether to wake the agent. Only wake if something requires attention:
# pending corrections ≥ 3, memory archived, or audit promoted patterns.
SHOULD_WAKE=false
REASONS=()

if [ "$PENDING_COUNT" -ge 3 ]; then
  SHOULD_WAKE=true
  REASONS+=("$PENDING_COUNT pending corrections need review")
fi

if [ "$ARCHIVED" = "true" ]; then
  SHOULD_WAKE=true
  REASONS+=("MEMORY.md archived ($MEMORY_LINES lines → split in half)")
fi

if echo "$AUDIT_OUTPUT" | grep -q "promoted=[1-9]"; then
  SHOULD_WAKE=true
  REASONS+=("discovery audit promoted new patterns")
fi

if [ "$SHOULD_WAKE" = "true" ]; then
  REASON_STR=$(printf '%s; ' "${REASONS[@]}")
  echo "{\"wakeAgent\": true, \"data\": {\"reasons\": \"${REASON_STR%; }\", \"pendingCorrections\": $PENDING_COUNT, \"memoryLines\": $MEMORY_LINES, \"memoryArchived\": $ARCHIVED, \"auditOutput\": $(echo "$AUDIT_OUTPUT" | python3 -c 'import sys,json; print(json.dumps(sys.stdin.read()))')}}"
else
  echo '{"wakeAgent": false}'
fi
