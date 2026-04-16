#!/usr/bin/env bash
# heartbeat-poll.sh — Pre-flight check and inbox poll for fleet heartbeat.
#
# Called by task-scheduler as a script-phase task. Reads agent-config.yaml
# from NANOCLAW_WORKSPACE_GROUP, polls Convex inbox, and outputs a JSON
# result for the agent-runner's script handler.
#
# Output: { "wakeAgent": true/false, "data": { ... } }
#   - wakeAgent=true: agent-runner wakes and processes inbox items
#   - wakeAgent=false: nothing to do, stay idle
#
# Requires: curl, yq (or falls back to grep-based YAML parsing)

set -euo pipefail

GROUP_DIR="${NANOCLAW_WORKSPACE_GROUP:-}"
if [ -z "$GROUP_DIR" ]; then
  echo '{"wakeAgent": false}'
  exit 0
fi

CONFIG_FILE="$GROUP_DIR/agent-config.yaml"
if [ ! -f "$CONFIG_FILE" ]; then
  echo '{"wakeAgent": false}'
  exit 0
fi

# Parse agent-config.yaml
if command -v yq &>/dev/null; then
  CONVEX_URL=$(yq -r '.convex_url // empty' "$CONFIG_FILE" 2>/dev/null || true)
  PROJECT=$(yq -r '.project // empty' "$CONFIG_FILE" 2>/dev/null || true)
  CLAN=$(yq -r '.clan // empty' "$CONFIG_FILE" 2>/dev/null || true)
else
  # Fallback: grep-based parsing
  CONVEX_URL=$(grep '^convex_url:' "$CONFIG_FILE" | sed 's/^convex_url:\s*//' | tr -d '"' || true)
  PROJECT=$(grep '^project:' "$CONFIG_FILE" | sed 's/^project:\s*//' | tr -d '"' || true)
  CLAN=$(grep '^clan:' "$CONFIG_FILE" | sed 's/^clan:\s*//' | tr -d '"' || true)
fi

# Derive agent name from group folder name
AGENT_NAME=$(basename "$GROUP_DIR")

# Pre-flight: validate required fields
if [ -z "$CONVEX_URL" ] || [ -z "$PROJECT" ] || [ -z "$CLAN" ]; then
  echo '{"wakeAgent": true, "data": {"error": "agent-config.yaml missing required fields (convex_url, project, clan)", "preflightFailed": true}}'
  exit 0
fi

# Auth token
AUTH_TOKEN_FILE="${MC_AUTH_TOKEN_PATH:-$HOME/.secrets/mc-auth-token}"
if [ ! -f "$AUTH_TOKEN_FILE" ]; then
  echo '{"wakeAgent": false}'
  exit 0
fi
AUTH_TOKEN=$(cat "$AUTH_TOKEN_FILE")

# Poll inbox
INBOX_RESPONSE=$(curl -s --max-time 3 \
  -H "X-MC-Token: $AUTH_TOKEN" \
  "${CONVEX_URL}/api/inbox/poll?agent=${AGENT_NAME}&project=${PROJECT}&clan=${CLAN}" \
  2>/dev/null || echo "[]")

# Check for items (skip items with skip-labels)
ITEM_COUNT=$(echo "$INBOX_RESPONSE" | python3 -c "
import json, sys
try:
    items = json.load(sys.stdin)
    skip = {'parked', 'needs-approval', 'blocked', 'later', 'in-review'}
    actionable = [i for i in items if not set(i.get('labels', [])) & skip]
    print(len(actionable))
except:
    print(0)
" 2>/dev/null || echo "0")

# Check for corrections needing review
CORRECTIONS_FILE="$GROUP_DIR/self-improving/corrections.md"
PENDING_CORRECTIONS=0
if [ -f "$CORRECTIONS_FILE" ]; then
  PENDING_CORRECTIONS=$(grep -c 'Status:\*\* pending-review' "$CORRECTIONS_FILE" 2>/dev/null || echo "0")
fi

if [ "$ITEM_COUNT" -gt 0 ] || [ "$PENDING_CORRECTIONS" -gt 3 ]; then
  # Wake the agent with inbox data
  echo "{\"wakeAgent\": true, \"data\": {\"inboxItems\": $ITEM_COUNT, \"pendingCorrections\": $PENDING_CORRECTIONS, \"inbox\": $INBOX_RESPONSE}}"
else
  echo '{"wakeAgent": false}'
fi
