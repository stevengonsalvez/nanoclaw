# Fleet Setup Playbook — NanoClaw

> A replicable guide for standing up a two-agent NanoClaw fleet with shared infrastructure, cross-agent learning, Convex inbox integration, and character-driven identities.
>
> This playbook mirrors the structure of [Hermes Fleet Lambda Setup Playbook](https://github.com/NousResearch/hermes-agent) and documents the NanoClaw-native equivalents for each concept. Everything here is reproducible.

**Last updated**: 2026-04-16
**NanoClaw version**: latest (`main` branch)
**Runtime mode**: `native` (recommended) or `container`

---

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Identity Design](#2-identity-design)
3. [Fleet Infrastructure](#3-fleet-infrastructure)
4. [Agent Config Files](#4-agent-config-files)
5. [Group Provisioning](#5-group-provisioning)
6. [Service Launch](#6-service-launch)
7. [Post-Launch Verification](#7-post-launch-verification)
8. [File Inventory](#8-file-inventory)
9. [Implementation PRs](#9-implementation-prs)
10. [Port-Back Candidates](#10-port-back-candidates)
11. [Design Decisions & Hermes Comparison](#11-design-decisions--hermes-comparison)

---

## 1. Prerequisites

### Software

```bash
# Node.js 20+
node --version

# NanoClaw (cloned and built)
cd ~/d/git/nanoclaw && npm run build

# tmux (for service sessions)
tmux -V

# yq (for YAML parsing in heartbeat scripts)
yq --version

# curl (for inbox API calls)
curl --version

# Claude Code SDK (installed via agent-runner deps)
cd container/agent-runner && npm install
```

### Discord Setup

Create a Discord bot **per agent**:

1. Go to [discord.com/developers/applications](https://discord.com/developers/applications)
2. Create a new application for each agent (e.g. "Geordi", "Data")
3. Under each app's **Bot** tab: enable **Message Content Intent** and **Server Members Intent**
4. Copy the bot token — you will need it during provisioning
5. Generate an OAuth2 invite URL with `bot` scope and permissions: `Send Messages`, `Read Messages`, `Use Slash Commands`, `Add Reactions`, `Read Message History`
6. Invite both bots to your Discord server

### Convex Backend

You need a Convex deployment with an inbox API. The expected endpoints:

```
GET  /api/inbox/poll?agent=<name>&project=<project>&clan=<clan>
POST /api/inbox/ack      {"inboxId": "<id>", "agentId": "<name>"}
POST /api/inbox/complete {"inboxId": "<id>", "agentId": "<name>", "result": "summary"}
POST /api/inbox/create   {"agent": "<name>", "project": "<project>", "clan": "<clan>", "title": "...", "body": "..."}
```

Auth: `X-MC-Token` header. Store the token at `~/.secrets/mc-auth-token`.

### NanoClaw vs Hermes: Key Differences

| Concept | Hermes | NanoClaw |
|---------|--------|----------|
| Agent isolation | Separate processes (`HERMES_HOME`) | Shared orchestrator, separate group dirs |
| Identity loading | `SOUL.md` as Layer 1 system prompt | `CLAUDE.md` + `identity.md` + `soul.md` per group |
| Multi-bot Discord | Per-profile `DISCORD_BOT_TOKEN` | `DISCORD_BOTS=name:token:triggerName;...` env var |
| Hook system | Python plugins (`pre/post_llm_call`) | Claude Agent SDK hooks (`PreCompact`, `UserPromptSubmit`, `Stop`, etc.) |
| Session persistence | SQLite + compression + Honcho | Claude Agent SDK `sessionId` resume + per-group transcript archival |
| Model provider | Multi-provider (MiniMax, OpenAI, etc.) | Claude-only (via `CLAUDE_CODE_OAUTH_TOKEN`) |
| Skill system | Native skills + fleet-skills dir | 4-tier taxonomy (feature/utility/operational/container) |

---

## 2. Identity Design

### Character-to-Role Mapping

A fleet works better when each agent has a coherent personality. The character drives consistency: voice, priorities, decision style, and inter-agent dynamics.

**Methodology** (same as Hermes):

1. Define the role first (builder vs operator, generalist vs specialist)
2. Find a fictional archetype that embodies that role at the extreme
3. Extract operational traits from the archetype — not aesthetics, mechanics
4. Design the relationship between agents as complementary, not redundant

**Example — Fleet Lambda on NanoClaw:**

| Agent | Role | Archetype | Core trait |
|---|---|---|---|
| Geordi | Builder — ships features, owns first deploy | Lt. Cmdr. Geordi La Forge (TNG) | Prophet-builder, sees through the VISOR what others miss, finishes everything |
| Data | Operator — owns production, responds to incidents | Lt. Cmdr. Data (TNG) | Samurai-android, absolute autonomy, hunts problems pre-emptively |

The division is explicit: **Geordi owns idea → first deploy. Data owns first deploy → end of time.** Both own post-mortems. Neither duplicates the other.

### Identity File Roles (NanoClaw-specific)

NanoClaw uses a three-file identity stack per group, loaded as context from the group's working directory. This is more granular than Hermes's single `SOUL.md`.

| File | Purpose | NanoClaw path | Size guidance |
|---|---|---|---|
| `CLAUDE.md` | Core instructions — standing orders, fleet integration, tool priorities | `groups/{name}/CLAUDE.md` | 30–80 lines |
| `identity.md` | Role definition, origin story, visual identity | `groups/{name}/identity.md` | 50–100 lines |
| `soul.md` | Full personality — voice, values, mutation protocol, relationships | `groups/{name}/soul.md` | 80–150 lines |
| `HEARTBEAT.md` | Cron-triggered pre-flight and inbox protocol | `groups/{name}/HEARTBEAT.md` | 90–140 lines |
| `STANDING_ORDERS.md` | Fleet-wide non-negotiable rules (shared) | `groups/global/CLAUDE.md` | 200–300 lines |
| `agent-config.yaml` | Fleet runtime config (repos, Convex, routing) | `groups/{name}/agent-config.yaml` | 40–60 lines |
| `memories/MEMORY.md` | Bounded curated facts. Hard limit: 2,200 chars. | `groups/{name}/memories/MEMORY.md` | ~800 tokens |
| `memories/USER.md` | User profile. Hard limit: 1,375 chars. | `groups/{name}/memories/USER.md` | ~500 tokens |

**Hermes mapping:** Hermes loads `SOUL.md` as Layer 1 of system prompt. NanoClaw loads `CLAUDE.md` as the primary context file (auto-discovered by Claude Agent SDK from `cwd`), with `identity.md` and `soul.md` as additional files in the same directory. The SDK loads all `.md` files it finds — no explicit injection needed.

### What Goes in Each File

**CLAUDE.md** (operational — the "how"):
- Fleet integration (inbox protocol summary, ACP/1 reference)
- Tool priority order
- Safety rules (destructive actions, secrets, tmux)
- Correction logging format
- Config validation pre-flight reference
- Discord formatting conventions

**identity.md** (role — the "who"):
- Character name and archetype
- Role definition and scope
- Visual identity (avatar emoji, color palette)
- Relationship to partner agent and captain
- Origin story (optional, for voice consistency)

**soul.md** (personality — the "why"):
- Voice and register across situations
- The 2am test
- Values (4–6, ranked)
- Anti-patterns (NEVER list)
- Correction protocol
- 3-Failure Mutation Protocol (M1–M8)
- Personality fit to role (why this character for this job)

### ACP/1 — Agent Communication Protocol

Identical to Hermes. All agent-to-agent messages use structured headers:

```
[HEADER:value HEADER:value ...] [inbox:ID]
Body (1-3 sentences max). @mentions
```

Common headers: `PR:<num>`, `STATE:<state>`, `GATE:<gate>=<result>`, `ACTION:<verb>`, `TARGET:<agent>`, `BLOCKER:<desc>`, `ACK`.

Rules: max 3 sentences in body, don't restate headers, headers are truth. This is prompt-enforced via STANDING_ORDERS in `groups/global/CLAUDE.md`.

### 3-Failure Mutation Protocol

Same as Hermes. Documented in each agent's `soul.md`:

- M1: Reduce Scope
- M2: Change Substrate
- M3: Invert the Problem
- M4: Ask (escalate)
- M5: Decompose
- M6: Reference Implementation
- M7: Simplify State
- M8: Hard Reset

### Anti-Sycophancy: `_thinkingoutloud`

Messages ending with `_thinkingoutloud` are exempt from inbox ID, tagging, and loop-detection rules. Agents can post work-in-progress without governance overhead. Enforced via STANDING_ORDERS.

---

## 3. Fleet Infrastructure

### Working Directory Layout

NanoClaw decouples **code** from **runtime data**:

```
~/d/git/nanoclaw/          # Code (git-managed, shared across installs)
├── src/                   # Orchestrator source
├── container/             # Agent runner + container skills
│   ├── agent-runner/      # Claude Agent SDK integration
│   └── skills/            # Container skills (synced to agent on startup)
└── dist/                  # Compiled output

~/.nanoclaw/               # Runtime data (per-install, NOT git-managed)
├── .env                   # Secrets + config
├── store/                 # SQLite database
├── groups/                # Per-agent group directories
│   ├── global/            # Shared context (mounted read-only to all agents)
│   │   └── CLAUDE.md      # Fleet-wide STANDING_ORDERS
│   ├── main/              # Agent 1 (e.g. Geordi)
│   │   ├── CLAUDE.md
│   │   ├── identity.md
│   │   ├── soul.md
│   │   ├── HEARTBEAT.md
│   │   ├── agent-config.yaml
│   │   ├── memories/
│   │   │   ├── MEMORY.md
│   │   │   └── USER.md
│   │   ├── self-improving/
│   │   │   ├── corrections.md
│   │   │   └── journals/
│   │   ├── conversations/   # Archived transcripts (auto-generated)
│   │   └── logs/            # Agent run logs
│   └── data/              # Agent 2 (e.g. Data)
│       └── (same structure as main)
├── data/                  # Session persistence
│   └── sessions/          # Per-group Claude SDK sessions
└── container → ~/d/git/nanoclaw/container  # Symlink
```

### Shared Learnings Store

Create at `~/.clan/learnings/` (shared across all harnesses — Hermes, NanoClaw, etc.):

```bash
mkdir -p ~/.clan/learnings/{templates,metrics,scripts}
touch ~/.clan/learnings/patterns.jsonl
touch ~/.clan/learnings/discoveries.jsonl
touch ~/.clan/learnings/corrections-raw.jsonl
```

**Pattern entry schema:**

```json
{
  "id": "p-<agent>-<unix-ms>",
  "uuid": "<uuidv4>",
  "agent": "<agent-name>",
  "harness": "nanoclaw",
  "clan": "lambda",
  "ts": "2026-04-16T00:00:00Z",
  "category": "architecture",
  "title": "Short title",
  "problem": "What situation triggers this",
  "solution": "What works",
  "tags": ["tag1", "tag2"],
  "status": "active",
  "supersedes": null
}
```

Fields `harness` and `agent` enable cross-fleet filtering. The `uuid` field is used for dedup when multiple harnesses write to the same file.

**`append-pattern` script** — atomic append with flock (same as Hermes):

```python
#!/usr/bin/env python3
"""Atomic append to patterns.jsonl with flock guard."""
import fcntl, json, sys, os, uuid

PATTERNS_FILE = os.path.expanduser("~/.clan/learnings/patterns.jsonl")

def main():
    if len(sys.argv) < 2:
        print("Usage: append-pattern '<json>'", file=sys.stderr)
        sys.exit(1)
    entry = json.loads(sys.argv[1])
    required = ["id", "agent", "category", "title", "problem", "solution"]
    missing = [f for f in required if f not in entry]
    if missing:
        print(f"Missing: {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)
    entry.setdefault("uuid", str(uuid.uuid4()))
    entry.setdefault("clan", "lambda")
    entry.setdefault("harness", "nanoclaw")
    entry.setdefault("status", "active")
    entry.setdefault("supersedes", None)
    line = json.dumps(entry, separators=(",", ":")) + "\n"
    with open(PATTERNS_FILE, "a") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        try:
            f.write(line)
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)
    print(f"Appended: {entry['id']}")

if __name__ == "__main__":
    main()
```

Save to `~/.clan/learnings/scripts/append-pattern` and make executable.

### SKILL-PROPOSAL.md Template

Save at `~/.clan/learnings/templates/SKILL-PROPOSAL.md`. Same as Hermes — detection heuristics, proposal format, cross-review protocol, validation scoring, 30-day monitoring.

---

## 4. Agent Config Files

### agent-config.yaml

Per-group fleet runtime config. Separate from NanoClaw's `.env` (which is for NanoClaw orchestrator settings). The agent reads this as a context file from its group directory.

**Agent 1 (builder — app-biased routing):**

```yaml
# agent-config.yaml — Builder Agent Runtime Configuration
project: lambda
fleet: nanoclaw
clan: lambda
channel: general
channel_id: "<YOUR_DISCORD_CHANNEL_SNOWFLAKE>"
repos:
  - <your-org>/<app-repo>
  - <your-org>/<dashboard-repo>
  - <your-org>/<platform-repo>
convex_url: https://<your-deployment>.convex.site
github_project: <number>
issue_ref_required: true
issue_ref_format: "<repo>#<number>"
inbox_poll_interval: 60
default_repo: <your-org>/<app-repo>
default_branch: main
issue_tracker_url: https://github.com/<your-org>/<app-repo>/issues

repo_routing:
  app:
    repo: <your-org>/<app-repo>
    match: [app, UI, features, migrations, frontend, components]
  dashboard:
    repo: <your-org>/<dashboard-repo>
    match: [dashboard, pipeline, inbox, fleet status, mission control]
  platform:
    repo: <your-org>/<platform-repo>
    match: [infra, GCP, VM, cloudflared, NixOS, provisioning, secrets]
```

**Agent 2 (operator — infra-biased routing):** Same structure, swap `default_repo` to infra and adjust `repo_routing` match keywords.

### HEARTBEAT.md

Per-group heartbeat protocol. Triggered by a scheduled task at `inbox_poll_interval` seconds.

```markdown
# HEARTBEAT — <Agent Name>

Triggered every 60 seconds by scheduled task. Also run on session start.

---

## 1. Pre-flight Check (every heartbeat, every session start)

1. Read `agent-config.yaml` from workspace root
2. If missing or invalid: reply "<Agent> offline" and STOP
3. Validate required fields: project, fleet, clan, channel_id, repos, convex_url

---

## 2. Session Health Check (every heartbeat)

Claude Agent SDK handles compaction automatically at ~165K tokens. Monitor for:
1. If the session feels sluggish, note it in `_thinkingoutloud`
2. During incidents: context grows fast. Be aware of bloat.
3. PreCompact hook archives full transcript to `conversations/` before compaction

---

## 3. Inbox Poll (every heartbeat — PRIORITY ORDERING)

```bash
curl -s -H "X-MC-Token: $(cat ~/.secrets/mc-auth-token)" \
  "$(yq '.convex_url' agent-config.yaml)/api/inbox/poll?agent=<name>&project=lambda&clan=lambda"
```

Priority ordering:
1. **Incidents** (sev-1, sev-2): ACK immediately, drop all other work
2. **Deploy/handoff**: ACK, review artifact, execute or feedback
3. **Routine**: ACK, work, complete

Skip labels: `parked`, `needs-approval`, `blocked`, `later`, `in-review`

If empty: reply `HEARTBEAT_OK`

---

## 4. Cross-Agent Learnings Check (session start only)

```bash
grep -A5 "<relevant-topic>" ~/.clan/learnings/patterns.md 2>/dev/null
```

Apply relevant patterns. Pay attention to `security` and `config` categories.

---

## 5. Self-Improving Protocol (session start only)

1. Load `self-improving/corrections.md` — apply patterns
2. Load `memories/MEMORY.md` — hardened rules, always apply
3. If corrections.md > 200 lines: consolidate
4. If picking up a worktree from partner agent: read JOURNAL.md last 50 lines
```

Operator agents should add an **Incident Response Protocol** section (identical to Hermes Motoko's) and a **Monitoring Awareness** section for passive heartbeat checks.

### MEMORY.md Seeds

Stay under 2,200 chars. Use `§` as separator.

```
Fleet: Lambda. Partner: <partner-name> (<role>). Captain: <human-name>.
§
Convex inbox at <deployment>.convex.site. Agent ID: <name>. Clan: lambda.
§
Primary repos: <repo-list>.
§
Heartbeat: 60s scheduled task. Shared learnings at ~/.clan/learnings/.
§
Write patterns with clan: "lambda", harness: "nanoclaw". Read all clans.
```

### USER.md Seeds

Stay under 1,375 chars.

```
<Name> — senior engineer/operator. Hands-on. Reads every line of code. Will call out BS instantly.
§
Prefers: demos over decks, shipped code over proposals, direct answers over hedging.
§
Challenge honestly when you disagree — once, clearly, with reasoning. If still wants it their way, ship their way and note the disagreement.
§
Protect their time. No unnecessary interruptions. If it can wait, it waits.
```

---

## 5. Group Provisioning

### Create Group Directories

```bash
NANOCLAW_HOME=~/.nanoclaw

# Agent 1 (builder)
AGENT1=main
mkdir -p "$NANOCLAW_HOME/groups/$AGENT1"/{memories,self-improving/journals,conversations,logs}

# Agent 2 (operator)
AGENT2=data
mkdir -p "$NANOCLAW_HOME/groups/$AGENT2"/{memories,self-improving/journals,conversations,logs}

# Global (shared standing orders)
mkdir -p "$NANOCLAW_HOME/groups/global"
```

### Seed Identity Files

```bash
for AGENT in "$AGENT1" "$AGENT2"; do
  GROUP_DIR="$NANOCLAW_HOME/groups/$AGENT"

  # Copy identity stack (from your staging area or write fresh)
  # CLAUDE.md, identity.md, soul.md — see Section 2 for content
  # HEARTBEAT.md — see Section 4
  # agent-config.yaml — see Section 4

  touch "$GROUP_DIR/self-improving/corrections.md"
done

# Global standing orders
# Write groups/global/CLAUDE.md with fleet-wide rules (ACP/1, inbox protocol, etc.)
```

### NanoClaw .env Setup

Write `~/.nanoclaw/.env`:

```bash
# Runtime mode
RUNTIME_MODE=native

# Assistant name (used as default trigger)
ASSISTANT_NAME=<primary-agent-name>

# Timezone
TZ=Europe/London

# Claude credentials (direct — no OneCLI gateway in native mode)
CLAUDE_CODE_OAUTH_TOKEN=<your-token>

# Multi-bot Discord
# Format: name:token:triggerName separated by semicolons
# Each bot gets its own JID prefix (dc-{name}:) and trigger injection
DISCORD_BOTS=<agent1-name>:<BOT_TOKEN_1>:<TriggerName1>;<agent2-name>:<BOT_TOKEN_2>:<TriggerName2>
```

**Hermes mapping:** Hermes uses per-profile `.env` files. NanoClaw uses a single `.env` with the `DISCORD_BOTS` env var encoding multiple bots. Bot names must be alphanumeric + hyphens (validated by `/^[a-z0-9-]+$/i`).

### Register Groups in Discord

After first launch, register each Discord channel for each agent:

```
@<Agent1> /register
@<Agent2> /register
```

Or register via SQLite directly:

```sql
INSERT INTO registered_groups (jid, name, folder, is_main)
VALUES ('dc-<agent1-name>:<channel_id>', '<display-name>', '<group-folder>', 0);
```

Set `is_main=0` for all agents to ensure trigger-only responses (agents only respond when @mentioned).

### Convex Auth Token

```bash
mkdir -p ~/.secrets
# Write your mission-control auth token
echo "<your-token>" > ~/.secrets/mc-auth-token
chmod 600 ~/.secrets/mc-auth-token
```

---

## 6. Service Launch

### macOS (launchd)

Write `~/Library/LaunchAgents/com.nanoclaw.plist`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>com.nanoclaw</string>
  <key>ProgramArguments</key>
  <array>
    <string>/path/to/node</string>
    <string>/path/to/nanoclaw/dist/index.js</string>
  </array>
  <key>WorkingDirectory</key>
  <string>/Users/<you>/.nanoclaw</string>
  <key>EnvironmentVariables</key>
  <dict>
    <key>PATH</key>
    <string>/usr/local/bin:/usr/bin:/bin</string>
    <key>RUNTIME_MODE</key>
    <string>native</string>
  </dict>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>StandardOutPath</key>
  <string>/Users/<you>/.nanoclaw/nanoclaw.log</string>
  <key>StandardErrorPath</key>
  <string>/Users/<you>/.nanoclaw/nanoclaw.err</string>
</dict>
</plist>
```

```bash
launchctl load ~/Library/LaunchAgents/com.nanoclaw.plist
```

### Linux (systemd)

```bash
systemctl --user start nanoclaw
```

### Manual (tmux — for development)

```bash
SESSION="nanoclaw-$(date +%s)"
tmux new-session -d -s "$SESSION"
tmux send-keys -t "$SESSION" \
  "cd ~/.nanoclaw && node ~/d/git/nanoclaw/dist/index.js 2>&1 | tee nanoclaw.log" C-m

echo "NanoClaw running: tmux attach -t $SESSION"
```

NanoClaw is a **single process** that manages all agents. Unlike Hermes (which runs separate gateway processes per profile), NanoClaw routes all Discord messages through one orchestrator. The `DISCORD_BOTS` env var tells it to connect multiple bot tokens simultaneously.

---

## 7. Post-Launch Verification

### Checklist

```
[ ] NanoClaw process starts without "No channels connected" fatal
[ ] Both Discord bots show "Connected as <BotName>" in logs
[ ] JID prefixes are distinct (dc-<agent1>: and dc-<agent2>:)
[ ] @mention Agent 1 → responds in Agent 1's voice
[ ] @mention Agent 2 → responds in Agent 2's voice
[ ] Reply to Agent 1's message → triggers Agent 1 (not Agent 2)
[ ] Unregistered channels are logged but ignored
[ ] groups/<agent1>/CLAUDE.md is loaded (check agent response style)
[ ] groups/<agent2>/CLAUDE.md is loaded
[ ] groups/global/CLAUDE.md is loaded by both agents
[ ] Scheduled tasks visible in store/nanoclaw.db (if heartbeat is configured)
```

### Test Cross-Agent Communication

In Discord, @mention Agent 1 with a task that references Agent 2:

```
@Agent1 build a /health endpoint and flag @Agent2 for deploy review
```

Verify:
- Agent 1 responds in its voice (builder, technical)
- Agent 1 uses ACP/1 headers when referencing Agent 2
- Both agents use `[inbox:ID]` tags for traceability

### Test Inbox Round-Trip

```bash
curl -X POST \
  -H "Content-Type: application/json" \
  -H "X-MC-Token: $(cat ~/.secrets/mc-auth-token)" \
  "https://<your-deployment>.convex.site/api/inbox/create" \
  -d '{
    "agent": "<agent1-name>",
    "project": "lambda",
    "clan": "lambda",
    "title": "Inbox integration test",
    "body": "Reply with INBOX_TEST_OK to confirm inbox poll is working."
  }'
```

Wait for next heartbeat cycle, then verify the agent ACKs and responds.

---

## 8. File Inventory

### Shared Infrastructure

```
~/.clan/learnings/
├── patterns.jsonl                 # Runtime — grows with agent learning
├── patterns.md                    # Generated — rebuilt by render-patterns
├── discoveries.jsonl              # Runtime
├── corrections-raw.jsonl          # Runtime
├── templates/
│   └── SKILL-PROPOSAL.md          # Cross-review skill pipeline template
├── metrics/
│   └── skill-usage.jsonl          # 30-day monitoring data
└── scripts/
    ├── append-pattern             # Atomic JSONL append (Python)
    └── render-patterns            # Generates patterns.md from jsonl

~/.secrets/
└── mc-auth-token                  # Convex auth token (chmod 600)
```

### NanoClaw Runtime

```
~/.nanoclaw/
├── .env                           # Secrets + config (DISCORD_BOTS, CLAUDE_CODE_OAUTH_TOKEN)
├── store/
│   └── nanoclaw.db                # SQLite — messages, groups, sessions, scheduled_tasks
├── groups/
│   ├── global/
│   │   └── CLAUDE.md              # Fleet-wide STANDING_ORDERS (~200-300 lines)
│   ├── main/                      # Agent 1 (builder)
│   │   ├── CLAUDE.md              # Agent instructions (~40 lines)
│   │   ├── identity.md            # Role + origin (~55 lines)
│   │   ├── soul.md                # Personality + values (~110 lines)
│   │   ├── HEARTBEAT.md           # Cron protocol (~90 lines)
│   │   ├── agent-config.yaml      # Fleet routing (~45 lines)
│   │   ├── memories/
│   │   │   ├── MEMORY.md          # Bounded facts (~13 lines seed)
│   │   │   └── USER.md            # User profile (~7 lines seed)
│   │   ├── self-improving/
│   │   │   ├── corrections.md     # Auto-detected corrections
│   │   │   └── journals/          # Archived JOURNAL.md files
│   │   ├── conversations/         # Archived transcripts (auto, PreCompact hook)
│   │   └── logs/                  # Agent run logs
│   └── data/                      # Agent 2 (operator) — same structure
├── data/
│   └── sessions/                  # Per-group Claude SDK sessions
│       ├── main/
│       │   └── .claude/           # Session state (settings.json, transcripts)
│       └── data/
│           └── .claude/
├── container → ~/d/git/nanoclaw/container   # Symlink
└── scripts → ~/d/git/nanoclaw/scripts       # Symlink
```

### NanoClaw Code

```
~/d/git/nanoclaw/
├── src/
│   ├── index.ts                   # Orchestrator (state, message loop, agent invocation)
│   ├── channels/
│   │   ├── registry.ts            # Self-registering channel system
│   │   └── discord.ts             # Multi-bot Discord (DISCORD_BOTS env var)
│   ├── router.ts                  # Message formatting + outbound routing
│   ├── native-runner.ts           # Spawns agent-runner as native child process
│   ├── container-runner.ts        # Spawns agent-runner in Docker container
│   ├── task-scheduler.ts          # Cron/interval task execution
│   ├── db.ts                      # SQLite (messages, groups, sessions, tasks)
│   └── config.ts                  # Trigger pattern, paths, intervals
├── container/
│   ├── agent-runner/
│   │   └── src/
│   │       ├── index.ts           # Claude Agent SDK query loop + hooks
│   │       └── ipc-mcp-stdio.ts   # IPC ↔ MCP bridge
│   └── skills/                    # Container skills (synced to agent on startup)
└── CONTRIBUTING.md                # Skill taxonomy documentation
```

---

## 9. Implementation PRs

Reaching full Fleet Lambda parity requires these NanoClaw-side changes. None require forking the Claude Agent SDK.

### PR1 — Wire 5 Additional SDK Hooks (~600 LOC) — HIGH

**File:** `container/agent-runner/src/index.ts`

Currently only `PreCompact` is wired (line 636). Add:

```typescript
hooks: {
  PreCompact: [
    { hooks: [createPreCompactHook(containerInput.assistantName)] },
  ],
  UserPromptSubmit: [
    { hooks: [createSessionRulesHook(), createManifestContextHook()] },
  ],
  Stop: [
    { hooks: [createInboxEnforcerHook(), createLearningSyncHook(), createLearningVerifierHook()] },
  ],
}
```

| Hook | SDK event | Purpose | Determinism |
|---|---|---|---|
| `createSessionRulesHook()` | `UserPromptSubmit` | Inject STANDING_ORDERS before model sees prompt | Real-time, blocking |
| `createManifestContextHook()` | `UserPromptSubmit` (first turn) | Load `agent-config.yaml` → inject repo context | Real-time, blocking |
| `createInboxEnforcerHook()` | `Stop` | Scan assistant response for untagged @mentions | Real-time, blocking |
| `createLearningSyncHook()` | `Stop` | Detect corrections.md changes → append to patterns.jsonl | Real-time, post-response |
| `createLearningVerifierHook()` | `Stop` | FTS5 search patterns.jsonl for missed knowledge | Real-time, post-response |

**Key advantage over Hermes:** Hermes uses Python `pre/post_llm_call` hooks. NanoClaw uses the Claude Agent SDK's native hook events, which provide typed inputs (transcript path, session ID, tool use context) without custom parsing.

### PR2 — Per-Group Memory Loader (~30 LOC) — HIGH

**File:** `container/agent-runner/src/index.ts`

In `runQuery()` (line 585), extend the `systemPrompt` to include `memories/MEMORY.md` and `memories/USER.md`:

```typescript
const memoryPath = `${WORKSPACE_GROUP}/memories/MEMORY.md`;
const userPath = `${WORKSPACE_GROUP}/memories/USER.md`;
let appendContent = globalClaudeMd || '';
if (fs.existsSync(memoryPath)) appendContent += '\n' + fs.readFileSync(memoryPath, 'utf-8');
if (fs.existsSync(userPath)) appendContent += '\n' + fs.readFileSync(userPath, 'utf-8');
```

### PR3 — Convex Inbox Client + Poll Task (~150 LOC) — HIGH

**New file:** `container/agent-runner/src/inbox/convex-client.ts`

Thin HTTP client:

```typescript
interface ConvexInboxClient {
  pollInbox(agent: string, project: string, clan: string): Promise<InboxItem[]>;
  ackItem(inboxId: string, agentId: string): Promise<void>;
  completeItem(inboxId: string, agentId: string, result: string): Promise<void>;
  postEvent(event: AgentEvent): Promise<void>;
}
```

- Read `convex_url` from `agent-config.yaml` (loaded as context file)
- Read auth token from `~/.secrets/mc-auth-token`
- `postEvent()`: fire-and-forget POST to `/api/events` with `X-MC-Token` header, 3s timeout
- `pollInbox()`: GET from `/api/inbox/poll`, parse response, filter by skip-labels

**New file:** `container/agent-runner/src/inbox/poll-task.ts`

Registered as a scheduled task type in `src/task-scheduler.ts`:

```typescript
// Insert heartbeat task per group
insertTask({
  group_folder: '<agent-folder>',
  chat_jid: '<agent-jid>',
  prompt: 'HEARTBEAT — Run pre-flight from HEARTBEAT.md',
  schedule_type: 'interval',
  schedule_value: '60000',
  status: 'active',
});
```

**Wire `Stop` hook** to emit `message:sent` lifecycle event via `postEvent()`. Wire `SessionStart`/`SessionEnd` for lifecycle tracking.

### PR4 — Heartbeat Synthetic-Message Injection (~200 LOC) — HIGH

**File:** `src/task-scheduler.ts`

Add a `heartbeat` task type that:
1. Runs the script phase (inbox poll, pre-flight check)
2. If `wakeAgent=true`, injects a synthetic message into the agent's IPC input
3. Agent wakes, reads HEARTBEAT.md, follows protocol

This already works with the existing `script` field on scheduled tasks (line 803 of agent-runner). The script phase returns `{ wakeAgent: true, data: { inbox: [...] } }` and the prompt includes the HEARTBEAT instructions.

### PR5 — Skill-Level Hook Registration (~80 LOC) — MEDIUM

**File:** `container/agent-runner/src/index.ts`

Extend the skill loader to accept a `hooks` block in skill frontmatter:

```yaml
---
name: inbox-enforcer
description: Validates [inbox:ID] tags in outbound messages
hooks:
  Stop: true
---
```

When building the hooks config for `query()`, merge skill-declared hooks with the built-in ones.

---

## 10. Port-Back Candidates

Features NanoClaw does better than Hermes, with evidence for porting back.

### 1. Skill Taxonomy (4-tier)

**Evidence:** `CONTRIBUTING.md:22-94`

NanoClaw explicitly classifies skills into feature (branch-based), utility (code+SKILL.md), operational (instruction-only), and container (agent runtime). Each type has clear guidelines, location conventions, and contributing rules.

**Hermes equivalent:** Flat — skills are just skills. No taxonomy, no location conventions.

**Port-back:** Add a `SKILL_TYPES.md` to Hermes docs. Classify existing skills. Enforce via CONTRIBUTING guidelines.

### 2. Self-Registering Channel System

**Evidence:** `src/channels/registry.ts:16-20`

```typescript
const registry = new Map<string, ChannelFactory>();
export function registerChannel(name: string, factory: ChannelFactory): void {
  registry.set(name, factory);
}
```

Each channel module (Discord, Telegram, Slack, Gmail) calls `registerChannel()` at import time. Adding a new channel is a branch merge — no orchestrator changes needed.

**Hermes equivalent:** Discord-only, hardcoded in gateway.

**Port-back:** Abstract Hermes gateway into a channel registry. Enable Telegram/Slack/etc. via plugins.

### 3. Per-Group Identity Triple (CLAUDE.md + identity.md + soul.md)

**Evidence:** `~/.nanoclaw/groups/main/{CLAUDE.md,identity.md,soul.md}` (38+54+109 lines)

Three files separate operational instructions, role definition, and personality. Claude Agent SDK auto-discovers all `.md` files in the group directory. This is finer-grained than Hermes's single `SOUL.md` which mixes all three concerns.

**Hermes equivalent:** `SOUL.md` (442 lines) contains everything.

**Port-back:** Split Hermes `SOUL.md` into the triple. Less monolithic, easier to maintain.

### 4. Claude Agent SDK Session Resumption

**Evidence:** `container/agent-runner/src/index.ts:590-591`

```typescript
resume: sessionId,
resumeSessionAt: resumeAt,
```

And `src/db.ts` SQLite `sessions` table for persistence.

Sessions resume natively across turns. No custom compaction or context rebuilding logic needed. The SDK handles it.

**Hermes equivalent:** Manual session management with SQLite `state.db`, custom compression, Honcho for long-term memory.

**Port-back:** If Hermes adopts Claude Agent SDK backend, it gains native session persistence for free.

### 5. Single-Process Multi-Agent

**Evidence:** `src/index.ts` orchestrator + `DISCORD_BOTS` env var

NanoClaw runs all agents in one process. Lower resource footprint (~150 MB vs ~310 MB for two Hermes gateway processes). Simpler operations — one launchd/systemd unit, one log stream, one restart.

**Hermes equivalent:** Separate process per profile.

**Port-back:** Consider a unified gateway mode in Hermes that multiplexes profiles.

---

## 11. Design Decisions & Hermes Comparison

**Why single process instead of one per agent?**
NanoClaw's orchestrator handles message routing, session management, and agent spawning for all groups. Each agent still runs as an isolated child process (native-runner spawns a separate `tsx` process per query). The orchestrator is the coordinator, not the executor. Cost: simpler ops. Risk: orchestrator crash takes down all agents (mitigated by launchd `KeepAlive`).

**Why three identity files instead of one SOUL.md?**
Separation of concerns. CLAUDE.md changes often (tool config, standing orders). identity.md changes rarely (role, archetype). soul.md almost never changes (personality, values). Hermes's 442-line SOUL.md mixes all three, making diffs noisy and reviews harder.

**Why Claude-only instead of multi-provider?**
NanoClaw uses the Claude Agent SDK, which provides native tool use, session resumption, hooks, and agent teams. Multi-provider would require abstracting away these features. For fleets that need MiniMax/OpenAI fallback, Hermes is the better harness.

**Why `DISCORD_BOTS` env var instead of per-profile config?**
NanoClaw's single-process model means all bot tokens live in one `.env`. The `DISCORD_BOTS=name:token:triggerName;...` format is compact and validates names against `/^[a-z0-9-]+$/i` to prevent JID collisions. Each bot gets a distinct JID prefix (`dc-{name}:`) and trigger pattern.

**Why shared `~/.clan/learnings/` across harnesses?**
Cross-fleet learning is the whole point. A pattern Geordi discovers on NanoClaw is immediately available to Freeman on Hermes, and vice versa. The `harness` and `agent` fields enable filtering without requiring separate stores. UUID-based dedup handles concurrent writes from both harnesses.

**Why Convex for inbox instead of same-channel multi-group routing?**
Discord's per-bot-token delivery means Agent 1 and Agent 2 can both see messages in the same channel, but the NanoClaw registry binds each channel to one group. Routing messages to multiple groups would create a second source of truth and complicate trigger semantics. Convex provides a clean, external coordination layer that both harnesses already use.

---

*Fleet Lambda on NanoClaw — same learning loop, different engine.*
