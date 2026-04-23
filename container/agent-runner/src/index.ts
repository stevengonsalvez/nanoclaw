/**
 * NanoClaw Agent Runner
 * Runs inside a container, receives config via stdin, outputs result to stdout
 *
 * Input protocol:
 *   Stdin: Full ContainerInput JSON (read until EOF, like before)
 *   IPC:   Follow-up messages written as JSON files to /workspace/ipc/input/
 *          Files: {type:"message", text:"..."}.json — polled and consumed
 *          Sentinel: /workspace/ipc/input/_close — signals session end
 *
 * Stdout protocol:
 *   Each result is wrapped in OUTPUT_START_MARKER / OUTPUT_END_MARKER pairs.
 *   Multiple results may be emitted (one per agent teams result).
 *   Final marker after loop ends signals completion.
 */

import fs from 'fs';
import path from 'path';
import { execFile } from 'child_process';
import {
  query,
  HookCallback,
  PreCompactHookInput,
  UserPromptSubmitHookInput,
  StopHookInput,
  SessionStartHookInput,
} from '@anthropic-ai/claude-agent-sdk';
import { fileURLToPath } from 'url';
import {
  loadConvexConfig,
  postEvent,
  postMetric,
  pollInbox,
  prioritizeItems,
  ACPMetrics,
} from './inbox/convex-client.js';
import { readRecentDiscoveries } from './inbox/discoveries.js';

interface ContainerInput {
  prompt: string;
  sessionId?: string;
  groupFolder: string;
  chatJid: string;
  isMain: boolean;
  isScheduledTask?: boolean;
  assistantName?: string;
  script?: string;
}

interface ContainerOutput {
  status: 'success' | 'error';
  result: string | null;
  newSessionId?: string;
  error?: string;
}

interface SessionEntry {
  sessionId: string;
  fullPath: string;
  summary: string;
  firstPrompt: string;
}

interface SessionsIndex {
  entries: SessionEntry[];
}

interface SDKUserMessage {
  type: 'user';
  message: { role: 'user'; content: string };
  parent_tool_use_id: null;
  session_id: string;
}

// Paths are configurable via env vars for native (non-container) execution.
// In containers these default to the standard /workspace layout.
const WORKSPACE_GROUP = process.env.NANOCLAW_WORKSPACE_GROUP || '/workspace/group';
const WORKSPACE_GLOBAL = process.env.NANOCLAW_WORKSPACE_GLOBAL || '/workspace/global';
const WORKSPACE_EXTRA = process.env.NANOCLAW_WORKSPACE_EXTRA || '/workspace/extra';
const WORKSPACE_PROJECT = process.env.NANOCLAW_WORKSPACE_PROJECT || '/workspace/project';
const CLAUDE_HOME = process.env.NANOCLAW_CLAUDE_HOME || '/home/node/.claude';
const IPC_INPUT_DIR = process.env.NANOCLAW_IPC_INPUT || '/workspace/ipc/input';
const IPC_INPUT_CLOSE_SENTINEL = path.join(IPC_INPUT_DIR, '_close');
const IPC_POLL_MS = 500;

/**
 * Push-based async iterable for streaming user messages to the SDK.
 * Keeps the iterable alive until end() is called, preventing isSingleUserTurn.
 */
class MessageStream {
  private queue: SDKUserMessage[] = [];
  private waiting: (() => void) | null = null;
  private done = false;

  push(text: string): void {
    this.queue.push({
      type: 'user',
      message: { role: 'user', content: text },
      parent_tool_use_id: null,
      session_id: '',
    });
    this.waiting?.();
  }

  end(): void {
    this.done = true;
    this.waiting?.();
  }

  async *[Symbol.asyncIterator](): AsyncGenerator<SDKUserMessage> {
    while (true) {
      while (this.queue.length > 0) {
        yield this.queue.shift()!;
      }
      if (this.done) return;
      await new Promise<void>((r) => {
        this.waiting = r;
      });
      this.waiting = null;
    }
  }
}

async function readStdin(): Promise<string> {
  return new Promise((resolve, reject) => {
    let data = '';
    process.stdin.setEncoding('utf8');
    process.stdin.on('data', (chunk) => {
      data += chunk;
    });
    process.stdin.on('end', () => resolve(data));
    process.stdin.on('error', reject);
  });
}

const OUTPUT_START_MARKER = '---NANOCLAW_OUTPUT_START---';
const OUTPUT_END_MARKER = '---NANOCLAW_OUTPUT_END---';

function writeOutput(output: ContainerOutput): void {
  console.log(OUTPUT_START_MARKER);
  console.log(JSON.stringify(output));
  console.log(OUTPUT_END_MARKER);
}

function log(message: string): void {
  console.error(`[agent-runner] ${message}`);
}

function getSessionSummary(
  sessionId: string,
  transcriptPath: string,
): string | null {
  const projectDir = path.dirname(transcriptPath);
  const indexPath = path.join(projectDir, 'sessions-index.json');

  if (!fs.existsSync(indexPath)) {
    log(`Sessions index not found at ${indexPath}`);
    return null;
  }

  try {
    const index: SessionsIndex = JSON.parse(
      fs.readFileSync(indexPath, 'utf-8'),
    );
    const entry = index.entries.find((e) => e.sessionId === sessionId);
    if (entry?.summary) {
      return entry.summary;
    }
  } catch (err) {
    log(
      `Failed to read sessions index: ${err instanceof Error ? err.message : String(err)}`,
    );
  }

  return null;
}

/**
 * Archive the full transcript to conversations/ before compaction.
 */
function createPreCompactHook(assistantName?: string): HookCallback {
  return async (input, _toolUseId, _context) => {
    const preCompact = input as PreCompactHookInput;
    const transcriptPath = preCompact.transcript_path;
    const sessionId = preCompact.session_id;

    if (!transcriptPath || !fs.existsSync(transcriptPath)) {
      log('No transcript found for archiving');
      return {};
    }

    try {
      const content = fs.readFileSync(transcriptPath, 'utf-8');
      const messages = parseTranscript(content);

      if (messages.length === 0) {
        log('No messages to archive');
        return {};
      }

      const summary = getSessionSummary(sessionId, transcriptPath);
      const name = summary ? sanitizeFilename(summary) : generateFallbackName();

      const conversationsDir = `${WORKSPACE_GROUP}/conversations`;
      fs.mkdirSync(conversationsDir, { recursive: true });

      const date = new Date().toISOString().split('T')[0];
      const filename = `${date}-${name}.md`;
      const filePath = path.join(conversationsDir, filename);

      const markdown = formatTranscriptMarkdown(
        messages,
        summary,
        assistantName,
      );
      fs.writeFileSync(filePath, markdown);

      log(`Archived conversation to ${filePath}`);
    } catch (err) {
      log(
        `Failed to archive transcript: ${err instanceof Error ? err.message : String(err)}`,
      );
    }

    return {};
  };
}

// ---------------------------------------------------------------------------
// Fleet hooks — session-rules, manifest-context, inbox-enforcer,
// learning-sync, learning-verifier
// ---------------------------------------------------------------------------

// Critical rules included in the condensed STANDING_ORDERS injection
// (turns after the first). Full orders load on turn 1 to save tokens thereafter.
const CONDENSED_RULE_PREFIXES = ['Rule 1', 'Rule 2', 'Rule 8', 'Rule 10', 'Rule 15'];

let _fullStandingOrders = '';
let _condensedStandingOrders = '';
let _standingOrdersLoaded = false;
let _seenSessions = new Set<string>();

function loadStandingOrders(): void {
  if (_standingOrdersLoaded) return;
  _standingOrdersLoaded = true;

  // Prefer dedicated STANDING_ORDERS.md, fall back to global CLAUDE.md
  const candidates = [
    `${WORKSPACE_GLOBAL}/STANDING_ORDERS.md`,
    `${WORKSPACE_GLOBAL}/CLAUDE.md`,
  ];
  let content = '';
  for (const p of candidates) {
    if (fs.existsSync(p)) {
      content = fs.readFileSync(p, 'utf-8');
      break;
    }
  }
  if (!content) return;

  _fullStandingOrders = content;

  // Build condensed version — keep only the critical rules listed above
  const lines = content.split('\n');
  const condensedParts: string[] = [
    '# Standing Orders (condensed — critical rules only)',
    '',
  ];
  let inCritical = false;
  for (const line of lines) {
    if (line.startsWith('## Rule ')) {
      const label = line.split(':')[0].replace('## ', '').trim();
      inCritical = CONDENSED_RULE_PREFIXES.some((p) => label.startsWith(p));
      if (inCritical) condensedParts.push(line);
    } else if (inCritical) {
      if (line.startsWith('## Rule ') || line.startsWith('---')) {
        inCritical = false;
      } else {
        condensedParts.push(line);
      }
    }
  }
  _condensedStandingOrders = condensedParts.join('\n').trim();
  if (!_condensedStandingOrders) {
    _condensedStandingOrders = content.slice(0, 500) + '\n\n[...condensed]';
  }
}

/**
 * UserPromptSubmit hook: inject STANDING_ORDERS on every turn so rules
 * survive compaction. First turn of each session gets the full document;
 * subsequent turns get only the critical rules to conserve tokens.
 */
function createSessionRulesHook(): HookCallback {
  return async (input, _toolUseId, _context) => {
    const evt = input as UserPromptSubmitHookInput;
    loadStandingOrders();

    if (!_fullStandingOrders) return {};

    const sessionId = evt.session_id;
    const isFirstTurn = !_seenSessions.has(sessionId);
    if (isFirstTurn) _seenSessions.add(sessionId);

    const parts: string[] = [];
    if (isFirstTurn) {
      parts.push('[FLEET-RULES — full load]\n\n' + _fullStandingOrders);

      // Include agent-config.yaml once on first turn for routing context
      const agentConfigPath = `${WORKSPACE_GROUP}/agent-config.yaml`;
      if (fs.existsSync(agentConfigPath)) {
        const config = fs.readFileSync(agentConfigPath, 'utf-8');
        parts.push('# Agent Config (routing reference)\n```yaml\n' + config + '\n```');
      }
    } else {
      parts.push('[FLEET-RULES — condensed]\n\n' + _condensedStandingOrders);
    }

    log(
      `Session-rules: ${isFirstTurn ? 'full' : 'condensed'} injection (session ${sessionId.slice(0, 8)})`,
    );
    return {
      hookSpecificOutput: {
        hookEventName: 'UserPromptSubmit' as const,
        additionalContext: parts.join('\n\n'),
      },
    };
  };
}

/**
 * SessionStart hook: inject manifest context and cross-agent learnings
 * on the first turn of a new session.
 */
function createManifestContextHook(agentName?: string): HookCallback {
  return async (input, _toolUseId, _context) => {
    const evt = input as SessionStartHookInput;
    const parts: string[] = [];

    // Load clan manifest if it exists
    const manifestPath = path.join(
      process.env.HOME || '~',
      '.clan',
      'manifest.yaml',
    );
    if (fs.existsSync(manifestPath)) {
      const content = fs.readFileSync(manifestPath, 'utf-8');
      parts.push('# Clan Manifest\n```yaml\n' + content + '\n```');
    }

    // Load cross-agent learnings (patterns.md — human-readable summary)
    const patternsPath = path.join(
      process.env.HOME || '~',
      '.clan',
      'learnings',
      'patterns.md',
    );
    if (fs.existsSync(patternsPath)) {
      const content = fs.readFileSync(patternsPath, 'utf-8');
      // Only include last 2000 chars to stay within token budget
      const trimmed =
        content.length > 2000
          ? '...(truncated)\n' + content.slice(-2000)
          : content;
      parts.push('# Cross-Agent Learnings\n' + trimmed);
    }

    // Discovery gossip — last 20 entries from shared discoveries.jsonl
    const recentDiscoveries = readRecentDiscoveries(20);
    if (recentDiscoveries.length > 0) {
      parts.push(
        '# Recent Discoveries (last 20 — non-obvious findings from the fleet)\n' +
          recentDiscoveries.join('\n'),
      );
    }

    // Resume-from-inbox — poll Convex for pending items targeting this agent.
    // Inbox is the source of truth; Discord is notification. On session start
    // we check inbox FIRST before reading chat scrollback so the agent resumes
    // pending work even if Discord notifications were missed.
    try {
      const agentConfigPath = `${WORKSPACE_GROUP}/agent-config.yaml`;
      const config = loadConvexConfig(agentConfigPath, agentName || 'unknown');
      if (config && config.agent !== 'unknown') {
        const items = await pollInbox(config);
        const prioritized = prioritizeItems(items);
        if (prioritized.length > 0) {
          const summary = prioritized.slice(0, 10).map((item, idx) => {
            const labels = (item.labels || []).join(',') || 'routine';
            return `${idx + 1}. [${labels}] ${item._id} — ${item.title}`;
          });
          parts.push(
            '# Pending Inbox Items (resume-from-inbox on session start)\n' +
              `You have ${prioritized.length} pending inbox item(s). Read them first before any Discord chatter.\n\n` +
              summary.join('\n') +
              '\n\nProtocol: ACK → work → complete. sev-1/sev-2 first, then deploy/handoff, then routine.',
          );
          log(`Resume-from-inbox: ${prioritized.length} pending item(s) for ${config.agent}`);
        }
      }
    } catch (err) {
      log(`Resume-from-inbox poll failed (non-fatal): ${err instanceof Error ? err.message : String(err)}`);
    }

    if (parts.length === 0) return {};

    log(
      `Manifest-context hook: injected ${parts.length} context block(s) (source: ${evt.source})`,
    );
    return {
      hookSpecificOutput: {
        hookEventName: 'SessionStart' as const,
        additionalContext: parts.join('\n\n'),
      },
    };
  };
}

/**
 * Stop hook: scan the last assistant message for untagged @mentions.
 * Logs violations to self-improving/violations.jsonl.
 * Messages ending with _thinkingoutloud are exempt.
 */
const INBOX_ENFORCER_ACK_PATTERNS = [
  'heartbeat_ok', 'no_reply', 'merged', 'done',
  'acknowledged', "ack'd", 'ack', 'got it', 'on it',
];

function createInboxEnforcerHook(platform?: string): HookCallback {
  return async (input, _toolUseId, _context) => {
    const evt = input as StopHookInput;
    const message = evt.last_assistant_message;
    if (!message) return {};

    // Platform filter: inbox enforcement only applies to gateway platforms
    // (Discord, Slack, etc.), not CLI/local runs. Skip if platform is CLI-like.
    const plat = (platform || process.env.NANOCLAW_PLATFORM || '').toLowerCase();
    if (plat === 'cli' || plat === 'local') return {};

    const trimmed = message.trim();
    const lower = trimmed.toLowerCase();

    // Exempt: _thinkingoutloud suffix
    if (trimmed.endsWith('_thinkingoutloud')) return {};

    // Exempt: contains HEARTBEAT_OK or NO_REPLY sentinels (any case variant)
    if (lower.includes('heartbeat_ok') || lower.includes('no_reply')) return {};

    // Exempt: pure ack under 30 chars containing a known ack phrase
    if (trimmed.length < 30 && INBOX_ENFORCER_ACK_PATTERNS.some((p) => lower.includes(p))) {
      return {};
    }

    // Skip very short messages outright (below old 50-char threshold kept
    // for non-ack messages that are just too brief to meaningfully enforce)
    if (trimmed.length < 30) return {};

    // Check for @mentions without [inbox:ID] tags
    const mentionPattern = /@\w+|<@!?\d+>/g;
    const inboxPattern = /\[inbox:[^\]]+\]/i;
    const mentions = message.match(mentionPattern);

    if (mentions && mentions.length > 0 && !inboxPattern.test(message)) {
      const violation = {
        ts: new Date().toISOString(),
        sessionId: evt.session_id,
        mentions: mentions,
        messagePreview: message.slice(0, 200),
        type: 'orphan-mention',
        platform: plat || 'unknown',
      };

      const violationsPath = `${WORKSPACE_GROUP}/self-improving/violations.jsonl`;
      const dir = path.dirname(violationsPath);
      fs.mkdirSync(dir, { recursive: true });
      fs.appendFileSync(
        violationsPath,
        JSON.stringify(violation) + '\n',
      );

      log(
        `Inbox-enforcer: logged violation — ${mentions.length} mention(s) without [inbox:ID]`,
      );
    }

    // Content validation: if the message contains inbox-creation intent
    // (natural language like "create inbox item", "filing inbox", or a
    // structured announcement pattern), validate required fields are
    // present. Required: subject, targetAgent (agent name or "mortal"),
    // and either issueRef or explicit "no-issue" justification.
    const creationIntent =
      /\b(create|creating|filed?|filing|new)\s+inbox\s+(item|task|request)/i.test(
        message,
      ) ||
      /\binbox:\s*(new|create)/i.test(message) ||
      /\b(targetAgent|target_agent|target):\s*(geordi|data|freeman|motoko|mortal)/i.test(
        message,
      );

    if (creationIntent) {
      const missing: string[] = [];
      // Check for subject/title line
      if (
        !/\b(subject|title|task):\s*\S/i.test(message) &&
        !/\binbox-item:\s*\S/i.test(message)
      ) {
        missing.push('subject');
      }
      // Check for targetAgent
      if (
        !/\b(targetAgent|target_agent|target|to)\s*[:=]\s*(geordi|data|freeman|motoko|mortal)/i.test(
          message,
        )
      ) {
        missing.push('targetAgent');
      }
      // Check for issueRef (or explicit no-issue/bug-fix/config disclaimer)
      const hasIssueRef = /\b\S+\/\S+#\d+\b/.test(message);
      const noIssueJustified =
        /\b(no[-\s]?issue|bug[-\s]?fix|config|docs|no\s+issue\s+needed)\b/i.test(
          message,
        );
      if (!hasIssueRef && !noIssueJustified) {
        missing.push('issueRef (or explicit no-issue justification)');
      }

      if (missing.length > 0) {
        const violation = {
          ts: new Date().toISOString(),
          sessionId: evt.session_id,
          messagePreview: message.slice(0, 300),
          type: 'incomplete-inbox-content',
          missingFields: missing,
          platform: plat || 'unknown',
        };
        const violationsPath = `${WORKSPACE_GROUP}/self-improving/violations.jsonl`;
        const dir = path.dirname(violationsPath);
        fs.mkdirSync(dir, { recursive: true });
        fs.appendFileSync(violationsPath, JSON.stringify(violation) + '\n');
        log(
          `Inbox-enforcer: content violation — inbox creation intent missing ${missing.join(', ')}`,
        );
      }
    }

    return {};
  };
}

/**
 * Stop hook: sync corrections from self-improving/corrections.md
 * to the shared clan learnings store (~/.clan/learnings/patterns.jsonl).
 * Tracks last sync position to avoid re-appending.
 */
function createLearningSyncHook(agentName?: string): HookCallback {
  return async (_input, _toolUseId, _context) => {
    const correctionsPath = `${WORKSPACE_GROUP}/self-improving/corrections.md`;
    const syncStatePath = `${WORKSPACE_GROUP}/self-improving/.learning-sync-state.json`;
    const patternsPath = path.join(
      process.env.HOME || '~',
      '.clan',
      'learnings',
      'patterns.jsonl',
    );

    if (!fs.existsSync(correctionsPath)) return {};

    // Check if corrections.md has changed since last sync
    const stat = fs.statSync(correctionsPath);
    let lastMtime = 0;
    let syncedKeys = new Set<string>();
    if (fs.existsSync(syncStatePath)) {
      try {
        const state = JSON.parse(fs.readFileSync(syncStatePath, 'utf-8'));
        lastMtime = state.lastMtime || 0;
        syncedKeys = new Set(state.syncedKeys || []);
      } catch { /* ignore */ }
    }

    if (stat.mtimeMs <= lastMtime) return {};

    // Parse ALL new correction entries since last sync.
    // A correction is a block starting with `## YYYY-MM-DD HH:MM — <title>`
    // followed by **What I got wrong:** or **Content:** fields.
    const content = fs.readFileSync(correctionsPath, 'utf-8');
    const entryRegex = /^## (\d{4}-\d{2}-\d{2} \d{2}:\d{2}) — (.+)$([\s\S]*?)(?=^## \d{4}-|\Z)/gm;

    const patternsDir = path.dirname(patternsPath);
    fs.mkdirSync(patternsDir, { recursive: true });

    let appended = 0;
    let match: RegExpExecArray | null;
    while ((match = entryRegex.exec(content)) !== null) {
      const [, ts, title, body] = match;
      const key = `${ts}::${title.trim().slice(0, 80)}`;
      if (syncedKeys.has(key)) continue;

      // Extract body fields
      const gotWrong = body.match(/\*\*What I got wrong:\*\*\s*(.+)/)?.[1]?.trim();
      const correctApproach = body.match(/\*\*Correct approach:\*\*\s*(.+)/)?.[1]?.trim();
      const source = body.match(/\*\*Source:\*\*\s*(.+)/)?.[1]?.trim()
        || body.match(/\*\*Signal from:\*\*\s*(.+)/)?.[1]?.trim()
        || 'unknown';
      const patternDesc = body.match(/\*\*Pattern:\*\*\s*(.+)/)?.[1]?.trim();
      const signalContent = body.match(/\*\*Content:\*\*\s*(.+)/)?.[1]?.trim();
      const tier = body.match(/\*\*Tier:\*\*\s*(\w+)/)?.[1]?.trim();

      const problem = gotWrong || signalContent || title.trim();
      const solution = correctApproach || patternDesc || 'See corrections.md for details';

      const pattern = {
        id: `p-${agentName || 'unknown'}-${Date.now()}-${appended}`,
        uuid: crypto.randomUUID(),
        agent: agentName || 'unknown',
        harness: 'nanoclaw',
        clan: 'lambda',
        ts: new Date().toISOString(),
        source_ts: ts,
        category: 'correction',
        title: title.trim().slice(0, 120),
        problem: problem.slice(0, 500),
        solution: solution.slice(0, 500),
        tags: tier ? ['auto-detected', `tier:${tier}`, `source:${source}`] : ['manual', `source:${source}`],
        status: 'active',
        supersedes: null,
      };
      fs.appendFileSync(patternsPath, JSON.stringify(pattern) + '\n');
      syncedKeys.add(key);
      appended++;
    }

    if (appended > 0) {
      log(
        `Learning-sync: appended ${appended} pattern(s) to patterns.jsonl`,
      );
    }

    // Update sync state
    fs.writeFileSync(
      syncStatePath,
      JSON.stringify({
        lastMtime: stat.mtimeMs,
        syncedKeys: Array.from(syncedKeys),
      }) + '\n',
    );

    return {};
  };
}

/**
 * Stop hook: check if the agent missed a known pattern from the shared
 * learnings store. Scans the last assistant message for keywords that
 * match patterns in patterns.jsonl but weren't applied.
 */
function createLearningVerifierHook(): HookCallback {
  return async (input, _toolUseId, _context) => {
    const evt = input as StopHookInput;
    const message = evt.last_assistant_message;
    if (!message) return {};

    const patternsPath = path.join(
      process.env.HOME || '~',
      '.clan',
      'learnings',
      'patterns.jsonl',
    );
    if (!fs.existsSync(patternsPath)) return {};

    let patterns: Array<{
      id: string;
      title: string;
      problem: string;
      solution: string;
      tags?: string[];
    }>;
    try {
      patterns = fs
        .readFileSync(patternsPath, 'utf-8')
        .split('\n')
        .filter((line) => line.trim())
        .map((line) => JSON.parse(line))
        .filter((p) => p.status === 'active');
    } catch {
      return {};
    }

    if (patterns.length === 0) return {};

    // Extract keywords from message (words 4+ chars, lowered)
    const messageWords = new Set(
      message
        .toLowerCase()
        .split(/\W+/)
        .filter((w) => w.length >= 4),
    );

    // Check each pattern for keyword overlap (2+ matching keywords)
    const missedPatterns: string[] = [];
    for (const pattern of patterns) {
      const patternWords = [
        ...(pattern.problem || '').toLowerCase().split(/\W+/),
        ...(pattern.tags || []).map((t) => t.toLowerCase()),
      ].filter((w) => w.length >= 4);

      const overlap = patternWords.filter((w) => messageWords.has(w));
      if (overlap.length >= 2) {
        missedPatterns.push(pattern.id);
      }
    }

    if (missedPatterns.length > 0) {
      const missedPath = `${WORKSPACE_GROUP}/self-improving/missed-learnings.jsonl`;
      const dir = path.dirname(missedPath);
      fs.mkdirSync(dir, { recursive: true });

      const entry = {
        ts: new Date().toISOString(),
        sessionId: evt.session_id,
        matchedPatterns: missedPatterns,
        messagePreview: message.slice(0, 200),
      };
      fs.appendFileSync(missedPath, JSON.stringify(entry) + '\n');

      log(
        `Learning-verifier: ${missedPatterns.length} potentially missed pattern(s)`,
      );
    }

    return {};
  };
}

/**
 * Stop hook: emit lifecycle events to Convex (message:sent, session events).
 * Fire-and-forget — does not block the agent.
 */
function createConvexEventHook(agentName?: string): HookCallback {
  return async (input, _toolUseId, _context) => {
    const evt = input as StopHookInput;

    const agentConfigPath = `${WORKSPACE_GROUP}/agent-config.yaml`;
    const config = loadConvexConfig(agentConfigPath, agentName || 'unknown');
    if (!config) return {};

    await postEvent(config.convexUrl, {
      type: 'message:sent',
      agent: config.agent,
      project: config.project,
      clan: config.clan,
      data: {
        sessionId: evt.session_id,
        hasMessage: !!evt.last_assistant_message,
        messageLength: evt.last_assistant_message?.length || 0,
      },
    });

    return {};
  };
}

/**
 * Emit a Fleet Lambda lifecycle event (session:start / session:end) to
 * Convex. Mirrors Hermes' wololo-events handler.py: fire-and-forget, 3s
 * timeout, never throws. Loads convex config lazily from the group's
 * agent-config.yaml so the same helper works for both native and
 * container runs.
 *
 * @param type       Event kind — session:start at runner boot, session:end at shutdown.
 * @param agentName  Agent display id (e.g. 'geordi'). Derived from identity.md if omitted.
 * @param sessionId  Current query sessionId, or undefined at boot before the SDK picks one.
 * @param extra      Optional metadata (e.g. exit reason) merged into the event.data payload.
 */
async function fireLifecycleEvent(
  type: 'session:start' | 'session:end',
  agentName: string | undefined,
  sessionId: string | undefined,
  extra: Record<string, unknown> = {},
): Promise<void> {
  try {
    const agentConfigPath = `${WORKSPACE_GROUP}/agent-config.yaml`;
    const config = loadConvexConfig(agentConfigPath, agentName || 'unknown');
    if (!config) return;

    await postEvent(config.convexUrl, {
      type,
      agent: config.agent,
      project: config.project,
      clan: config.clan,
      data: {
        sessionId: sessionId || '',
        source: 'nanoclaw',
        runtime: process.env.NANOCLAW_RUNTIME || 'native',
        ...extra,
      },
    });
  } catch (err) {
    // Never block the pipeline on lifecycle telemetry.
    log(
      `Lifecycle event ${type} failed (non-fatal): ${err instanceof Error ? err.message : String(err)}`,
    );
  }
}

/**
 * Detect thread-close intent from the last assistant message.
 * Returns the detected outcome, or null if not a close.
 */
const THREAD_CLOSE_PATTERNS: Array<{
  re: RegExp;
  outcome: ACPMetrics['outcome'];
}> = [
  { re: /\bSTATE:\s*(MERGED|CLOSED)\b/i, outcome: 'resolved' },
  { re: /\bACTION:\s*(close|done|completed?)\b/i, outcome: 'resolved' },
  { re: /\b(thread[-\s]?close(d)?|resolved|resolving)\b/i, outcome: 'resolved' },
  { re: /\bACTION:\s*handoff\b/i, outcome: 'handed-off' },
  { re: /\bTARGET:\s*\w+\b.*\bACTION:\s*(deploy|review)\b/i, outcome: 'handed-off' },
  { re: /\b(abandon(ed|ing)?|giving up|stalled)\b/i, outcome: 'abandoned' },
];

function detectThreadClose(message: string): ACPMetrics['outcome'] | null {
  for (const { re, outcome } of THREAD_CLOSE_PATTERNS) {
    if (re.test(message)) return outcome;
  }
  return null;
}

/**
 * Count ACP/1 structured headers and naturalism in the message.
 * Heuristic protocol detector.
 */
function detectProtocol(message: string): ACPMetrics['protocol'] {
  const hasAcpHeaders =
    /\b(STATE|PR|ACTION|TARGET|BLOCKER|GATE):\S+/i.test(message) ||
    /\[(PR|STATE|ACTION|TARGET|BLOCKER|GATE|ACK)[:\s]/i.test(message);
  const wordsOutsideHeaders = message
    .replace(/\[[^\]]+\]/g, '')
    .replace(/\b[A-Z]+:\S+/g, '')
    .trim().split(/\s+/).filter(Boolean).length;
  if (hasAcpHeaders && wordsOutsideHeaders < 50) return 'acp/1';
  if (hasAcpHeaders) return 'mixed';
  return 'natural';
}

/**
 * Stop hook: on thread-close detection, emit ACP metrics to
 * ~/.clan/learnings/acp-metrics.jsonl + POST /api/metrics/acp.
 *
 * Best-effort metrics — messageCount/timeToResolution are based on the
 * transcript, not Discord history. Loops/humans counted from the
 * last_assistant_message alone (post-hoc thread reconstruction is out
 * of scope for this hook).
 */
function createACPMetricsHook(agentName?: string): HookCallback {
  return async (input, _toolUseId, _context) => {
    const evt = input as StopHookInput;
    const message = evt.last_assistant_message;
    if (!message) return {};

    const outcome = detectThreadClose(message);
    if (!outcome) return {};

    const protocol = detectProtocol(message);

    // Extract @mentioned agents from message
    const agentPattern = /@(geordi|data|freeman|motoko)\b/gi;
    const mentioned = new Set<string>();
    if (agentName) mentioned.add(agentName);
    for (const m of message.matchAll(agentPattern)) mentioned.add(m[1].toLowerCase());

    // Extract threadId — prefer [thread:ID], fall back to sessionId
    const threadMatch = message.match(/\[thread:([^\]]+)\]/i);
    const threadId = threadMatch?.[1] || evt.session_id;

    // Crude estimates — transcript-derived values would be better but
    // require parsing the session jsonl which is costly per hook fire.
    const messageCount = 0; // caller can enrich later
    const timeToResolutionMin = 0; // unknown from single message
    const loopsDetected = 0;
    const humanInterventions = /@stevie\b/i.test(message) ? 1 : 0;

    const agentConfigPath = `${WORKSPACE_GROUP}/agent-config.yaml`;
    const config = loadConvexConfig(agentConfigPath, agentName || 'unknown');

    const metric: ACPMetrics = {
      threadId,
      protocol,
      messageCount,
      agentsInvolved: Array.from(mentioned),
      timeToResolutionMin,
      loopsDetected,
      humanInterventions,
      outcome,
      project: config?.project,
      clan: config?.clan,
      harness: 'nanoclaw',
      ts: new Date().toISOString(),
    };

    // Persist locally to shared clan store
    try {
      const metricsPath = path.join(
        process.env.HOME || '~',
        '.clan',
        'learnings',
        'acp-metrics.jsonl',
      );
      fs.mkdirSync(path.dirname(metricsPath), { recursive: true });
      fs.appendFileSync(metricsPath, JSON.stringify(metric) + '\n');
    } catch (err) {
      log(`ACP-metrics local persist failed (non-fatal): ${err instanceof Error ? err.message : String(err)}`);
    }

    // Post to Convex (fire-and-forget)
    if (config) {
      await postMetric(config.convexUrl, metric);
    }

    log(`ACP-metrics: thread-close detected (outcome=${outcome}, protocol=${protocol}, agents=${metric.agentsInvolved.length})`);
    return {};
  };
}

// ---------------------------------------------------------------------------
// Skill-level hook discovery
// ---------------------------------------------------------------------------

interface SkillHookDeclaration {
  skillName: string;
  events: string[];
  scriptPath?: string;
}

/**
 * Scan container skills for hook declarations in frontmatter.
 * Skills can declare hooks via:
 *   ---
 *   hooks:
 *     Stop: true
 *     UserPromptSubmit: true
 *   ---
 * or:
 *   ---
 *   hooks:
 *     Stop: ./scripts/my-hook.sh
 *   ---
 *
 * Returns declarations that the agent-runner can wire into the SDK query.
 * Currently used for documentation/discovery — script-based hooks execute
 * the declared script; boolean hooks log the event for the skill to consume.
 */
function discoverSkillHooks(): SkillHookDeclaration[] {
  const skillsDir = path.join(CLAUDE_HOME, 'skills');
  if (!fs.existsSync(skillsDir)) return [];

  const declarations: SkillHookDeclaration[] = [];

  for (const entry of fs.readdirSync(skillsDir)) {
    const skillDir = path.join(skillsDir, entry);
    if (!fs.statSync(skillDir).isDirectory()) continue;

    const skillMdPath = path.join(skillDir, 'SKILL.md');
    if (!fs.existsSync(skillMdPath)) continue;

    const content = fs.readFileSync(skillMdPath, 'utf-8');
    // Parse YAML frontmatter between --- markers
    const fmMatch = content.match(/^---\n([\s\S]*?)\n---/);
    if (!fmMatch) continue;

    const frontmatter = fmMatch[1];
    // Check for hooks: block
    const hooksMatch = frontmatter.match(/^hooks:\s*\n((?:\s+\w+:.*\n?)*)/m);
    if (!hooksMatch) continue;

    const hookLines = hooksMatch[1].split('\n').filter((l) => l.trim());
    const events: string[] = [];
    for (const line of hookLines) {
      const lineMatch = line.match(/^\s+(\w+):\s*(.+)$/);
      if (lineMatch) {
        events.push(lineMatch[1]);
      }
    }

    if (events.length > 0) {
      declarations.push({ skillName: entry, events });
      log(`Skill "${entry}" declares hooks: ${events.join(', ')}`);
    }
  }

  return declarations;
}

interface SignalsConfig {
  correctionSignals: {
    strong: string[];
    medium: string[];
    weak: string[];
  };
  excludePatterns: string[];
  minimumConfidence: 'strong' | 'medium' | 'weak';
  agentNames: string[];
}

let _signalsCache: SignalsConfig | null = null;

/**
 * Load signals.json from the agent-runner source directory. Cached after
 * first read. Falls back to empty config if file missing.
 */
function loadSignals(): SignalsConfig {
  if (_signalsCache) return _signalsCache;

  const __dirname = path.dirname(fileURLToPath(import.meta.url));
  const signalsPath = path.join(__dirname, 'signals.json');

  const empty: SignalsConfig = {
    correctionSignals: { strong: [], medium: [], weak: [] },
    excludePatterns: [],
    minimumConfidence: 'medium',
    agentNames: [],
  };

  try {
    if (!fs.existsSync(signalsPath)) {
      log(`signals.json not found at ${signalsPath} — self-reflection disabled`);
      _signalsCache = empty;
      return empty;
    }
    _signalsCache = JSON.parse(fs.readFileSync(signalsPath, 'utf-8')) as SignalsConfig;
    log(`Loaded signals.json (strong: ${_signalsCache.correctionSignals.strong.length}, medium: ${_signalsCache.correctionSignals.medium.length}, weak: ${_signalsCache.correctionSignals.weak.length})`);
    return _signalsCache;
  } catch (err) {
    log(`Failed to load signals.json: ${err instanceof Error ? err.message : String(err)}`);
    _signalsCache = empty;
    return empty;
  }
}

/**
 * Detect the highest signal tier that matches the text, respecting
 * minimumConfidence. Returns the tier name or null if no match.
 */
function detectSignalTier(
  lowerText: string,
  signals: SignalsConfig,
): 'strong' | 'medium' | 'weak' | null {
  const order: Array<'strong' | 'medium' | 'weak'> = ['strong', 'medium', 'weak'];
  const minIdx = order.indexOf(signals.minimumConfidence);
  // Consider tiers from strong down to minimumConfidence
  for (let i = 0; i <= minIdx; i++) {
    const tier = order[i];
    const patterns = signals.correctionSignals[tier];
    if (patterns.some((p) => lowerText.includes(p.toLowerCase()))) {
      return tier;
    }
  }
  return null;
}

/**
 * Self-reflection hook: scan recent transcript for correction signals.
 * Runs after each query completes. Detects corrections from humans or build
 * failures and logs them to self-improving/corrections.md. Promotes repeated
 * patterns to self-improving/memory.md (HOT tier).
 */
async function runSelfReflection(sessionId: string | undefined): Promise<void> {
  const correctionsPath = `${WORKSPACE_GROUP}/self-improving/corrections.md`;
  const memoryPath = `${WORKSPACE_GROUP}/self-improving/memory.md`;
  const dir = `${WORKSPACE_GROUP}/self-improving`;

  // Find the most recent transcript to scan
  const claudeDir = CLAUDE_HOME;
  const projectsDir = path.join(claudeDir, 'projects');
  if (!sessionId || !fs.existsSync(projectsDir)) return;

  // Find transcript file for current session
  let transcriptPath: string | undefined;
  try {
    const walkForSession = (base: string): string | undefined => {
      for (const entry of fs.readdirSync(base)) {
        const full = path.join(base, entry);
        if (fs.statSync(full).isDirectory()) {
          const found = walkForSession(full);
          if (found) return found;
        } else if (entry === `${sessionId}.jsonl`) {
          return full;
        }
      }
      return undefined;
    };
    transcriptPath = walkForSession(projectsDir);
  } catch {
    return;
  }

  if (!transcriptPath || !fs.existsSync(transcriptPath)) return;

  // Read last N lines of transcript (recent messages only)
  let content: string;
  try {
    content = fs.readFileSync(transcriptPath, 'utf-8');
  } catch {
    return;
  }

  const lines = content.split('\n').filter((l) => l.trim());
  // Only scan the last 20 messages for corrections
  const recentLines = lines.slice(-20);

  // Load 3-tier signal dictionary from signals.json (falls back to empty on error)
  const signals = loadSignals();
  const detectedCorrections: Array<{ text: string; source: string; tier: string }> = [];

  for (const line of recentLines) {
    try {
      const entry = JSON.parse(line);
      if (entry.type !== 'user' || !entry.message?.content) continue;

      const text =
        typeof entry.message.content === 'string'
          ? entry.message.content
          : entry.message.content
              .map((c: { text?: string }) => c.text || '')
              .join('');

      const lowerText = text.toLowerCase();

      // Exclude-pattern check first — skip known false-positives
      if (signals.excludePatterns.some((p) => lowerText.includes(p))) continue;

      // Check tiers in priority order: strong > medium > weak
      // Respect minimumConfidence threshold
      const tier = detectSignalTier(lowerText, signals);
      if (tier) {
        detectedCorrections.push({ text: text.slice(0, 500), source: 'human', tier });
      }
    } catch {
      continue;
    }
  }

  if (detectedCorrections.length === 0) return;

  log(`Self-reflection: detected ${detectedCorrections.length} correction signal(s)`);

  // Write corrections
  fs.mkdirSync(dir, { recursive: true });

  const now = new Date();
  const dateStr = now.toISOString().replace('T', ' ').slice(0, 16);
  let correctionsContent = '';

  if (fs.existsSync(correctionsPath)) {
    correctionsContent = fs.readFileSync(correctionsPath, 'utf-8');
  } else {
    correctionsContent = '# Corrections Log\n\nAutomatically detected correction signals.\n\n';
  }

  for (const correction of detectedCorrections) {
    correctionsContent += `\n## ${dateStr} — Detected correction (${correction.tier})\n`;
    correctionsContent += `**Signal from:** ${correction.source}\n`;
    correctionsContent += `**Tier:** ${correction.tier}\n`;
    correctionsContent += `**Content:** ${correction.text}\n`;
    correctionsContent += `**Status:** pending-review\n\n`;
  }

  fs.writeFileSync(correctionsPath, correctionsContent);
  log(`Self-reflection: logged ${detectedCorrections.length} correction(s)`);

  // Pattern promotion: count similar corrections. If 3+ pending, promote to memory.
  const pendingCount = (correctionsContent.match(/\*\*Status:\*\* pending-review/g) || []).length;
  if (pendingCount >= 3) {
    let memoryContent = '';
    if (fs.existsSync(memoryPath)) {
      memoryContent = fs.readFileSync(memoryPath, 'utf-8');
    } else {
      memoryContent = '# Memory (HOT Tier)\n\nPromoted patterns from repeated corrections.\n\n';
    }

    memoryContent += `\n## ${dateStr} — Promoted pattern (${pendingCount} corrections)\n`;
    memoryContent += `**Note:** Review self-improving/corrections.md for details. ${pendingCount} pending corrections detected — likely a recurring pattern.\n\n`;
    fs.writeFileSync(memoryPath, memoryContent);

    // Mark promoted corrections as reviewed
    correctionsContent = correctionsContent.replace(
      /\*\*Status:\*\* pending-review/g,
      '**Status:** promoted',
    );
    fs.writeFileSync(correctionsPath, correctionsContent);

    log(`Self-reflection: promoted pattern to memory (${pendingCount} corrections)`);
  }
}

function sanitizeFilename(summary: string): string {
  return summary
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 50);
}

function generateFallbackName(): string {
  const time = new Date();
  return `conversation-${time.getHours().toString().padStart(2, '0')}${time.getMinutes().toString().padStart(2, '0')}`;
}

interface ParsedMessage {
  role: 'user' | 'assistant';
  content: string;
}

function parseTranscript(content: string): ParsedMessage[] {
  const messages: ParsedMessage[] = [];

  for (const line of content.split('\n')) {
    if (!line.trim()) continue;
    try {
      const entry = JSON.parse(line);
      if (entry.type === 'user' && entry.message?.content) {
        const text =
          typeof entry.message.content === 'string'
            ? entry.message.content
            : entry.message.content
                .map((c: { text?: string }) => c.text || '')
                .join('');
        if (text) messages.push({ role: 'user', content: text });
      } else if (entry.type === 'assistant' && entry.message?.content) {
        const textParts = entry.message.content
          .filter((c: { type: string }) => c.type === 'text')
          .map((c: { text: string }) => c.text);
        const text = textParts.join('');
        if (text) messages.push({ role: 'assistant', content: text });
      }
    } catch {}
  }

  return messages;
}

function formatTranscriptMarkdown(
  messages: ParsedMessage[],
  title?: string | null,
  assistantName?: string,
): string {
  const now = new Date();
  const formatDateTime = (d: Date) =>
    d.toLocaleString('en-US', {
      month: 'short',
      day: 'numeric',
      hour: 'numeric',
      minute: '2-digit',
      hour12: true,
    });

  const lines: string[] = [];
  lines.push(`# ${title || 'Conversation'}`);
  lines.push('');
  lines.push(`Archived: ${formatDateTime(now)}`);
  lines.push('');
  lines.push('---');
  lines.push('');

  for (const msg of messages) {
    const sender = msg.role === 'user' ? 'User' : assistantName || 'Assistant';
    const content =
      msg.content.length > 2000
        ? msg.content.slice(0, 2000) + '...'
        : msg.content;
    lines.push(`**${sender}**: ${content}`);
    lines.push('');
  }

  return lines.join('\n');
}

/**
 * Check for _close sentinel.
 */
function shouldClose(): boolean {
  if (fs.existsSync(IPC_INPUT_CLOSE_SENTINEL)) {
    try {
      fs.unlinkSync(IPC_INPUT_CLOSE_SENTINEL);
    } catch {
      /* ignore */
    }
    return true;
  }
  return false;
}

/**
 * Drain all pending IPC input messages.
 * Returns messages found, or empty array.
 */
function drainIpcInput(): string[] {
  try {
    fs.mkdirSync(IPC_INPUT_DIR, { recursive: true });
    const files = fs
      .readdirSync(IPC_INPUT_DIR)
      .filter((f) => f.endsWith('.json'))
      .sort();

    const messages: string[] = [];
    for (const file of files) {
      const filePath = path.join(IPC_INPUT_DIR, file);
      try {
        const data = JSON.parse(fs.readFileSync(filePath, 'utf-8'));
        fs.unlinkSync(filePath);
        if (data.type === 'message' && data.text) {
          messages.push(data.text);
        }
      } catch (err) {
        log(
          `Failed to process input file ${file}: ${err instanceof Error ? err.message : String(err)}`,
        );
        try {
          fs.unlinkSync(filePath);
        } catch {
          /* ignore */
        }
      }
    }
    return messages;
  } catch (err) {
    log(`IPC drain error: ${err instanceof Error ? err.message : String(err)}`);
    return [];
  }
}

/**
 * Wait for a new IPC message or _close sentinel.
 * Returns the messages as a single string, or null if _close.
 */
function waitForIpcMessage(): Promise<string | null> {
  return new Promise((resolve) => {
    const poll = () => {
      if (shouldClose()) {
        resolve(null);
        return;
      }
      const messages = drainIpcInput();
      if (messages.length > 0) {
        resolve(messages.join('\n'));
        return;
      }
      setTimeout(poll, IPC_POLL_MS);
    };
    poll();
  });
}

/**
 * Run a single query and stream results via writeOutput.
 * Uses MessageStream (AsyncIterable) to keep isSingleUserTurn=false,
 * allowing agent teams subagents to run to completion.
 * Also pipes IPC messages into the stream during the query.
 */
async function runQuery(
  prompt: string,
  sessionId: string | undefined,
  mcpServerPath: string,
  containerInput: ContainerInput,
  sdkEnv: Record<string, string | undefined>,
  resumeAt?: string,
): Promise<{
  newSessionId?: string;
  lastAssistantUuid?: string;
  closedDuringQuery: boolean;
}> {
  const stream = new MessageStream();
  stream.push(prompt);

  // Poll IPC for follow-up messages and _close sentinel during the query
  let ipcPolling = true;
  let closedDuringQuery = false;
  const pollIpcDuringQuery = () => {
    if (!ipcPolling) return;
    if (shouldClose()) {
      log('Close sentinel detected during query, ending stream');
      closedDuringQuery = true;
      stream.end();
      ipcPolling = false;
      return;
    }
    const messages = drainIpcInput();
    for (const text of messages) {
      log(`Piping IPC message into active query (${text.length} chars)`);
      stream.push(text);
    }
    setTimeout(pollIpcDuringQuery, IPC_POLL_MS);
  };
  setTimeout(pollIpcDuringQuery, IPC_POLL_MS);

  let newSessionId: string | undefined;
  let lastAssistantUuid: string | undefined;
  let messageCount = 0;
  let resultCount = 0;

  // Load global CLAUDE.md as additional system context (shared across all groups)
  const globalClaudeMdPath = `${WORKSPACE_GLOBAL}/CLAUDE.md`;
  let globalClaudeMd: string | undefined;
  if (!containerInput.isMain && fs.existsSync(globalClaudeMdPath)) {
    globalClaudeMd = fs.readFileSync(globalClaudeMdPath, 'utf-8');
  }

  // Load per-group bounded memory files (MEMORY.md + USER.md)
  const memoryPath = `${WORKSPACE_GROUP}/memories/MEMORY.md`;
  const userProfilePath = `${WORKSPACE_GROUP}/memories/USER.md`;
  let memoryContext = '';
  if (fs.existsSync(memoryPath)) {
    memoryContext += '\n\n# Agent Memory\n' + fs.readFileSync(memoryPath, 'utf-8');
    log('Loaded memories/MEMORY.md');
  }
  if (fs.existsSync(userProfilePath)) {
    memoryContext += '\n\n# User Profile\n' + fs.readFileSync(userProfilePath, 'utf-8');
    log('Loaded memories/USER.md');
  }

  // Discover additional directories mounted at /workspace/extra/*
  // These are passed to the SDK so their CLAUDE.md files are loaded automatically
  const extraDirs: string[] = [];
  const extraBase = WORKSPACE_EXTRA;
  if (fs.existsSync(extraBase)) {
    for (const entry of fs.readdirSync(extraBase)) {
      const fullPath = path.join(extraBase, entry);
      if (fs.statSync(fullPath).isDirectory()) {
        extraDirs.push(fullPath);
      }
    }
  }
  if (extraDirs.length > 0) {
    log(`Additional directories: ${extraDirs.join(', ')}`);
  }

  for await (const message of query({
    prompt: stream,
    options: {
      cwd: WORKSPACE_GROUP,
      additionalDirectories: extraDirs.length > 0 ? extraDirs : undefined,
      resume: sessionId,
      resumeSessionAt: resumeAt,
      systemPrompt: (globalClaudeMd || memoryContext)
        ? {
            type: 'preset' as const,
            preset: 'claude_code' as const,
            append: (globalClaudeMd || '') + memoryContext,
          }
        : undefined,
      allowedTools: [
        'Bash',
        'Read',
        'Write',
        'Edit',
        'Glob',
        'Grep',
        'WebSearch',
        'WebFetch',
        'Task',
        'TaskOutput',
        'TaskStop',
        'TeamCreate',
        'TeamDelete',
        'SendMessage',
        'TodoWrite',
        'ToolSearch',
        'Skill',
        'NotebookEdit',
        'mcp__nanoclaw__*',
      ],
      env: sdkEnv,
      permissionMode: 'bypassPermissions',
      allowDangerouslySkipPermissions: true,
      settingSources: ['project', 'user'],
      mcpServers: {
        nanoclaw: {
          command: 'node',
          args: [mcpServerPath],
          env: {
            NANOCLAW_CHAT_JID: containerInput.chatJid,
            NANOCLAW_GROUP_FOLDER: containerInput.groupFolder,
            NANOCLAW_IS_MAIN: containerInput.isMain ? '1' : '0',
          },
        },
      },
      hooks: {
        PreCompact: [
          { hooks: [createPreCompactHook(containerInput.assistantName)] },
        ],
        UserPromptSubmit: [
          { hooks: [createSessionRulesHook()] },
        ],
        SessionStart: [
          { hooks: [createManifestContextHook(containerInput.assistantName)] },
        ],
        Stop: [
          {
            hooks: [
              createInboxEnforcerHook(),
              createLearningSyncHook(containerInput.assistantName),
              createLearningVerifierHook(),
              createConvexEventHook(containerInput.assistantName),
              createACPMetricsHook(containerInput.assistantName),
            ],
          },
        ],
      },
    },
  })) {
    messageCount++;
    const msgType =
      message.type === 'system'
        ? `system/${(message as { subtype?: string }).subtype}`
        : message.type;
    log(`[msg #${messageCount}] type=${msgType}`);

    if (message.type === 'assistant' && 'uuid' in message) {
      lastAssistantUuid = (message as { uuid: string }).uuid;
    }

    if (message.type === 'system' && message.subtype === 'init') {
      newSessionId = message.session_id;
      log(`Session initialized: ${newSessionId}`);
    }

    if (
      message.type === 'system' &&
      (message as { subtype?: string }).subtype === 'task_notification'
    ) {
      const tn = message as {
        task_id: string;
        status: string;
        summary: string;
      };
      log(
        `Task notification: task=${tn.task_id} status=${tn.status} summary=${tn.summary}`,
      );
    }

    if (message.type === 'result') {
      resultCount++;
      const textResult =
        'result' in message ? (message as { result?: string }).result : null;
      log(
        `Result #${resultCount}: subtype=${message.subtype}${textResult ? ` text=${textResult.slice(0, 200)}` : ''}`,
      );
      writeOutput({
        status: 'success',
        result: textResult || null,
        newSessionId,
      });
    }
  }

  ipcPolling = false;
  log(
    `Query done. Messages: ${messageCount}, results: ${resultCount}, lastAssistantUuid: ${lastAssistantUuid || 'none'}, closedDuringQuery: ${closedDuringQuery}`,
  );
  return { newSessionId, lastAssistantUuid, closedDuringQuery };
}

interface ScriptResult {
  wakeAgent: boolean;
  data?: unknown;
}

const SCRIPT_TIMEOUT_MS = 30_000;

async function runScript(script: string): Promise<ScriptResult | null> {
  const scriptPath = '/tmp/task-script.sh';
  fs.writeFileSync(scriptPath, script, { mode: 0o755 });

  return new Promise((resolve) => {
    execFile(
      'bash',
      [scriptPath],
      {
        timeout: SCRIPT_TIMEOUT_MS,
        maxBuffer: 1024 * 1024,
        env: process.env,
      },
      (error, stdout, stderr) => {
        if (stderr) {
          log(`Script stderr: ${stderr.slice(0, 500)}`);
        }

        if (error) {
          log(`Script error: ${error.message}`);
          return resolve(null);
        }

        // Parse last non-empty line of stdout as JSON
        const lines = stdout.trim().split('\n');
        const lastLine = lines[lines.length - 1];
        if (!lastLine) {
          log('Script produced no output');
          return resolve(null);
        }

        try {
          const result = JSON.parse(lastLine);
          if (typeof result.wakeAgent !== 'boolean') {
            log(
              `Script output missing wakeAgent boolean: ${lastLine.slice(0, 200)}`,
            );
            return resolve(null);
          }
          resolve(result as ScriptResult);
        } catch {
          log(`Script output is not valid JSON: ${lastLine.slice(0, 200)}`);
          resolve(null);
        }
      },
    );
  });
}

async function main(): Promise<void> {
  let containerInput: ContainerInput;

  try {
    const stdinData = await readStdin();
    containerInput = JSON.parse(stdinData);
    try {
      fs.unlinkSync('/tmp/input.json');
    } catch {
      /* may not exist */
    }
    log(`Received input for group: ${containerInput.groupFolder}`);
  } catch (err) {
    writeOutput({
      status: 'error',
      result: null,
      error: `Failed to parse input: ${err instanceof Error ? err.message : String(err)}`,
    });
    process.exit(1);
  }

  // Discover skill-level hook declarations (logged for observability)
  const skillHooks = discoverSkillHooks();
  if (skillHooks.length > 0) {
    log(`Discovered ${skillHooks.length} skill(s) with hook declarations`);
  }

  // P2: Fleet Lambda lifecycle event — emitted once at runner boot.
  // Fire-and-forget; the helper swallows errors and the 3s HTTP timeout
  // cannot stall the agent pipeline.
  await fireLifecycleEvent(
    'session:start',
    containerInput.assistantName,
    containerInput.sessionId,
    {
      isScheduledTask: !!containerInput.isScheduledTask,
      hasScript: !!containerInput.script,
    },
  );

  // Credentials are injected by the host's credential proxy via ANTHROPIC_BASE_URL.
  // No real secrets exist in the container environment.
  const sdkEnv: Record<string, string | undefined> = {
    ...process.env,
    CLAUDE_CODE_AUTO_COMPACT_WINDOW: '165000',
  };

  const __dirname = path.dirname(fileURLToPath(import.meta.url));
  const mcpServerPath = path.join(__dirname, 'ipc-mcp-stdio.js');

  let sessionId = containerInput.sessionId;
  fs.mkdirSync(IPC_INPUT_DIR, { recursive: true });

  // Clean up stale _close sentinel from previous container runs
  try {
    fs.unlinkSync(IPC_INPUT_CLOSE_SENTINEL);
  } catch {
    /* ignore */
  }

  // Build initial prompt (drain any pending IPC messages too)
  let prompt = containerInput.prompt;
  if (containerInput.isScheduledTask) {
    prompt = `[SCHEDULED TASK - The following message was sent automatically and is not coming directly from the user or group.]\n\n${prompt}`;
  }
  const pending = drainIpcInput();
  if (pending.length > 0) {
    log(`Draining ${pending.length} pending IPC messages into initial prompt`);
    prompt += '\n' + pending.join('\n');
  }

  // Script phase: run script before waking agent
  if (containerInput.script && containerInput.isScheduledTask) {
    log('Running task script...');
    const scriptResult = await runScript(containerInput.script);

    if (!scriptResult || !scriptResult.wakeAgent) {
      const reason = scriptResult
        ? 'wakeAgent=false'
        : 'script error/no output';
      log(`Script decided not to wake agent: ${reason}`);
      writeOutput({
        status: 'success',
        result: null,
      });
      return;
    }

    // Script says wake agent — enrich prompt with script data
    log(`Script wakeAgent=true, enriching prompt with data`);
    prompt = `[SCHEDULED TASK]\n\nScript output:\n${JSON.stringify(scriptResult.data, null, 2)}\n\nInstructions:\n${containerInput.prompt}`;
  }

  // Query loop: run query → wait for IPC message → run new query → repeat
  let resumeAt: string | undefined;
  // P2: Capture the exit reason so session:end carries useful context.
  let exitReason: 'close-during-query' | 'close-sentinel' | 'error' | 'loop-exit' =
    'loop-exit';

  try {
    while (true) {
      log(
        `Starting query (session: ${sessionId || 'new'}, resumeAt: ${resumeAt || 'latest'})...`,
      );

      const queryResult = await runQuery(
        prompt,
        sessionId,
        mcpServerPath,
        containerInput,
        sdkEnv,
        resumeAt,
      );
      if (queryResult.newSessionId) {
        sessionId = queryResult.newSessionId;
      }
      if (queryResult.lastAssistantUuid) {
        resumeAt = queryResult.lastAssistantUuid;
      }

      // If _close was consumed during the query, exit immediately.
      // Don't emit a session-update marker (it would reset the host's
      // idle timer and cause a 30-min delay before the next _close).
      if (queryResult.closedDuringQuery) {
        log('Close sentinel consumed during query, exiting');
        exitReason = 'close-during-query';
        break;
      }

      // Self-reflection: scan for correction signals after each query
      try {
        await runSelfReflection(sessionId);
      } catch (err) {
        log(`Self-reflection error (non-fatal): ${err instanceof Error ? err.message : String(err)}`);
      }

      // Emit session update so host can track it
      writeOutput({ status: 'success', result: null, newSessionId: sessionId });

      log('Query ended, waiting for next IPC message...');

      // Wait for the next message or _close sentinel
      const nextMessage = await waitForIpcMessage();
      if (nextMessage === null) {
        log('Close sentinel received, exiting');
        exitReason = 'close-sentinel';
        break;
      }

      log(`Got new message (${nextMessage.length} chars), starting new query`);
      prompt = nextMessage;
    }
  } catch (err) {
    exitReason = 'error';
    const errorMessage = err instanceof Error ? err.message : String(err);
    log(`Agent error: ${errorMessage}`);
    // P2: emit session:end with error context before exiting.
    await fireLifecycleEvent(
      'session:end',
      containerInput.assistantName,
      sessionId,
      { exitReason, error: errorMessage },
    );
    writeOutput({
      status: 'error',
      result: null,
      newSessionId: sessionId,
      error: errorMessage,
    });
    process.exit(1);
  }

  // P2: Normal shutdown path — covers `break` exits above.
  await fireLifecycleEvent(
    'session:end',
    containerInput.assistantName,
    sessionId,
    { exitReason },
  );
}

main();
