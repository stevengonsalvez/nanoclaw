/**
 * Circuit Breaker — track consecutive-failure count per agent and halt
 * with a STUCK.flag when a threshold is crossed.
 *
 * Mirrors Hermes fleet-hooks/circuit_breaker.py.
 *
 * Design:
 * - Pluggable: other hooks call recordFailure(...) when they detect
 *   a failure (tool error, 3rd-strike correction, rate limit, etc.).
 * - Counter-based: consecutive failures persist as JSONL rows in
 *   <WORKSPACE_GROUP>/self-improving/.circuit-breaker.jsonl.
 *   reset() appends a "reset" row that resets the counter.
 * - Halt signal: at threshold the breaker writes
 *   <WORKSPACE_GROUP>/self-improving/STUCK.flag and posts a violation
 *   to mission-control via /api/violations/report. Heartbeat / startup
 *   logic can read STUCK.flag and skip work while it is present.
 * - Manual override: if `.resume-after-stuck` exists in the same dir,
 *   STUCK.flag is cleared and the counter is reset on next checkResumeMarker
 *   (called from the Stop hook chain).
 *
 * Related standing order: Rule 15 (3-Failure Mutation Protocol).
 */
import * as fs from 'node:fs';
import * as path from 'node:path';
import { postViolation, loadConvexConfig } from './convex-client.js';

export const THRESHOLD = 3;

interface FailureRow {
  ts: string;
  kind: 'failure' | 'reset';
  signal?: string;
  detail?: Record<string, unknown>;
  reason?: string;
}

function counterPath(workspaceGroup: string): string {
  return path.join(workspaceGroup, 'self-improving', '.circuit-breaker.jsonl');
}

function stuckFlagPath(workspaceGroup: string): string {
  return path.join(workspaceGroup, 'self-improving', 'STUCK.flag');
}

function resumeFlagPath(workspaceGroup: string): string {
  return path.join(workspaceGroup, 'self-improving', '.resume-after-stuck');
}

function nowIso(): string {
  return new Date().toISOString().replace(/\.\d{3}Z$/, 'Z');
}

function appendRow(workspaceGroup: string, row: FailureRow): void {
  const p = counterPath(workspaceGroup);
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.appendFileSync(p, JSON.stringify(row) + '\n');
}

/** Walk the JSONL backwards from end, count contiguous "failure" rows. */
function countConsecutiveFailures(workspaceGroup: string): number {
  const p = counterPath(workspaceGroup);
  if (!fs.existsSync(p)) return 0;
  let raw: string;
  try {
    raw = fs.readFileSync(p, 'utf-8');
  } catch {
    return 0;
  }
  const lines = raw.split('\n');
  let count = 0;
  for (let i = lines.length - 1; i >= 0; i--) {
    const line = lines[i].trim();
    if (!line) continue;
    try {
      const row = JSON.parse(line) as FailureRow;
      if (row.kind === 'failure') count += 1;
      else break;
    } catch {
      continue;
    }
  }
  return count;
}

export function isStuck(workspaceGroup: string): boolean {
  return fs.existsSync(stuckFlagPath(workspaceGroup));
}

async function openCircuit(
  workspaceGroup: string,
  agent: string,
  signal: string,
  detail: Record<string, unknown>,
  count: number,
): Promise<void> {
  const flag = stuckFlagPath(workspaceGroup);
  const content = {
    opened_at: nowIso(),
    signal,
    consecutive_failures: count,
    detail,
    guidance:
      'Circuit opened per Rule 15. Declare a named M1-M8 pivot in JOURNAL.md ' +
      `before continuing. Create ${resumeFlagPath(workspaceGroup)} to clear.`,
  };
  try {
    fs.mkdirSync(path.dirname(flag), { recursive: true });
    fs.writeFileSync(flag, JSON.stringify(content, null, 2));
    console.error(
      `[circuit-breaker] OPENED for ${agent} signal=${signal} consecutive=${count}`,
    );
  } catch (err) {
    console.error(
      `[circuit-breaker] flag write failed: ${err instanceof Error ? err.message : String(err)}`,
    );
  }

  // Best-effort MC violation post.
  try {
    const cfg = loadConvexConfig(
      path.join(workspaceGroup, 'agent-config.yaml'),
      agent,
    );
    if (cfg && cfg.convexUrl) {
      await postViolation(cfg.convexUrl, {
        agent,
        clan: 'lambda',
        ruleId: 'rule-15-mutation',
        description: `Circuit opened after ${count} consecutive failures (signal=${signal})`,
        severity: 'violation',
        snippet: JSON.stringify({ signal, detail, guidance: content.guidance }).slice(0, 500),
        ts: nowIso(),
      });
    }
  } catch (err) {
    console.error(
      `[circuit-breaker] mc post skipped: ${err instanceof Error ? err.message : String(err)}`,
    );
  }
}

/**
 * Record a failure for an agent and, if threshold reached, open the circuit.
 * Returns the current consecutive-failure count.
 */
export async function recordFailure(
  workspaceGroup: string,
  agent: string,
  signal: string = 'generic',
  detail: Record<string, unknown> = {},
): Promise<number> {
  appendRow(workspaceGroup, {
    ts: nowIso(),
    kind: 'failure',
    signal,
    detail,
  });
  const count = countConsecutiveFailures(workspaceGroup);
  if (count >= THRESHOLD && !isStuck(workspaceGroup)) {
    await openCircuit(workspaceGroup, agent, signal, detail, count);
  }
  return count;
}

/** Append a success row and clear STUCK.flag if present. */
export function reset(workspaceGroup: string, reason: string = ''): void {
  appendRow(workspaceGroup, {
    ts: nowIso(),
    kind: 'reset',
    reason,
  });
  const flag = stuckFlagPath(workspaceGroup);
  try {
    if (fs.existsSync(flag)) {
      fs.unlinkSync(flag);
      console.error(`[circuit-breaker] STUCK.flag cleared (reason=${reason})`);
    }
  } catch (err) {
    console.error(
      `[circuit-breaker] flag clear failed: ${err instanceof Error ? err.message : String(err)}`,
    );
  }
}

/**
 * Stop-hook entry point: if .resume-after-stuck exists, read it, delete it,
 * and reset the circuit. Mirrors Hermes circuit_breaker.on_post_llm.
 */
export function checkResumeMarker(workspaceGroup: string): void {
  const resume = resumeFlagPath(workspaceGroup);
  if (!fs.existsSync(resume)) return;
  let reason = 'manual';
  try {
    reason = fs.readFileSync(resume, 'utf-8').trim() || 'manual';
    fs.unlinkSync(resume);
  } catch (err) {
    console.error(
      `[circuit-breaker] resume marker read failed: ${err instanceof Error ? err.message : String(err)}`,
    );
    return;
  }
  reset(workspaceGroup, `resume-marker:${reason}`);
}
