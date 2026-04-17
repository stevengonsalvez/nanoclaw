/**
 * Discovery gossip helper — append non-obvious findings to the shared
 * ~/.clan/learnings/discoveries.jsonl store so partner agents (across
 * harnesses) can read them at session start.
 *
 * Mirrors the Python append-discovery script but is callable in-process
 * from agent-runner hooks without shelling out.
 */

import fs from 'fs';
import path from 'path';
import crypto from 'crypto';

const DISCOVERIES_PATH = path.join(
  process.env.HOME || '~',
  '.clan',
  'learnings',
  'discoveries.jsonl',
);

const HERMES_AGENTS = new Set(['freeman', 'motoko']);
const NANOCLAW_AGENTS = new Set(['geordi', 'data']);

export type DiscoveryType =
  | 'discovery'
  | 'gotcha'
  | 'tip'
  | 'workaround';

export type Confidence = 'low' | 'medium' | 'high';

export interface DiscoveryEntry {
  id?: string;
  uuid?: string;
  type: DiscoveryType;
  agent: string;
  harness?: string;
  clan?: string;
  ts?: string;
  context: string;
  problem: string;
  solution: string;
  tags?: string[];
  confidence?: Confidence;
  supersedes?: string | null;
}

function inferHarness(agent: string): string {
  if (HERMES_AGENTS.has(agent)) return 'hermes';
  if (NANOCLAW_AGENTS.has(agent)) return 'nanoclaw';
  return 'unknown';
}

/**
 * Append a discovery entry to ~/.clan/learnings/discoveries.jsonl.
 * Creates the file and parent dirs if missing. Fills default fields.
 * Non-throwing — returns false on error.
 */
export function appendDiscovery(entry: DiscoveryEntry): boolean {
  try {
    const now = new Date().toISOString();
    const complete = {
      id: entry.id || `d-${entry.agent}-${Date.now()}`,
      uuid: entry.uuid || crypto.randomUUID(),
      type: entry.type,
      agent: entry.agent,
      harness: entry.harness || inferHarness(entry.agent),
      clan: entry.clan || 'lambda',
      ts: entry.ts || now,
      context: entry.context,
      problem: entry.problem,
      solution: entry.solution,
      tags: entry.tags || [],
      confidence: entry.confidence || 'medium',
      supersedes: entry.supersedes ?? null,
    };

    fs.mkdirSync(path.dirname(DISCOVERIES_PATH), { recursive: true });
    fs.appendFileSync(DISCOVERIES_PATH, JSON.stringify(complete) + '\n');
    return true;
  } catch {
    return false;
  }
}

/**
 * Read the last N discoveries. Used by SessionStart hook to inject
 * recent gossip into the agent context.
 */
export function readRecentDiscoveries(n = 20): string[] {
  if (!fs.existsSync(DISCOVERIES_PATH)) return [];
  try {
    return fs
      .readFileSync(DISCOVERIES_PATH, 'utf-8')
      .split('\n')
      .filter((l) => l.trim())
      .slice(-n);
  } catch {
    return [];
  }
}
