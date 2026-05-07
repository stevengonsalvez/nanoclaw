/**
 * BANK tier retrieval wrapper — BM25 over shared Fleet learnings.
 *
 * NanoClaw is TypeScript; the BANK index (FTS5/sqlite) lives at
 * ~/.clan/learnings/bank.db and is driven by the Python index/query
 * scripts at ~/.clan/learnings/scripts/bank/. Keeping a single source
 * of truth for the retrieval logic means this wrapper just shells out
 * to `python3 query.py ...` and parses the JSON stdout.
 *
 * Design:
 *  - Synchronous child_process.execSync keeps callers simple (fits the
 *    existing SessionStart hook pattern which is also sync).
 *  - Short 1500ms timeout so a broken index can never block an agent
 *    turn.
 *  - Keyword extraction is symmetric with the Python bank_lookup hook
 *    so Hermes and NanoClaw return comparable results for the same
 *    prompt.
 */

import { execFileSync } from 'child_process';
import path from 'path';
import os from 'os';

const HOME = process.env.HOME || os.homedir();
const QUERY_SCRIPT = path.join(
  HOME,
  '.clan',
  'learnings',
  'scripts',
  'bank',
  'query.py',
);

const PYTHON_BIN = process.env.BANK_PYTHON_BIN || 'python3';

const STOPWORDS = new Set([
  'the', 'a', 'an', 'and', 'or', 'but', 'if', 'then', 'else',
  'when', 'where', 'what', 'which', 'who', 'whom', 'whose',
  'while', 'with', 'without', 'into', 'onto', 'about', 'after',
  'before', 'under', 'over', 'out', 'of', 'in', 'on', 'at', 'by',
  'for', 'to', 'from', 'as', 'is', 'are', 'was', 'were', 'be',
  'been', 'being', 'do', 'does', 'did', 'done', 'have', 'has',
  'had', 'having', 'this', 'that', 'these', 'those', 'there',
  'here', 'not', 'no', 'nor', 'can', 'could', 'should', 'would',
  'will', 'shall', 'may', 'might', 'must', 'just', 'also', 'than',
  'too', 'very', 'much', 'many', 'any', 'all', 'some', 'each',
  'every', 'other', 'another', 'such', 'same', 'own',
]);

export interface BankHit {
  id: string;
  kind: string;
  agent: string;
  ts: string;
  source: string;
  content: string;
  score: number;
  score_norm: number;
}

export interface BankQueryOptions {
  limit?: number;
  kind?: 'pattern' | 'discovery' | 'discovery-archive';
  agent?: string;
  since?: string; // ISO date
  raw?: boolean;
}

/**
 * Extract salient keywords from a user message. Symmetric with the
 * Python bank_lookup._keywords implementation.
 */
export function extractKeywords(userMessage: string, max = 8): string[] {
  const re = /[A-Za-z0-9][A-Za-z0-9_-]+/g;
  const seen = new Set<string>();
  const out: string[] = [];
  let m: RegExpExecArray | null;
  while ((m = re.exec(userMessage)) !== null) {
    const tok = m[0].toLowerCase();
    if (tok.length < 4) continue;
    if (STOPWORDS.has(tok)) continue;
    if (seen.has(tok)) continue;
    seen.add(tok);
    out.push(tok);
    if (out.length >= max) break;
  }
  return out;
}

/**
 * Run a BANK query. Returns [] on any failure — best-effort.
 */
export function queryBank(
  terms: string,
  opts: BankQueryOptions = {},
): BankHit[] {
  if (!terms.trim()) return [];

  const args: string[] = [QUERY_SCRIPT, terms];
  if (opts.limit) args.push('--limit', String(opts.limit));
  if (opts.kind) args.push('--kind', opts.kind);
  if (opts.agent) args.push('--agent', opts.agent);
  if (opts.since) args.push('--since', opts.since);
  if (opts.raw) args.push('--raw');

  try {
    const stdout = execFileSync(PYTHON_BIN, args, {
      timeout: 1500,
      encoding: 'utf-8',
      stdio: ['ignore', 'pipe', 'ignore'],
    });
    const parsed = JSON.parse(stdout);
    // query.py returns either an array (success) or {error, results: []}
    // (handled failure) — normalise both shapes.
    if (Array.isArray(parsed)) return parsed as BankHit[];
    if (parsed && Array.isArray(parsed.results)) return parsed.results;
    return [];
  } catch {
    // Any failure — missing python, missing db, timeout, JSON error —
    // is non-fatal. The caller carries on without injected context.
    return [];
  }
}

/**
 * Render BANK hits as a compact context block suitable for injection
 * into the SessionStart / pre-turn hook output.
 */
export function formatBankContext(hits: BankHit[], keywords: string[]): string {
  if (hits.length === 0) return '';
  const lines = [
    '## BANK Retrieval (fleet learnings index)',
    '',
    `Top matches for keywords: ${keywords.slice(0, 6).join(', ')}`,
    '',
  ];
  for (const hit of hits) {
    const ts = (hit.ts || '').slice(0, 10);
    const content = (hit.content || '').replace(/\n/g, ' ');
    const trimmed = content.length > 400 ? content.slice(0, 400) + ' …' : content;
    lines.push(`- [${hit.kind}] ${hit.agent} (${ts}): ${trimmed}`);
  }
  return lines.join('\n');
}
