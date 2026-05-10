/**
 * Trigger-fingerprint signature for learning-sync.
 *
 * Mirrors Hermes plugins/fleet-hooks/learning_sync.py byte-for-byte. The
 * canonical algorithm (Layer 2 of the dedup contract — see
 * fleet-hooks-spec/behaviors/learning-sync.md) is:
 *
 *   1. tokenize user_message.toLowerCase via [A-Za-z0-9][A-Za-z0-9_\-]+
 *   2. drop tokens with length < 4
 *   3. drop tokens in TRIGGER_STOPWORDS
 *   4. dedup preserving first-occurrence order
 *   5. cap at TRIGGER_TOP_K = 8 tokens
 *   6. sort the kept tokens alphabetically
 *   7. lowercase + strip + dedup + sort matched_signals
 *   8. raw = sorted_tokens.join('|') + '::' + sorted_signals.join('|')
 *   9. sha1(raw, utf-8) → first 12 hex chars
 *
 * Fallback when raw audit data is missing:
 *   1. norm_title  = title.trim().toLowerCase().replace(/\s+/, ' ')
 *   2. norm_problem = problem.toLowerCase().replace(/[^a-z0-9 ]+/g, ' ')
 *                                          .replace(/\s+/g, ' ').trim()
 *                                          .slice(0, 100)
 *   3. raw = norm_title + '|' + norm_problem
 *   4. 'L-' + sha1(raw, utf-8) first 10 hex chars  →  total length 12
 *
 * The 12-character signature is the cross-fleet dedup-on-append key
 * stamped onto every patterns.jsonl row. Hermes and NanoClaw MUST agree
 * byte-for-byte on the produced hash.
 */

import * as crypto from 'crypto';

export const TRIGGER_TOP_K = 8;
export const TRIGGER_MIN_TOKEN_LEN = 4;

const TRIGGER_TOKEN_RE = /[A-Za-z0-9][A-Za-z0-9_\-]+/g;

export const TRIGGER_STOPWORDS: ReadonlySet<string> = new Set(
  `the a an and or but if then else when where what which who whom whose
   while with without into onto about after before under over out of in on
   at by for to from as is are was were be been being do does did done
   have has had having i me my we our you your he she it they them their
   this that these those there here so not no nor can could should would
   will shall may might must just also than too very much many any all
   some each every other another such same own said say saying get got`
    .split(/\s+/)
    .filter((w) => w.length > 0),
);

function sha1Hex(input: string): string {
  return crypto.createHash('sha1').update(input, 'utf-8').digest('hex');
}

export function computeTriggerFingerprint(
  userMessage: string,
  matchedSignals: string[],
): string {
  const seen = new Set<string>();
  const tokens: string[] = [];
  const lowered = (userMessage || '').toLowerCase();
  const matches = lowered.matchAll(TRIGGER_TOKEN_RE);
  for (const m of matches) {
    const tok = m[0];
    if (tok.length < TRIGGER_MIN_TOKEN_LEN) continue;
    if (TRIGGER_STOPWORDS.has(tok)) continue;
    if (seen.has(tok)) continue;
    seen.add(tok);
    tokens.push(tok);
    if (tokens.length >= TRIGGER_TOP_K) break;
  }

  const sigTokens = [...tokens].sort();
  const sigSignals = Array.from(
    new Set(
      (matchedSignals || [])
        .filter((s) => s)
        .map((s) => s.toLowerCase().trim()),
    ),
  ).sort();

  const raw = sigTokens.join('|') + '::' + sigSignals.join('|');
  return sha1Hex(raw).slice(0, 12);
}

export function computeLegacyFingerprint(title: string, problem: string): string {
  const normTitle = (title || '').trim().toLowerCase().replace(/\s+/g, ' ');
  let normProblem = (problem || '').toLowerCase().replace(/[^a-z0-9 ]+/g, ' ');
  normProblem = normProblem.replace(/\s+/g, ' ').trim().slice(0, 100);
  const raw = `${normTitle}|${normProblem}`;
  return 'L-' + sha1Hex(raw).slice(0, 10);
}

/**
 * Pick the right fingerprint algorithm: trigger when both userMessage and
 * matchedSignals are present, legacy otherwise.
 */
export function computeSignature(opts: {
  userMessage?: string;
  matchedSignals?: string[];
  title?: string;
  problem?: string;
}): string {
  if (opts.userMessage && opts.matchedSignals && opts.matchedSignals.length > 0) {
    return computeTriggerFingerprint(opts.userMessage, opts.matchedSignals);
  }
  return computeLegacyFingerprint(opts.title || '', opts.problem || '');
}

/**
 * Read corrections-raw.jsonl (from CLAN_LEARNINGS_DIR or ~/.clan/learnings)
 * and return the most-recent matching entry for `sessionId`. Tail-reads
 * the last 500 lines — same bound Hermes uses.
 */
export function readLatestRawForSession(
  sessionId: string,
  rawPath: string,
  fs: typeof import('fs'),
): { user_message?: string; matched_signals?: string[] } | null {
  if (!sessionId || !fs.existsSync(rawPath)) return null;
  try {
    const lines = fs.readFileSync(rawPath, 'utf-8').split('\n');
    const tail = lines.slice(-500);
    let latest: any = null;
    for (const raw of tail) {
      const line = raw.trim();
      if (!line) continue;
      try {
        const entry = JSON.parse(line);
        if (entry.session_id !== sessionId) continue;
        // Prefer finalized (assistant_response non-null); fall back to latest.
        if (
          latest === null ||
          (entry.assistant_response !== null && latest.assistant_response === null)
        ) {
          latest = entry;
        } else {
          latest = entry;
        }
      } catch {
        /* ignore malformed lines */
      }
    }
    return latest;
  } catch {
    return null;
  }
}
