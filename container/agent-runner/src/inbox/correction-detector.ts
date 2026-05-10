/**
 * Correction Detector — multi-class signal capture (TS port of Pass 6).
 *
 * Mirrors Hermes fleet-hooks/correction_detector.py. Scans user_message
 * on UserPromptSubmit for any of:
 *   - correctionSignals  → corrections-raw.jsonl + pending-corrections
 *                          + finalize-on-Stop into corrections.md
 *   - validationSignals  → validations.jsonl (positive-feedback ledger)
 *   - knowledgeSignals   → discoveries.jsonl (knowledge stream)
 *   - frustrationMarkers → corrections-raw.jsonl + frustration_boost flag
 *
 * Precedence: frustration > knowledge > correction > validation.
 *
 * FP guards:
 *   - negationGuards — sliding 25-char prefix window before each match
 *   - excludePatterns / excludeContextMarkers — whole-message exclusions
 *
 * Stop hook finalises pending corrections (pre-armed on UserPromptSubmit)
 * with the assistant response. Validation and knowledge classes complete
 * deterministically in the pre-hook; only corrections need a Stop pass.
 *
 * Module-level Map keys are session_id, cleared by clearSession() (called
 * from the session:end lifecycle event in index.ts).
 */
import * as fs from 'node:fs';
import * as path from 'node:path';
import * as crypto from 'node:crypto';
import * as injectionHistory from './injection-history.js';
import { validateOrWarn } from './spec-validator.js';

interface CorrectionSignals {
  strong?: string[];
  medium?: string[];
  weak?: string[];
}

interface ValidationSignals {
  strong?: string[];
  medium?: string[];
}

interface KnowledgeSignals {
  root_cause?: string[];
  fix_confirmed?: string[];
  docs_vs_reality?: string[];
}

interface SignalsConfig {
  schemaVersion?: number;
  correctionSignals?: CorrectionSignals;
  validationSignals?: ValidationSignals;
  knowledgeSignals?: KnowledgeSignals;
  frustrationMarkers?: string[];
  negationGuards?: string[];
  excludePatterns?: string[];
  excludeContextMarkers?: string[];
  minimumConfidence?: 'weak' | 'medium' | 'strong';
}

type SignalClass = 'correction' | 'validation' | 'knowledge' | 'frustration' | null;
type Confidence = 'strong' | 'medium' | 'weak' | null;
type KnowledgeSubkey = 'root_cause' | 'fix_confirmed' | 'docs_vs_reality';

interface PendingRaw {
  id: string;
  ts: string;
  session_id: string;
  agent: string;
  platform: string;
  user_message: string;
  matched_signals: string[];
  confidence: Confidence;
  signal_class: SignalClass;
  frustration_boost: boolean;
  assistant_response: string | null;
  pending_corr_id: string;
}

const _pendingFlags: Map<string, string[]> = new Map();
const _pendingRaw: Map<string, PendingRaw> = new Map();

let _signalsCache: SignalsConfig | null = null;

function loadSignals(signalsPath: string): SignalsConfig {
  if (_signalsCache) return _signalsCache;
  try {
    const raw = fs.readFileSync(signalsPath, 'utf-8');
    _signalsCache = JSON.parse(raw) as SignalsConfig;
  } catch (err) {
    console.error(
      `[correction-detector] Failed to load signals.json: ${err instanceof Error ? err.message : String(err)}`,
    );
    _signalsCache = { correctionSignals: {}, excludePatterns: [], minimumConfidence: 'medium' };
  }
  return _signalsCache;
}

function nowIso(): string {
  return new Date().toISOString().replace(/\.\d{3}Z$/, 'Z');
}

function uid(prefix: string): string {
  return `${prefix}-${crypto.randomBytes(6).toString('hex')}`;
}

function clanLearningsDir(): string {
  return process.env.CLAN_LEARNINGS_DIR || path.join(process.env.HOME || '~', '.clan', 'learnings');
}

function isNegated(textLower: string, matchIdx: number, negationGuards: string[]): boolean {
  if (matchIdx <= 0) return false;
  const start = Math.max(0, matchIdx - 25);
  const prefix = textLower.slice(start, matchIdx);
  return negationGuards.some((neg) => prefix.includes(neg));
}

function firstUnnegatedMatch(
  textLower: string,
  patterns: string[] | undefined,
  negationGuards: string[],
): string | null {
  if (!patterns) return null;
  for (const sig of patterns) {
    const sigL = sig.toLowerCase();
    const idx = textLower.indexOf(sigL);
    if (idx >= 0 && !isNegated(textLower, idx, negationGuards)) return sig;
  }
  return null;
}

interface DetectResult {
  signalClass: SignalClass;
  confidence: Confidence;
  matched: string[];
  subkey: KnowledgeSubkey | null;
}

export function detectSignals(text: string, signals: SignalsConfig): DetectResult {
  const textLower = text.toLowerCase();
  const empty: DetectResult = { signalClass: null, confidence: null, matched: [], subkey: null };

  // 1. Whole-message exclusions
  for (const p of signals.excludePatterns || []) {
    if (textLower.includes(p.toLowerCase())) return empty;
  }
  for (const m of signals.excludeContextMarkers || []) {
    if (textLower.includes(m.toLowerCase())) return empty;
  }

  const negationGuards = (signals.negationGuards || []).map((g) => g.toLowerCase());

  // 2. Frustration markers — top priority
  for (const marker of signals.frustrationMarkers || []) {
    const markerL = marker.toLowerCase();
    const idx = textLower.indexOf(markerL);
    if (idx >= 0 && !isNegated(textLower, idx, negationGuards)) {
      return { signalClass: 'frustration', confidence: 'strong', matched: [marker], subkey: null };
    }
  }

  // 3. Knowledge signals → discoveries.jsonl
  const knowledge = signals.knowledgeSignals || {};
  for (const sk of ['root_cause', 'fix_confirmed', 'docs_vs_reality'] as KnowledgeSubkey[]) {
    const m = firstUnnegatedMatch(textLower, knowledge[sk], negationGuards);
    if (m) {
      return { signalClass: 'knowledge', confidence: 'medium', matched: [m], subkey: sk };
    }
  }

  // 4. Correction signals
  const correction = signals.correctionSignals || {};
  let matched: string[] = [];
  let confidence: Confidence = null;

  let m = firstUnnegatedMatch(textLower, correction.strong, negationGuards);
  if (m) {
    matched.push(m);
    confidence = 'strong';
  }
  if (!confidence) {
    m = firstUnnegatedMatch(textLower, correction.medium, negationGuards);
    if (m) {
      matched.push(m);
      confidence = 'medium';
    }
  }
  if (!confidence && (signals.minimumConfidence || 'medium') === 'weak') {
    m = firstUnnegatedMatch(textLower, correction.weak, negationGuards);
    if (m) {
      matched.push(m);
      confidence = 'weak';
    }
  }

  if (confidence) {
    const minConf = signals.minimumConfidence || 'medium';
    const order: Confidence[] = ['weak', 'medium', 'strong'];
    if (order.indexOf(confidence) < order.indexOf(minConf as Confidence)) return empty;
    return { signalClass: 'correction', confidence, matched, subkey: null };
  }

  // 5. Validation signals (lowest priority)
  const validation = signals.validationSignals || {};
  m = firstUnnegatedMatch(textLower, validation.strong, negationGuards);
  if (m) return { signalClass: 'validation', confidence: 'strong', matched: [m], subkey: null };
  m = firstUnnegatedMatch(textLower, validation.medium, negationGuards);
  if (m) return { signalClass: 'validation', confidence: 'medium', matched: [m], subkey: null };

  return empty;
}

function appendJsonl(filePath: string, record: object): void {
  try {
    fs.mkdirSync(path.dirname(filePath), { recursive: true });
    fs.appendFileSync(filePath, JSON.stringify(record) + '\n');
  } catch (err) {
    console.error(
      `[correction-detector] write failed (${filePath}): ${err instanceof Error ? err.message : String(err)}`,
    );
  }
}

function recordCorrectionAfterInjection(
  sessionId: string,
  rawCorrectionId: string,
  matchedSignals: string[],
  confidence: Confidence,
): void {
  const recent = injectionHistory.recent(sessionId);
  if (recent.length === 0) return;
  const filePath = path.join(clanLearningsDir(), 'correction-after-injection.jsonl');
  const now = Date.now() / 1000;
  for (const inj of recent) {
    appendJsonl(filePath, {
      ts: nowIso(),
      injection_id: inj.injectionId,
      injection_age_sec: Math.floor(now - inj.ts),
      correction_id: rawCorrectionId,
      session_id: sessionId,
      matched_signals: matchedSignals,
      confidence,
      hit_ids_offered: inj.hitIds,
      hit_used: null,
    });
  }
}

function recordValidationAfterInjection(
  sessionId: string,
  validationId: string,
  matchedSignals: string[],
  confidence: Confidence,
): void {
  const recent = injectionHistory.recent(sessionId);
  if (recent.length === 0) return;
  const filePath = path.join(clanLearningsDir(), 'validation-after-injection.jsonl');
  const now = Date.now() / 1000;
  for (const inj of recent) {
    appendJsonl(filePath, {
      ts: nowIso(),
      injection_id: inj.injectionId,
      injection_age_sec: Math.floor(now - inj.ts),
      validation_id: validationId,
      session_id: sessionId,
      matched_signals: matchedSignals,
      confidence,
      hit_ids_confirmed: inj.hitIds,
    });
  }
}

function appendCorrectionsMd(
  correctionsMdPath: string,
  userMessage: string,
  assistantResponse: string,
  matched: string[],
  confidence: Confidence,
): boolean {
  try {
    fs.mkdirSync(path.dirname(correctionsMdPath), { recursive: true });
  } catch (err) {
    console.error(
      `[correction-detector] mkdir failed: ${err instanceof Error ? err.message : String(err)}`,
    );
    return false;
  }

  const ts = new Date()
    .toISOString()
    .replace('T', ' ')
    .slice(0, 16);
  const userExcerpt = (userMessage || '').slice(0, 500).replace(/\n/g, ' ');
  const asstExcerpt = (assistantResponse || '').slice(0, 500).replace(/\n/g, ' ');
  const signalsStr = matched.slice(0, 3).join(', ');

  const entry =
    `\n\n## ${ts} — auto-detected correction\n` +
    `**What I got wrong:** ${asstExcerpt}\n` +
    `**User flagged:** ${userExcerpt}\n` +
    `**Signal:** ${signalsStr} (confidence=${confidence})\n` +
    `**Source:** auto-detected\n` +
    `**Pattern:** _to be inferred at promotion time_\n`;

  // Cheap dedup: skip if same user excerpt landed in last 60 seconds.
  if (fs.existsSync(correctionsMdPath)) {
    try {
      const tail = fs.readFileSync(correctionsMdPath, 'utf-8').slice(-2000);
      const lastTsMatch = tail.match(/## (\d{4}-\d{2}-\d{2} \d{2}:\d{2})/);
      if (lastTsMatch) {
        const lastTs = Date.parse(lastTsMatch[1].replace(' ', 'T') + 'Z') / 1000;
        if (
          Date.now() / 1000 - lastTs < 60 &&
          tail.includes(userExcerpt.slice(0, 80))
        ) {
          console.error('[correction-detector] dedup: skipped duplicate within 60s');
          return false;
        }
      }
    } catch {
      // dedup is best-effort
    }
  }

  try {
    fs.appendFileSync(correctionsMdPath, entry);
    return true;
  } catch (err) {
    console.error(
      `[correction-detector] corrections.md append failed: ${err instanceof Error ? err.message : String(err)}`,
    );
    return false;
  }
}

function updatePendingStatus(pendingPath: string, ids: string[], newStatus: string): void {
  if (!fs.existsSync(pendingPath) || ids.length === 0) return;
  const idsSet = new Set(ids);
  let raw: string;
  try {
    raw = fs.readFileSync(pendingPath, 'utf-8');
  } catch {
    return;
  }
  const lines = raw.split('\n');
  const out: string[] = [];
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    try {
      const entry = JSON.parse(trimmed) as { id?: string; status?: string; updated_at?: string };
      if (entry.id && idsSet.has(entry.id)) {
        entry.status = newStatus;
        entry.updated_at = nowIso();
      }
      out.push(JSON.stringify(entry));
    } catch {
      out.push(line);
    }
  }
  try {
    fs.writeFileSync(pendingPath, out.join('\n') + (out.length ? '\n' : ''));
  } catch (err) {
    console.error(
      `[correction-detector] pending status update failed: ${err instanceof Error ? err.message : String(err)}`,
    );
  }
}

export interface CorrectionDetectorPaths {
  signalsPath: string;
  workspaceGroup: string;
  agent: string;
  platform: string;
}

/**
 * UserPromptSubmit handler. Routes by signal class:
 *   - correction/frustration → corrections-raw + pending + arm-on-Stop
 *   - validation → validations.jsonl (no markdown side effect)
 *   - knowledge → discoveries.jsonl (no markdown side effect)
 *
 * Returns nothing — the LLM is no longer responsible for formatting.
 */
export function onUserPromptSubmit(
  sessionId: string,
  userMessage: string,
  paths: CorrectionDetectorPaths,
): void {
  if (!userMessage || !sessionId) return;

  const signals = loadSignals(paths.signalsPath);
  const det = detectSignals(userMessage, signals);
  if (!det.signalClass) return;

  const platform = paths.platform;

  // Validation: write + record + return.
  if (det.signalClass === 'validation') {
    const validationId = uid('val');
    const record = {
      id: validationId,
      ts: nowIso(),
      session_id: sessionId,
      agent: paths.agent,
      platform,
      user_message: userMessage,
      matched_signals: det.matched,
      confidence: det.confidence,
      signal_class: 'validation',
    };
    appendJsonl(path.join(clanLearningsDir(), 'validations.jsonl'), record);
    recordValidationAfterInjection(sessionId, validationId, det.matched, det.confidence);
    console.error(
      `[correction-detector] validation captured (${det.confidence}) session=${sessionId.slice(0, 8)}`,
    );
    return;
  }

  // Knowledge: write to discoveries.jsonl.
  if (det.signalClass === 'knowledge') {
    const excerpt = userMessage.trim().slice(0, 200);
    const short = excerpt.split('.')[0]?.slice(0, 80) || 'knowledge signal';
    const record = {
      id: uid('disc'),
      ts: nowIso(),
      agent: paths.agent,
      session_id: sessionId,
      platform,
      title: `[${det.subkey || 'general'}] ${short}`,
      insight: excerpt,
      matched_signals: det.matched,
      subkey: det.subkey || 'general',
      source: 'knowledge-signal',
    };
    appendJsonl(path.join(clanLearningsDir(), 'discoveries.jsonl'), record);
    console.error(
      `[correction-detector] knowledge signal (${det.subkey}) session=${sessionId.slice(0, 8)}`,
    );
    return;
  }

  // Correction or frustration — share pipeline; latter sets boost flag.
  const isFrustration = det.signalClass === 'frustration';

  // Legacy pending entry (heartbeat counters).
  const pendingPath = path.join(paths.workspaceGroup, 'self-improving', 'pending-corrections.jsonl');
  const corrId = `corr-${Date.now()}`;
  appendJsonl(pendingPath, {
    id: corrId,
    timestamp: nowIso(),
    session_id: sessionId,
    message_excerpt: userMessage.slice(0, 500),
    confidence: det.confidence,
    matched_signals: det.matched,
    status: 'pending',
  });
  const flags = _pendingFlags.get(sessionId) || [];
  flags.push(corrId);
  _pendingFlags.set(sessionId, flags);

  // Immutable audit (corrections-raw.jsonl, shared).
  const rawId = uid('raw');
  const rawRecord: PendingRaw = {
    id: rawId,
    ts: nowIso(),
    session_id: sessionId,
    agent: paths.agent,
    platform,
    user_message: userMessage,
    matched_signals: det.matched,
    confidence: det.confidence,
    signal_class: det.signalClass,
    frustration_boost: isFrustration,
    assistant_response: null,
    pending_corr_id: corrId,
  };
  validateOrWarn('correction-raw', rawRecord);
  appendJsonl(path.join(clanLearningsDir(), 'corrections-raw.jsonl'), rawRecord);
  _pendingRaw.set(sessionId, rawRecord);

  // Cross-fleet join.
  recordCorrectionAfterInjection(sessionId, rawId, det.matched, det.confidence);

  console.error(
    `[correction-detector] ${det.confidence} ${det.signalClass} session=${sessionId.slice(0, 8)} — auto-capture armed${isFrustration ? ' (frustration-boost)' : ''}`,
  );
}

/**
 * Stop handler. Finalises the deterministic capture for any session that
 * armed a correction/frustration raw record on UserPromptSubmit. Validation
 * and knowledge classes complete in the pre-hook so this is a no-op for them.
 */
export function onStop(
  sessionId: string,
  assistantResponse: string,
  paths: CorrectionDetectorPaths,
): void {
  if (!sessionId) return;
  const raw = _pendingRaw.get(sessionId);
  _pendingRaw.delete(sessionId);
  if (!raw) return;

  // Re-write the raw record with the assistant response folded in.
  const finalised = {
    ...raw,
    assistant_response: (assistantResponse || '').slice(0, 2000),
    finalized_ts: nowIso(),
  };
  appendJsonl(path.join(clanLearningsDir(), 'corrections-raw.jsonl'), finalised);

  // Deterministic markdown append.
  const correctionsMdPath = path.join(paths.workspaceGroup, 'self-improving', 'corrections.md');
  const appended = appendCorrectionsMd(
    correctionsMdPath,
    raw.user_message,
    assistantResponse,
    raw.matched_signals,
    raw.confidence,
  );

  const pendingIds = _pendingFlags.get(sessionId) || [];
  _pendingFlags.delete(sessionId);

  if (appended && pendingIds.length > 0) {
    updatePendingStatus(
      path.join(paths.workspaceGroup, 'self-improving', 'pending-corrections.jsonl'),
      pendingIds,
      'auto-captured',
    );
    console.error(
      `[correction-detector] auto-captured correction session=${sessionId.slice(0, 8)}`,
    );
  } else if (pendingIds.length > 0) {
    console.error(
      `[correction-detector] markdown append failed session=${sessionId.slice(0, 8)} — left pending`,
    );
  }
}

/** Drop per-session state. Called from session:end lifecycle event. */
export function clearSession(sessionId: string): void {
  _pendingFlags.delete(sessionId);
  _pendingRaw.delete(sessionId);
  injectionHistory.clearSession(sessionId);
}

/** Test-only: drop the cached signals so a sandbox can swap signals.json. */
export function _reloadSignalsForTest(): void {
  _signalsCache = null;
}
