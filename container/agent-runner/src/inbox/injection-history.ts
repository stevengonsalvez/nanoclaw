/**
 * Shared in-memory ring buffer for BANK injection -> correction join.
 *
 * Mirrors Hermes' fleet-hooks/injection_history.py. The bank-lookup hook
 * (writer) records every injection; the correction-detector hook (reader)
 * pulls recent entries on user-correction events to build the
 * correction-after-injection / validation-after-injection joins that the
 * BANK indexer uses to compute confirmed_count per row.
 *
 * Bounded per session at maxlen=5 — the join only looks at recent
 * injections within a 30-minute window, so older entries are dropped.
 */
export interface InjectionEntry {
  ts: number;
  injectionId: string;
  hitIds: string[];
}

const MAX_PER_SESSION = 5;
const _history: Map<string, InjectionEntry[]> = new Map();

/** Record a BANK injection so correction-detect can join it later. */
export function record(
  sessionId: string,
  injectionId: string,
  hitIds: string[],
): void {
  if (!sessionId || !injectionId) return;
  const list = _history.get(sessionId) ?? [];
  list.push({ ts: Date.now() / 1000, injectionId, hitIds: [...hitIds] });
  if (list.length > MAX_PER_SESSION) list.splice(0, list.length - MAX_PER_SESSION);
  _history.set(sessionId, list);
}

/** Return injections for this session within max_age_sec (default 30 min). */
export function recent(
  sessionId: string,
  maxAgeSec: number = 1800,
): InjectionEntry[] {
  if (!sessionId) return [];
  const list = _history.get(sessionId);
  if (!list) return [];
  const cutoff = Date.now() / 1000 - maxAgeSec;
  return list.filter((h) => h.ts >= cutoff);
}

/** Drop all history for a session (used on session close). */
export function clearSession(sessionId: string): void {
  _history.delete(sessionId);
}
