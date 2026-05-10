/**
 * Convex Inbox Client — thin HTTP client for mission-control integration.
 *
 * Provides inbox polling, acknowledgement, completion, and event posting
 * against a Convex deployment's HTTP API. Auth via X-MC-Token header.
 *
 * This module is framework-agnostic — it can be used from agent-runner hooks,
 * heartbeat scripts, or standalone CLI tools.
 */

import fs from 'fs';
import path from 'path';

const REQUEST_TIMEOUT_MS = 3000;
const AUTH_TOKEN_TTL_MS = 60_000; // Cache auth token for 60s
const AUTH_TOKEN_PATH =
  process.env.MC_AUTH_TOKEN_PATH ||
  path.join(process.env.HOME || '~', '.secrets', 'mc-auth-token');

// Cached auth token + expiry — avoids per-request disk read at heartbeat rate.
let _cachedToken: string | null = null;
let _cachedTokenExpiry = 0;

export interface InboxItem {
  _id: string;
  agent: string;
  project: string;
  clan?: string;
  title: string;
  body: string;
  status: string;
  labels?: string[];
}

export interface AgentEvent {
  type: string;
  agent: string;
  project: string;
  clan: string;
  data?: Record<string, unknown>;
  ts?: string;
}

export interface ConvexConfig {
  convexUrl: string;
  agent: string;
  project: string;
  clan: string;
}

const SKIP_LABELS = new Set([
  'parked',
  'needs-approval',
  'blocked',
  'later',
  'in-review',
]);

function readAuthToken(): string {
  const now = Date.now();
  if (_cachedToken && now < _cachedTokenExpiry) {
    return _cachedToken;
  }
  try {
    const token = fs.readFileSync(AUTH_TOKEN_PATH, 'utf-8').trim();
    _cachedToken = token;
    _cachedTokenExpiry = now + AUTH_TOKEN_TTL_MS;
    return token;
  } catch {
    throw new Error(
      `Cannot read auth token from ${AUTH_TOKEN_PATH}. Create it with: echo "<token>" > ${AUTH_TOKEN_PATH}`,
    );
  }
}

/** Force-reload the cached token on next call. Used by tests / rotation. */
export function invalidateTokenCache(): void {
  _cachedToken = null;
  _cachedTokenExpiry = 0;
}

async function fetchWithTimeout(
  url: string,
  options: RequestInit,
  timeoutMs = REQUEST_TIMEOUT_MS,
): Promise<Response> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, { ...options, signal: controller.signal });
  } finally {
    clearTimeout(timer);
  }
}

function authHeaders(): Record<string, string> {
  return {
    'Content-Type': 'application/json',
    'X-MC-Token': readAuthToken(),
  };
}

/**
 * Poll the Convex inbox for pending items.
 * Filters out items with skip-labels automatically.
 */
export async function pollInbox(config: ConvexConfig): Promise<InboxItem[]> {
  const { convexUrl, agent, project, clan } = config;
  const url = `${convexUrl}/api/inbox/poll?agent=${encodeURIComponent(agent)}&project=${encodeURIComponent(project)}&clan=${encodeURIComponent(clan)}`;

  const response = await fetchWithTimeout(url, {
    method: 'GET',
    headers: authHeaders(),
  });

  if (!response.ok) {
    throw new Error(`Inbox poll failed: ${response.status} ${response.statusText}`);
  }

  const items: InboxItem[] = await response.json();
  return items.filter(
    (item) => !item.labels?.some((label) => SKIP_LABELS.has(label)),
  );
}

/** Acknowledge an inbox item (marks it as "acked"). */
export async function ackItem(
  convexUrl: string,
  inboxId: string,
  agentId: string,
): Promise<void> {
  await fetchWithTimeout(`${convexUrl}/api/inbox/ack`, {
    method: 'POST',
    headers: authHeaders(),
    body: JSON.stringify({ inboxId, agentId }),
  });
}

/** Complete an inbox item with a result summary. */
export async function completeItem(
  convexUrl: string,
  inboxId: string,
  agentId: string,
  result: string,
): Promise<void> {
  await fetchWithTimeout(`${convexUrl}/api/inbox/complete`, {
    method: 'POST',
    headers: authHeaders(),
    body: JSON.stringify({ inboxId, agentId, result }),
  });
}

export interface CreateItemRequest {
  agent: string;
  project: string;
  clan: string;
  title: string;
  body: string;
  labels?: string[];
  targetAgent?: string;
}

export interface CreateItemResponse {
  inboxId: string;
}

/**
 * Create a new inbox item. Used by agents to hand work off to each other
 * or to file mortal-inbox requests for Stevie.
 */
export async function createItem(
  convexUrl: string,
  req: CreateItemRequest,
): Promise<CreateItemResponse> {
  const response = await fetchWithTimeout(`${convexUrl}/api/inbox/create`, {
    method: 'POST',
    headers: authHeaders(),
    body: JSON.stringify(req),
  });
  if (!response.ok) {
    throw new Error(`Inbox create failed: ${response.status} ${response.statusText}`);
  }
  return (await response.json()) as CreateItemResponse;
}

/**
 * Post an event to Convex (fire-and-forget).
 * Does not throw on failure — logs to stderr instead.
 */
export async function postEvent(
  convexUrl: string,
  event: AgentEvent,
): Promise<void> {
  event.ts = event.ts || new Date().toISOString();
  try {
    await fetchWithTimeout(`${convexUrl}/api/events`, {
      method: 'POST',
      headers: authHeaders(),
      body: JSON.stringify(event),
    });
  } catch (err) {
    console.error(
      `[convex-client] Failed to post event: ${err instanceof Error ? err.message : String(err)}`,
    );
  }
}

/**
 * Shared ACP-metrics record shape used across harnesses.
 * Emitted on thread close (via /api/metrics/acp) + persisted locally.
 */
export interface ACPMetrics {
  threadId: string;
  protocol: 'acp/1' | 'natural' | 'mixed';
  messageCount: number;
  agentsInvolved: string[];
  timeToResolutionMin: number;
  estimatedTokens?: number;
  loopsDetected: number;
  humanInterventions: number;
  outcome: 'resolved' | 'handed-off' | 'abandoned';
  project?: string;
  clan?: string;
  harness?: string;
  ts?: string;
}

/**
 * Post ACP metrics on thread close. Fire-and-forget, logs failures.
 */
export async function postMetric(
  convexUrl: string,
  metric: ACPMetrics,
): Promise<void> {
  metric.ts = metric.ts || new Date().toISOString();
  try {
    await fetchWithTimeout(`${convexUrl}/api/metrics/acp`, {
      method: 'POST',
      headers: authHeaders(),
      body: JSON.stringify(metric),
    });
  } catch (err) {
    console.error(
      `[convex-client] Failed to post metric: ${err instanceof Error ? err.message : String(err)}`,
    );
  }
}

/**
 * Cross-fleet violation record posted to /api/violations/report on
 * circuit-breaker open, content-validation failure, etc. Schema mirrors
 * the Hermes mc_client.post() payload so the receiver is fleet-agnostic.
 */
export interface ViolationRecord {
  agent: string;
  clan: string;
  ruleId: string;
  description: string;
  severity: 'warning' | 'violation';
  snippet?: string;
  ts?: string;
}

/**
 * Post a violation to mission-control. Fire-and-forget, logs failures.
 */
export async function postViolation(
  convexUrl: string,
  violation: ViolationRecord,
): Promise<void> {
  violation.ts = violation.ts || new Date().toISOString();
  try {
    await fetchWithTimeout(`${convexUrl}/api/violations/report`, {
      method: 'POST',
      headers: authHeaders(),
      body: JSON.stringify(violation),
    });
  } catch (err) {
    console.error(
      `[convex-client] Failed to post violation: ${err instanceof Error ? err.message : String(err)}`,
    );
  }
}

/**
 * Prioritize inbox items per Fleet Lambda protocol:
 * 1. sev-1, sev-2 (incidents — immediate)
 * 2. deploy, handoff (partner handoff)
 * 3. everything else (routine)
 */
export function prioritizeItems(items: InboxItem[]): InboxItem[] {
  const priority = (item: InboxItem): number => {
    const labels = item.labels || [];
    if (labels.includes('sev-1') || labels.includes('sev-2')) return 0;
    if (labels.includes('deploy') || labels.includes('handoff')) return 1;
    return 2;
  };
  return [...items].sort((a, b) => priority(a) - priority(b));
}

/**
 * Load ConvexConfig from an agent-config.yaml file.
 * Parses the YAML manually (key: value format) to avoid yq dependency.
 */
export function loadConvexConfig(
  agentConfigPath: string,
  agentName: string,
): ConvexConfig | null {
  if (!fs.existsSync(agentConfigPath)) return null;

  const content = fs.readFileSync(agentConfigPath, 'utf-8');
  const lines = content.split('\n');

  let convexUrl = '';
  let project = '';
  let clan = '';

  for (const line of lines) {
    const match = line.match(/^(\w[\w_]*):\s*(.+)$/);
    if (!match) continue;
    const [, key, value] = match;
    const trimmed = value.trim().replace(/^["']|["']$/g, '');
    if (key === 'convex_url') convexUrl = trimmed;
    else if (key === 'project') project = trimmed;
    else if (key === 'clan') clan = trimmed;
  }

  if (!convexUrl || !project || !clan) return null;

  return { convexUrl, agent: agentName, project, clan };
}
