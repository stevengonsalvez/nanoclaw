/**
 * Fleet-hooks adapter — canonical NanoClaw implementation of
 * fleet-hooks-spec hooks.
 *
 * This module is the single named entry point for the 5 spec-conformant
 * hooks NanoClaw implements (mirroring Hermes plugins/fleet-hooks/):
 *
 *   1. session-rules         — STANDING_ORDERS injection (UserPromptSubmit)
 *   2. manifest-context      — .clan/manifest.yaml repo-context (UserPromptSubmit)
 *   3. learning-sync         — corrections.md → patterns.jsonl + strikes (Stop)
 *   4. learning-verifier     — signature-collision missed-learning (Stop)
 *   5. acp-metrics           — thread-close metric emission (Stop)
 *
 * The factories live alongside the agent-runner orchestrator in
 * container/agent-runner/src/index.ts; this module re-exports them with
 * stable names + metadata + per-event composers so callers register all
 * spec-conformant hooks via a single import.
 *
 * For the narrative, drift items, schemas, and test-vectors see the
 * fleet-hooks-spec sub-tree at:
 *   ~/d/git/hermes-fleet-backup/plugins/fleet-hooks-spec/
 */

import type { HookCallback } from '@anthropic-ai/claude-agent-sdk';
import {
  createSessionRulesHook,
  createRepoManifestContextHook,
  createLearningSyncHook,
  createLearningVerifierHook,
  createACPMetricsHook,
} from '../index.js';

/**
 * Spec generation this adapter targets. Bumped when fleet-hooks-spec
 * lands a schemaVersion change that requires NanoClaw impl changes.
 */
export const FLEET_HOOKS_SPEC_VERSION = 1;

/**
 * Metadata table — every spec-conformant hook NanoClaw provides, with
 * its SDK event, fleet-hooks-spec behavior file, and emitted schemas.
 */
export const SPEC_HOOKS = [
  {
    name: 'session-rules',
    sdkEvent: 'UserPromptSubmit',
    behavior: 'fleet-hooks-spec/behaviors/session-rules.md',
    schemas: ['standing-orders-injection'],
  },
  {
    name: 'manifest-context',
    sdkEvent: 'UserPromptSubmit',
    behavior: 'fleet-hooks-spec/behaviors/manifest-context.md',
    schemas: ['manifest-context-injection'],
  },
  {
    name: 'learning-sync',
    sdkEvent: 'Stop',
    behavior: 'fleet-hooks-spec/behaviors/learning-sync.md',
    schemas: ['pattern', 'learning-sync-state', 'strike'],
  },
  {
    name: 'learning-verifier',
    sdkEvent: 'Stop',
    behavior: 'fleet-hooks-spec/behaviors/learning-verifier.md',
    schemas: ['missed-learning', 'strike'],
  },
  {
    name: 'acp-metrics',
    sdkEvent: 'Stop',
    behavior: 'fleet-hooks-spec/behaviors/acp-metrics.md',
    schemas: ['acp-metric'],
  },
] as const;

export interface FleetHooksAdapterOptions {
  /** Agent name used to stamp pattern.agent / strike.agent / etc. */
  agentName?: string;
}

/**
 * Return the spec-conformant UserPromptSubmit hook callbacks in the
 * order the orchestrator should register them.
 *
 * Order matters: session-rules first (full STANDING_ORDERS load on turn 1
 * sets baseline), then manifest-context (repo-specific context layered
 * on top).
 */
export function composeUserPromptSubmit(
  _opts: FleetHooksAdapterOptions = {},
): HookCallback[] {
  return [createSessionRulesHook(), createRepoManifestContextHook()];
}

/**
 * Return the spec-conformant Stop hook callbacks. Order matters:
 * learning-sync runs first (writes patterns + strikes), then
 * learning-verifier reads the strikes ledger to detect missed-learnings,
 * then acp-metrics fires on detected thread-close.
 */
export function composeStop(opts: FleetHooksAdapterOptions = {}): HookCallback[] {
  return [
    createLearningSyncHook(opts.agentName),
    createLearningVerifierHook(),
    createACPMetricsHook(opts.agentName),
  ];
}

/**
 * Return the spec-conformant SessionStart hook callbacks. None today —
 * scaffolded for future spec'd hooks (e.g. discovery-context gossip
 * injection if it gets ported into the spec'd set).
 */
export function composeSessionStart(_opts: FleetHooksAdapterOptions = {}): HookCallback[] {
  return [];
}

// Re-export individual factories for fine-grained wiring (e.g. when
// non-spec'd nanoclaw extras need to interleave with spec'd hooks).
export {
  createSessionRulesHook,
  createRepoManifestContextHook,
  createLearningSyncHook,
  createLearningVerifierHook,
  createACPMetricsHook,
};
