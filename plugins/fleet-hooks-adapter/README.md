# fleet-hooks-adapter

Canonical NanoClaw implementation of [fleet-hooks-spec](../../../hermes-fleet-backup/plugins/fleet-hooks-spec/), mirroring [Hermes plugins/fleet-hooks/](../../../hermes-fleet-backup/plugins/fleet-hooks/).

This is the wrapper Stevie asked for: a single canonical surface for the 5 spec-conformant hooks NanoClaw provides, with `composeUserPromptSubmit` / `composeStop` / `composeSessionStart` handlers that delegate to spec-driven logic.

## Layout

| Path                                                            | Role                                                |
|-----------------------------------------------------------------|-----------------------------------------------------|
| `plugin.yaml`                                                   | Manifest — version, spec hooks, runtime module path |
| `README.md`                                                     | this file                                           |
| `../../container/agent-runner/src/fleet-hooks-adapter/index.ts` | Runtime module (TypeScript)                         |

NanoClaw's runtime is the agent-runner container, so the actual code lives under `container/agent-runner/src/fleet-hooks-adapter/`. This top-level `plugins/fleet-hooks-adapter/` directory holds the manifest + documentation that mirrors Hermes's conventional plugin shape.

## Spec hooks provided

| Hook              | SDK event           | Behavior file                                  | Schemas emitted                                |
|-------------------|---------------------|------------------------------------------------|------------------------------------------------|
| session-rules     | `UserPromptSubmit`  | `behaviors/session-rules.md`                   | `standing-orders-injection`                    |
| manifest-context  | `UserPromptSubmit`  | `behaviors/manifest-context.md`                | `manifest-context-injection`                   |
| learning-sync     | `Stop`              | `behaviors/learning-sync.md`                   | `pattern`, `learning-sync-state`, `strike`     |
| learning-verifier | `Stop`              | `behaviors/learning-verifier.md`               | `missed-learning`, `strike`                    |
| acp-metrics       | `Stop`              | `behaviors/acp-metrics.md`                     | `acp-metric`                                   |

The `SessionStart` slot is currently empty (no spec-conformant SessionStart hooks today). Scaffolded for future ports.

## Usage

```ts
import {
  composeUserPromptSubmit,
  composeStop,
  composeSessionStart,
} from './fleet-hooks-adapter/index.js';

const hooks = {
  UserPromptSubmit: [{ hooks: [
    ...composeUserPromptSubmit({ agentName }),
    // …nanoclaw-specific extras (BANK retrieval, correction-detector arming, …)
  ] }],
  Stop: [{ hooks: [
    // …nanoclaw-specific pre-spec hooks (inbox-enforcer, correction-detector finalize, …)
    ...composeStop({ agentName }),
    // …nanoclaw-specific post-spec hooks (Convex events, circuit-breaker resume, …)
  ] }],
};
```

The order inside `compose*` is locked by the adapter — `session-rules` precedes `manifest-context` so the full STANDING_ORDERS load on turn 1 sets baseline before repo-specific layering. `learning-sync` precedes `learning-verifier` so the strikes ledger is populated before signature-collision detection runs.

## Spec source of truth

Every hook in this adapter conforms to the published spec at:

```
~/d/git/hermes-fleet-backup/plugins/fleet-hooks-spec/
├── README.md             # PR template + contract
├── CHANGELOG.md          # cross-fleet audit trail
├── behaviors/            # canonical algorithms
├── schemas/              # JSON Schema (schemaVersion-stamped)
└── test-vectors/         # 59 cases, 13 schemas; both runners green
```

When `schemaVersion` of any schema bumps, both fleets MUST align within 7 days or CI fails. See spec README for the conflict protocol.

## Strictness

Validation is warn-only by default. Set `FLEET_HOOKS_SPEC_STRICT=1` (typically in CI) to throw on any record that fails its schema.

## Cross-fleet parity

| Concern                                | Status                                                                                       |
|----------------------------------------|----------------------------------------------------------------------------------------------|
| signature byte-for-byte parity         | Verified across 7 fingerprint cases (Python ↔ TypeScript)                                    |
| canonical record shapes                | All snake_case across both fleets                                                            |
| 59/59 test-vectors green               | Both Python (jsonschema) and Node (ajv 2020) runners pass                                    |
| Drift items                            | All schema-conformant or documented as deferred — see `fleet-hooks-spec/CHANGELOG.md` Week 4 |
