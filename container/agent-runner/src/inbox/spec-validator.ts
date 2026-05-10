/**
 * Spec-driven schema validator for fleet-hooks output records.
 *
 * Loads JSON Schemas from the cross-fleet spec dir and validates write
 * payloads. Strictness is env-gated:
 *
 *   FLEET_HOOKS_SPEC_STRICT=1  -> throw on schema violation (CI/prod-strict)
 *   default                    -> warn-only (dev mode)
 *
 * `ajv` is loaded gracefully via dynamic require — if the dep is unavailable
 * we log ONCE and degrade to no-op. This lets the spec ship without forcing
 * a package.json bump on every install.
 *
 * Usage:
 *
 *   import { validateOrWarn } from './spec-validator';
 *   validateOrWarn('violation', record);
 *   // then write to disk as usual
 *
 * Schemas covered (basenames in the spec dir, sans .schema.json):
 *   - violation
 *   - correction-raw
 *   - inbox-payload
 *
 * Spec-dir resolution order:
 *   1. FLEET_HOOKS_SPEC_DIR env var (explicit override)
 *   2. ~/d/git/hermes-fleet-backup/plugins/fleet-hooks-spec (default host path)
 *   3. <agent-runner-src>/fleet-hooks-spec (if NanoClaw bundles a copy in future)
 *
 * Resolution falls back gracefully: if no schemas are found, validator
 * becomes a one-time-warn no-op.
 */

import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

let _ajv: any = null;
let _ajvAttempted = false;
let _warnedUnavailable = false;
const _schemaCache = new Map<string, any>();
const _validatorCache = new Map<string, any>();

function tryLoadAjv(): any {
  if (_ajvAttempted) return _ajv;
  _ajvAttempted = true;
  try {
    // Schemas declare $schema: draft 2020-12 — must import the 2020 entrypoint.
    // The default `require('ajv')` only handles draft-07.
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const Ajv2020 = require('ajv/dist/2020').default || require('ajv/dist/2020');
    _ajv = new Ajv2020({ allErrors: true, strict: false });
  } catch {
    if (!_warnedUnavailable) {
      console.error(
        '[spec-validator] ajv (with 2020 draft) not available — schema validation disabled. ' +
          'Install with: npm install ajv',
      );
      _warnedUnavailable = true;
    }
    _ajv = null;
  }
  return _ajv;
}

function resolveSpecDir(): string | null {
  const envOverride = process.env.FLEET_HOOKS_SPEC_DIR;
  const candidates = [
    envOverride,
    path.join(os.homedir(), 'd/git/hermes-fleet-backup/plugins/fleet-hooks-spec'),
    path.join(__dirname, '..', '..', '..', 'fleet-hooks-spec'),
  ].filter((p): p is string => Boolean(p));
  for (const p of candidates) {
    if (fs.existsSync(path.join(p, 'schemas'))) return p;
  }
  return null;
}

function loadSchema(kind: string): any {
  if (_schemaCache.has(kind)) return _schemaCache.get(kind);
  const specDir = resolveSpecDir();
  if (!specDir) {
    _schemaCache.set(kind, null);
    return null;
  }
  const schemaPath = path.join(specDir, 'schemas', `${kind}.schema.json`);
  try {
    const raw = fs.readFileSync(schemaPath, 'utf-8');
    const schema = JSON.parse(raw);
    _schemaCache.set(kind, schema);
    return schema;
  } catch (err) {
    console.error(
      `[spec-validator] failed to load ${schemaPath}: ${err instanceof Error ? err.message : String(err)}`,
    );
    _schemaCache.set(kind, null);
    return null;
  }
}

function getValidator(kind: string): any {
  if (_validatorCache.has(kind)) return _validatorCache.get(kind);
  const ajv = tryLoadAjv();
  if (!ajv) return null;
  const schema = loadSchema(kind);
  if (!schema) return null;
  try {
    const v = ajv.compile(schema);
    _validatorCache.set(kind, v);
    return v;
  } catch (err) {
    console.error(
      `[spec-validator] failed to compile ${kind} schema: ${err instanceof Error ? err.message : String(err)}`,
    );
    _validatorCache.set(kind, null);
    return null;
  }
}

function isStrict(): boolean {
  const v = (process.env.FLEET_HOOKS_SPEC_STRICT || '').toLowerCase();
  return v === '1' || v === 'true' || v === 'yes';
}

export interface SpecValidationFailure extends Error {
  kind: string;
  errors: any[];
}

/**
 * Validate `record` against the schema for `kind`.
 *
 * Returns true if valid (or validator unavailable / schema missing — fail open).
 * Returns false on schema violation in warn-only mode.
 * Throws SpecValidationFailure in strict mode.
 *
 * `kind` is the schema basename, e.g. 'violation', 'correction-raw',
 * 'inbox-payload'.
 */
export function validateOrWarn(kind: string, record: unknown): boolean {
  const validator = getValidator(kind);
  if (!validator) return true;

  const ok = validator(record);
  if (ok) return true;

  const errors = validator.errors || [];
  const summary = errors
    .slice(0, 3)
    .map((e: any) => `${e.instancePath || '/'} ${e.message}`)
    .join('; ');
  const msg = `[spec-validator] ${kind} record FAILED schema: ${summary}`;

  if (isStrict()) {
    const err = new Error(msg) as SpecValidationFailure;
    err.kind = kind;
    err.errors = errors;
    throw err;
  }
  console.error(`${msg} (warn-only — set FLEET_HOOKS_SPEC_STRICT=1 to enforce)`);
  return false;
}

/** Test-only: drop caches so a sandbox can swap schemas. */
export function _resetCachesForTest(): void {
  _schemaCache.clear();
  _validatorCache.clear();
  _ajvAttempted = false;
  _ajv = null;
  _warnedUnavailable = false;
}
