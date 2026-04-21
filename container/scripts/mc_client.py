"""mc_client — Mission Control HTTP client for NanoClaw observability collectors.

Shared by session-usage-collector, agent-state-collector, violations-rollup,
and acp-metrics-flush. Python stdlib only. Never raises — collectors keep
running even when MC is down.

Ported from Hermes `~/.hermes/bin/mc_client.py`. NanoClaw-specific paths:
- Logs: ~/.nanoclaw/observability/logs/
- Sync state parent: ~/.nanoclaw/observability/state/
- Auth token: ~/.secrets/mc-auth-token (shared with Hermes)

Env overrides (checked in order):
- MC_CONVEX_URL
- CONVEX_URL
- WOLOLO_TENANT_ID (default: dev-tenant-000)
"""

from __future__ import annotations

import json
import logging
import logging.handlers
import os
import ssl
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Optional, Union

DEFAULT_CONVEX_URL = "https://upbeat-walrus-100.convex.site"
DEFAULT_TENANT_ID = "dev-tenant-000"
DEFAULT_TIMEOUT = 3.0
AUTH_TOKEN_PATH = Path.home() / ".secrets" / "mc-auth-token"
LOG_DIR = Path.home() / ".nanoclaw" / "observability" / "logs"
STATE_DIR = Path.home() / ".nanoclaw" / "observability" / "state"
TOKEN_CACHE_TTL_SECONDS = 60.0

_token_cache: dict[str, Any] = {"value": None, "loaded_at": 0.0}
_loggers: dict[str, logging.Logger] = {}


def setup_logger(name: str) -> logging.Logger:
    """Rotating-file logger. Silent on success, warnings/errors to disk."""
    if name in _loggers:
        return _loggers[name]

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(f"nanoclaw.{name}")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    if not logger.handlers:
        handler = logging.handlers.RotatingFileHandler(
            LOG_DIR / f"{name}.log",
            maxBytes=5 * 1024 * 1024,
            backupCount=3,
        )
        handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
        )
        logger.addHandler(handler)

    _loggers[name] = logger
    return logger


_log = setup_logger("mc_client")


def convex_url() -> str:
    return (
        os.environ.get("MC_CONVEX_URL")
        or os.environ.get("CONVEX_URL")
        or DEFAULT_CONVEX_URL
    ).rstrip("/")


def tenant_id() -> str:
    return os.environ.get("WOLOLO_TENANT_ID", DEFAULT_TENANT_ID)


def get_auth_token() -> Optional[str]:
    """Read ~/.secrets/mc-auth-token, caching for 60s."""
    now = time.time()
    if (
        _token_cache["value"] is not None
        and now - _token_cache["loaded_at"] < TOKEN_CACHE_TTL_SECONDS
    ):
        return _token_cache["value"]

    try:
        if AUTH_TOKEN_PATH.exists():
            token = AUTH_TOKEN_PATH.read_text().strip()
            if token:
                _token_cache["value"] = token
                _token_cache["loaded_at"] = now
                return token
    except Exception as exc:
        _log.warning("auth token read failed: %s", exc)

    _token_cache["value"] = None
    _token_cache["loaded_at"] = now
    return None


def state_path(name: str) -> Path:
    """Path to a per-collector sync-state file under ~/.nanoclaw/observability/state/."""
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    return STATE_DIR / name


def _build_request(
    endpoint: str,
    payload: Optional[dict] = None,
    method: str = "POST",
) -> urllib.request.Request:
    url = f"{convex_url()}{endpoint}"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "nanoclaw-observability/1.0",
    }
    token = get_auth_token()
    if token:
        headers["X-MC-Token"] = token

    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    return urllib.request.Request(url, data=data, headers=headers, method=method)


def post(endpoint: str, payload: dict, timeout: float = DEFAULT_TIMEOUT) -> bool:
    """Fire-and-forget POST. True on 2xx, False otherwise. Never raises."""
    if not get_auth_token():
        _log.warning("skip POST %s — no auth token", endpoint)
        return False

    try:
        req = _build_request(endpoint, payload, method="POST")
        ctx = ssl.create_default_context()
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
            status = getattr(resp, "status", resp.getcode())
            if 200 <= status < 300:
                return True
            _log.warning(
                "POST %s -> HTTP %s: %s",
                endpoint,
                status,
                resp.read()[:200].decode("utf-8", "replace"),
            )
            return False
    except urllib.error.HTTPError as exc:
        body = b""
        try:
            body = exc.read()
        except Exception:
            pass
        _log.warning(
            "POST %s -> HTTP %s: %s",
            endpoint,
            exc.code,
            body[:200].decode("utf-8", "replace"),
        )
        return False
    except urllib.error.URLError as exc:
        _log.warning("POST %s -> URLError: %s", endpoint, exc)
        return False
    except Exception as exc:
        _log.error("POST %s -> unexpected: %s", endpoint, exc)
        return False


def get(
    endpoint: str, timeout: float = DEFAULT_TIMEOUT
) -> Optional[Union[dict, list]]:
    """GET and parse JSON. None on any failure. Never raises."""
    if not get_auth_token():
        _log.warning("skip GET %s — no auth token", endpoint)
        return None

    try:
        req = _build_request(endpoint, payload=None, method="GET")
        ctx = ssl.create_default_context()
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
            status = getattr(resp, "status", resp.getcode())
            if 200 <= status < 300:
                raw = resp.read()
                if not raw:
                    return None
                return json.loads(raw.decode("utf-8"))
            _log.warning(
                "GET %s -> HTTP %s: %s",
                endpoint,
                status,
                resp.read()[:200].decode("utf-8", "replace"),
            )
            return None
    except urllib.error.HTTPError as exc:
        body = b""
        try:
            body = exc.read()
        except Exception:
            pass
        _log.warning(
            "GET %s -> HTTP %s: %s",
            endpoint,
            exc.code,
            body[:200].decode("utf-8", "replace"),
        )
        return None
    except urllib.error.URLError as exc:
        _log.warning("GET %s -> URLError: %s", endpoint, exc)
        return None
    except json.JSONDecodeError as exc:
        _log.warning("GET %s -> bad JSON: %s", endpoint, exc)
        return None
    except Exception as exc:
        _log.error("GET %s -> unexpected: %s", endpoint, exc)
        return None


def self_test() -> int:
    print(f"[mc_client] convex_url = {convex_url()}")
    print(f"[mc_client] tenant_id  = {tenant_id()}")

    token = get_auth_token()
    if not token:
        print(f"[mc_client] FAIL: no auth token at {AUTH_TOKEN_PATH}")
        return 1
    print(f"[mc_client] auth_token = {token[:6]}... (len={len(token)})")

    probe = get("/api/inbox/poll?agent=geordi&project=lambda&clan=lambda")
    if probe is None:
        print("[mc_client] FAIL: /api/inbox/poll unreachable (see logs)")
        return 2
    print(f"[mc_client] OK: inbox probe returned {type(probe).__name__}")
    return 0


if __name__ == "__main__":
    raise SystemExit(self_test())
