#!/usr/bin/env python3
"""pi-pool: let Claude Code delegate work to other models through pi.

Runs pi tasks across several subscription accounts. The account is chosen by
configured priority and ChatGPT/Codex subscription quota (/wham/usage); when a run
is refused for quota or an expired login, the next account continues the same
session. All accounts share one session directory, so no context is lost.

Tasks:
  pi-pool run (--prompt TEXT | --task-file FILE) [--account auto|NAME]
              [--size large|small] [--session ID] [--cwd DIR] [--max-switch N]
              [--refresh] [-- extra pi args, e.g. --tools read,grep --thinking high]
  pi-pool steer SESSION (TEXT | --file FILE) [--now] [--wait S]
                              message a running session; --now aborts the current step first
  pi-pool followup SESSION (TEXT | --file FILE) [--wait S]
                              deliver after pi finishes what it is doing
  pi-pool abort SESSION       stop a running session (while waiting for background
                              jobs: shut pi down, which cancels them)
  pi-pool show SESSION        last answer and accounts used (an ID prefix is enough)
  pi-pool ls [N]              the N most recent sessions (default 10)

Accounts:
  pi-pool status [--json] [--refresh]      quota, priority, tier, running sessions per account
  pi-pool pick [--size large|small] [--session ID]   the order accounts would be tried in
  pi-pool account list                     accounts in the configuration
  pi-pool account add NAME [--dir DIR] [--priority N] [--no-login]
                              create ~/.pi/agent-NAME (settings and extensions linked
                              from base_dir), log in by device code, save to the config
  pi-pool account login NAME               log in again (after the login expired)
  pi-pool account set NAME [--priority N] [--enable|--disable] [--dir DIR]
  pi-pool account remove NAME              remove from the config; directory and credentials stay
  pi-pool account sync [NAME...]           link entries added to base_dir, drop dangling links
  pi-pool account check [NAME...]          pi auth check (credentials are not refreshed)
  pi-pool config [--write]                 print the effective config; --write saves it
  pi-pool doctor                           check pi, account logins, and the skill install

pi runs as `pi --mode rpc`. steer/followup/abort write the message to
~/.pi/pool/inbox/<session>/; the running pi-pool picks it up every 0.5 s and forwards it:
  steer     after the current tool call, before the next model call (a running command
            is not interrupted)
  --now     abort the current step (including a running command), then send as a new prompt
  followup  after pi finishes what it is doing
A message that arrives just as a run ends is sent as a new prompt, not lost.

If pi stops while pi-async-bash still reports background bash jobs (RPC status
async-bash), pi-pool does not exit: it waits until the jobs finish, their results reach
the conversation, and the woken agent is done. steer/followup still work while waiting;
abort shuts pi down and cancels the jobs. There is no overall time limit.

Account order: lower priority first (default 50; legacy last_resort maps to 90); within
a priority, the session's last account first (prompt cache), then tier, running
sessions, and remaining quota. Tiers (threshold low_pct, default 5%):
  ok       every window has at least the threshold left; takes large tasks
  low      some window is below the threshold but requests are allowed; small tasks only
  blocked  the server refuses requests, a local cooldown is active, or the login expired
  unknown  quota could not be read; last-resort fallback

Config: ~/.config/pi-pool/config.json (or $PI_POOL_CONFIG), for example
  {"base_dir": "~/.pi/agent", "provider": "openai-codex", "low_pct": 5,
   "accounts": {"work": {"dir": "~/.pi/agent-work", "priority": 50},
                "main": {"dir": "~/.pi/agent", "priority": 90, "enabled": true}}}
Without a config file, ~/.pi/agent (if present) is the only account, "main".
Runtime data: ~/.pi/pool/ (or $PI_POOL_DIR): sessions/, logs/ (raw JSONL per run),
inbox/ (pending messages), quota.json (quota cache), state.json (sessions, cooldowns,
expired logins).

Exit codes: 0 success; 1 pi failed (steer etc.: not delivered; doctor: FAIL items);
2 bad arguments; 75 no usable account (all refused for quota or expired logins).
"""

from __future__ import annotations

import base64
import contextlib
import fcntl
import http.client
import json
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
import uuid
from collections import Counter
from collections.abc import Callable, Iterator
from typing import Any

HOME = os.path.expanduser("~")
CONFIG_PATH = os.environ.get("PI_POOL_CONFIG") or os.path.join(HOME, ".config", "pi-pool", "config.json")
POOL_DIR = os.environ.get("PI_POOL_DIR") or os.path.join(HOME, ".pi", "pool")
SESSION_DIR = os.path.join(POOL_DIR, "sessions")
LOG_DIR = os.path.join(POOL_DIR, "logs")
INBOX_DIR = os.path.join(POOL_DIR, "inbox")
QUOTA_CACHE = os.path.join(POOL_DIR, "quota.json")
STATE_PATH = os.path.join(POOL_DIR, "state.json")
STATE_LOCK = os.path.join(POOL_DIR, "state.lock")
# Caller session: the session ID each client sets in its shell. Recorded per run, to trace a child back to its parent.
CALLER_VARS = {"claude": "CLAUDE_CODE_SESSION_ID", "pi": "PI_SESSION_ID",
               "codex": "CODEX_THREAD_ID"}

DEFAULT_BASE_DIR = "~/.pi/agent"  # pi's default directory; new accounts link settings, extensions and skills from here
DEFAULT_PROVIDER = "openai-codex"
DEFAULT_PRIORITY = 50            # lower numbers are used first
LAST_RESORT_PRIORITY = 90        # priority given to accounts listed in the legacy last_resort
DEFAULT_LOW_PCT = 5
DEFAULT_QUOTA_TTL = 120          # seconds; quota cache lifetime
DEFAULT_AUTH_COOLDOWN = 7 * 24 * 3600  # seconds; an account with an expired login is skipped until re-login
DEFAULT_STALL_WARN = 600         # seconds without any event before one stderr notice
TOOL_ERROR_WARN = 5              # notice after this many consecutive failed tool calls
WATCH_TICK_S = 30                # watchdog check interval
BG_STATUS_KEY = "async-bash"     # status key pi-async-bash uses for background jobs in RPC mode
# Entries not linked into new account directories: credentials stay separate; sessions live in ~/.pi/pool/sessions
UNSHARED = {"auth.json"}
DEFAULT_COOLDOWN = 3600          # seconds; cooldown after a refusal with no known recovery time
QUOTA_TIMEOUT = 20               # seconds; timeout of one account's quota query
INBOX_POLL_S = 0.5               # how often a running pi-pool checks its inbox
ACK_WAIT_S = 20                  # default wait for a delivery acknowledgement (steer etc.)
CLOSE_GRACE_S = 15               # upper bound on waiting for the clear_queue response at shutdown
INBOX_KINDS = ("steer", "follow_up", "interrupt", "abort")
DIALOG_METHODS = {"select", "confirm", "input", "editor"}

# pi-pool manages sessions itself; these pi arguments may not be passed through
FORBIDDEN_PI_ARGS = {"--session", "--session-id", "--session-dir", "--no-session",
                     "--continue", "-c", "--resume", "-r", "--fork", "--mode",
                     "--print", "-p", "--export"}
AUTH_ERROR_RE = re.compile(
    r"(?<![0-9])401(?![0-9])|unauthori[sz]ed|invalid_grant|refresh.?token|token.{0,20}expired|"
    r"re-?authenticat|log ?in again|sign ?in again|not logged in|no credentials", re.IGNORECASE)
QUOTA_ERROR_RE = re.compile(
    r"usage.?limit|usage_not_included|rate.?limit|too many requests|"
    r"(?<![0-9])429(?![0-9])|quota|insufficient_quota", re.IGNORECASE)
TRY_AGAIN_RE = re.compile(r"try again in ~?(\d+)\s*min", re.IGNORECASE)

CONTINUE_PROMPT = ("(pi-pool) The previous turn on account {old} was interrupted by {why}; switched to "
                   "account {new}. Continue the task above; do not redo parts already finished.")
BG_LOST_NOTE = (" Background bash jobs from the previous process were killed with it and will not report "
                "back ({status}). Rerun those commands if you need their results.")


# --------------------------------------------------------------- configuration and state

def _number(user: dict[str, Any], key: str) -> Any:
    v = user[key]
    if not isinstance(v, (int, float)) or isinstance(v, bool):
        raise SystemExit(f"pi-pool: {key} must be a number")
    return v


def normalize_config(user: dict[str, Any]) -> dict[str, Any]:
    """Normalize the configuration file (or the legacy format) into one structure.

    Account values may be a directory string (legacy) or {"dir", "priority", "enabled"}.
    Accounts in the legacy last_resort get priority 90; all others default to 50.
    """
    if not isinstance(user, dict):
        raise SystemExit(f"pi-pool: {CONFIG_PATH} must be a JSON object")
    cfg: dict[str, Any] = {"low_pct": DEFAULT_LOW_PCT, "quota_ttl_s": DEFAULT_QUOTA_TTL,
                           "stall_warn_s": DEFAULT_STALL_WARN,
                           "auth_cooldown_s": DEFAULT_AUTH_COOLDOWN,
                           "base_dir": DEFAULT_BASE_DIR, "provider": DEFAULT_PROVIDER}
    for key in ("low_pct", "quota_ttl_s", "stall_warn_s", "auth_cooldown_s"):
        if key in user:
            cfg[key] = _number(user, key)
    for key in ("base_dir", "provider"):
        if key in user:
            if not isinstance(user[key], str) or not user[key]:
                raise SystemExit(f"pi-pool: {key} must be a non-empty string")
            cfg[key] = user[key]
    last = user.get("last_resort", [])
    if not isinstance(last, list) or not all(isinstance(x, str) for x in last):
        raise SystemExit("pi-pool: last_resort must be an array of account names")
    raw = user.get("accounts")
    if raw is None:
        raw = {}
    if not isinstance(raw, dict):
        raise SystemExit("pi-pool: accounts must be an object of {name: directory or {dir, priority, enabled}}")
    accounts: dict[str, dict[str, Any]] = {}
    for name, v in raw.items():
        if not re.fullmatch(r"[A-Za-z0-9_.-]+", name):
            raise SystemExit(f"pi-pool: account name {name!r} may only contain letters, digits, _ . -")
        if isinstance(v, str):
            v = {"dir": v}
        if not isinstance(v, dict) or not isinstance(v.get("dir"), str) or not v["dir"]:
            raise SystemExit(f"pi-pool: account {name} needs a dir (agent directory)")
        prio = v.get("priority", LAST_RESORT_PRIORITY if name in last else DEFAULT_PRIORITY)
        if not isinstance(prio, int) or isinstance(prio, bool):
            raise SystemExit(f"pi-pool: priority of account {name} must be an integer")
        enabled = v.get("enabled", True)
        if not isinstance(enabled, bool):
            raise SystemExit(f"pi-pool: enabled of account {name} must be true/false")
        accounts[name] = {"dir": v["dir"], "priority": prio, "enabled": enabled}
    cfg["accounts"] = accounts
    return cfg


def load_config() -> dict[str, Any]:
    try:
        with open(CONFIG_PATH, encoding="utf-8") as f:
            user = json.load(f)
    except FileNotFoundError:
        # No configuration: use pi's default directory, if present, as the only account "main"
        present = {"main": DEFAULT_BASE_DIR} if os.path.isdir(os.path.expanduser(DEFAULT_BASE_DIR)) else {}
        cfg = normalize_config({"accounts": present})
        cfg["from_file"] = False
        return cfg
    except (OSError, ValueError) as e:
        raise SystemExit(f"pi-pool: cannot read config {CONFIG_PATH}: {e}") from e
    cfg = normalize_config(user)
    cfg["from_file"] = True
    return cfg


def config_for_file(cfg: dict[str, Any]) -> dict[str, Any]:
    """Contents written back to the config file: user-tunable keys only, defaults included for easy editing."""
    keys = ("base_dir", "provider", "low_pct", "quota_ttl_s", "stall_warn_s", "auth_cooldown_s")
    out: dict[str, Any] = {k: cfg[k] for k in keys}
    out["accounts"] = {n: dict(a) for n, a in cfg["accounts"].items()}
    return out


def save_config(cfg: dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(CONFIG_PATH), exist_ok=True)
    write_json(CONFIG_PATH, config_for_file(cfg))
    cfg["from_file"] = True


def active_accounts(cfg: dict[str, Any]) -> list[str]:
    return [n for n, a in cfg["accounts"].items() if a["enabled"]]


def agent_dir(cfg: dict[str, Any], name: str) -> str:
    return os.path.expanduser(cfg["accounts"][name]["dir"])


def ensure_dirs() -> None:
    for d in (POOL_DIR, SESSION_DIR, LOG_DIR):
        os.makedirs(d, exist_ok=True)


def read_json(path: str, default: Any) -> Any:
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, ValueError):
        return default


def write_json(path: str, data: Any) -> None:
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path), prefix=".tmp-")
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=1)
    os.replace(tmp, path)


@contextlib.contextmanager
def locked_state() -> Iterator[dict[str, Any]]:
    """Read-modify-write state.json; concurrent pi-pool processes are serialized by a file lock."""
    ensure_dirs()
    with open(STATE_LOCK, "a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        state = read_json(STATE_PATH, {})
        state.setdefault("sessions", {})
        state.setdefault("cooldown", {})
        state.setdefault("auth_bad", {})
        yield state
        write_json(STATE_PATH, state)


def load_state() -> dict[str, Any]:
    state = read_json(STATE_PATH, {})
    state.setdefault("sessions", {})
    state.setdefault("cooldown", {})
    state.setdefault("auth_bad", {})
    return state


# --------------------------------------------------------------- quota

# Quota endpoint of ChatGPT/Codex subscriptions; the same data as the codex CLI status line. Read-only; never refreshes credentials.
WHAM_USAGE_URL = "https://chatgpt.com/backend-api/wham/usage"
JWT_AUTH_CLAIM = "https://api.openai.com/auth"
CODEX_CLIENT_VERSION = "0.144.0"   # codex version reported in the UA; the backend uses it to judge the client
AUTH_KEYS = ("openai-codex", "openai")


def as_dict(x: Any) -> dict[str, Any]:
    return x if isinstance(x, dict) else {}


def jwt_claims(token: Any) -> dict[str, Any]:
    """Decode only the JWT payload to get plan and account ID; no signature check (the server verifies it)."""
    if not isinstance(token, str):
        return {}
    try:
        seg = token.split(".")[1]
        claims = json.loads(base64.urlsafe_b64decode(seg + "=" * (-len(seg) % 4)))
    except (ValueError, IndexError):
        return {}
    return claims if isinstance(claims, dict) else {}


def read_codex_cred(path: str) -> tuple[dict[str, Any] | None, str]:
    """Get the ChatGPT OAuth credential from pi's auth.json; error messages never include credential contents."""
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        return None, "no auth.json (log in with pi-pool account login)"
    except (OSError, ValueError):
        return None, "auth.json is unreadable or malformed"
    if not isinstance(data, dict):
        return None, "auth.json is not a JSON object"
    for key in AUTH_KEYS:
        cred = data.get(key)
        if not isinstance(cred, dict):
            continue
        if key == "openai" and cred.get("type") != "oauth":
            continue   # an API key cannot query subscription quota
        if not isinstance(cred.get("access"), str) or not cred["access"]:
            return None, f"{key} in auth.json has no access token"
        return cred, ""
    return None, "no openai-codex OAuth credential in auth.json"


def window_label(seconds: Any) -> str | None:
    """Window label from the duration the server reports (5h, 7d, ...); the primary window is not assumed to be 5 hours."""
    if not isinstance(seconds, (int, float)) or isinstance(seconds, bool) or seconds <= 0:
        return None   # the plan has this window turned off
    mins = int(seconds // 60)
    if mins % 1440 == 0:
        return f"{mins // 1440}d"
    if mins % 60 == 0:
        return f"{mins // 60}h"
    return f"{mins}m"


def parse_usage(data: Any, now: float | None = None) -> dict[str, Any]:
    """Turn a /wham/usage response into a tiering record: allowed, remaining percent and reset time per window."""
    now = time.time() if now is None else now
    if not isinstance(data, dict):
        return {"ok": False, "allowed": None, "buckets": [], "issues": [{"text": "quota response is not an object"}]}
    rl = as_dict(data.get("rate_limit"))
    windows = []
    for key in ("primary_window", "secondary_window"):
        w = rl.get(key)
        if not isinstance(w, dict):
            continue
        label = window_label(w.get("limit_window_seconds"))
        if label is None:
            continue
        used = w.get("used_percent")
        used = used if isinstance(used, (int, float)) and not isinstance(used, bool) else 0
        reset = w.get("reset_at")
        if not isinstance(reset, (int, float)) or isinstance(reset, bool):
            after = w.get("reset_after_seconds")
            reset = now + after if isinstance(after, (int, float)) and not isinstance(after, bool) else None
        windows.append({"window": label, "used_pct": used,
                        "remaining_pct": max(0.0, min(100.0, 100.0 - used)),
                        "reset_at": int(reset) if reset else None})
    allowed = rl.get("allowed")
    if rl.get("limit_reached"):
        allowed = False
    return {"ok": bool(windows), "allowed": allowed if isinstance(allowed, bool) else None,
            "buckets": [{"name": "codex", "windows": windows}] if windows else [],
            "issues": [] if windows else [{"text": "quota response has no window data"}]}


def query_account_quota(auth_path: str) -> dict[str, Any]:
    cred, err = read_codex_cred(auth_path)
    if cred is None:
        return {"ok": False, "allowed": None, "plan": None, "buckets": [], "issues": [{"text": err}]}
    claims = jwt_claims(cred["access"])
    auth = as_dict(claims.get(JWT_AUTH_CLAIM))
    plan = auth.get("chatgpt_plan_type") if isinstance(auth.get("chatgpt_plan_type"), str) else None
    account_id = cred.get("accountId") or auth.get("chatgpt_account_id")
    u = os.uname()
    headers = {"Authorization": "Bearer " + cred["access"], "OpenAI-Beta": "responses=experimental",
               "originator": "codex_cli_rs", "Accept": "application/json",
               "User-Agent": f"codex_cli_rs/{CODEX_CLIENT_VERSION} ({u.sysname} {u.release}; {u.machine}) unknown"}
    if isinstance(account_id, str) and account_id:
        headers["chatgpt-account-id"] = account_id
    try:
        with urllib.request.urlopen(urllib.request.Request(WHAM_USAGE_URL, headers=headers),
                                    timeout=QUOTA_TIMEOUT) as r:
            data = json.loads(r.read())
    except urllib.error.HTTPError as e:
        hint = " (login may have expired: pi-pool account login)" if e.code in (401, 403) else ""
        rec = {"ok": False, "allowed": None, "buckets": [], "issues": [{"text": f"quota query HTTP {e.code}{hint}"}]}
    except (OSError, ValueError, http.client.HTTPException) as e:
        rec = {"ok": False, "allowed": None, "buckets": [],
               "issues": [{"text": f"quota query failed: {type(e).__name__}"}]}
    else:
        rec = parse_usage(data)
    rec["plan"] = plan
    return rec


def fetch_quota(cfg: dict[str, Any]) -> dict[str, Any]:
    """Query the quota of every enabled account in parallel."""
    names = active_accounts(cfg)
    if not names:
        return {"fetched_at": time.time(), "error": "no enabled accounts", "accounts": {}}
    out: dict[str, dict[str, Any]] = {}

    def one(n: str) -> None:
        out[n] = query_account_quota(os.path.join(agent_dir(cfg, n), "auth.json"))

    threads = [threading.Thread(target=one, args=(n,), daemon=True) for n in names]
    for th in threads:
        th.start()
    for th in threads:
        th.join(QUOTA_TIMEOUT + 5)
    return {"fetched_at": time.time(), "error": None, "accounts": out}


def get_quota(cfg: dict[str, Any], refresh: bool = False) -> dict[str, Any]:
    ensure_dirs()
    cache = read_json(QUOTA_CACHE, None)
    fresh = (isinstance(cache, dict) and not cache.get("error")
             and time.time() - cache.get("fetched_at", 0) < cfg["quota_ttl_s"]
             and set(cache.get("accounts", {})) >= set(active_accounts(cfg)))
    if fresh and not refresh:
        return cache
    quota = fetch_quota(cfg)
    write_json(QUOTA_CACHE, quota)
    return quota


def invalidate_quota() -> None:
    with contextlib.suppress(FileNotFoundError):
        os.unlink(QUOTA_CACHE)


def main_windows(rec: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not rec or not rec.get("buckets"):
        return []
    return rec["buckets"][0].get("windows") or []


def classify(name: str, rec: dict[str, Any] | None, state: dict[str, Any],
             low_pct: float) -> tuple[str, str]:
    """Return (tier, note)."""
    bad = state.get("auth_bad", {}).get(name)
    if bad and bad.get("until", 0) > time.time():
        return "blocked", f"login expired; run pi-pool account login {name} ({bad.get('reason', '')[:80]})"
    until = state["cooldown"].get(name, {}).get("until", 0)
    if until > time.time():
        return "blocked", f"local cooldown until {clock(until)} ({state['cooldown'][name].get('reason', '')})"
    if not rec or not rec.get("ok"):
        issues = (rec or {}).get("issues") or []
        return "unknown", issues[0]["text"] if issues else "quota not available"
    if rec.get("allowed") is False:
        return "blocked", "server: allowed=false"
    low = [w for w in main_windows(rec) if w["remaining_pct"] < low_pct]
    if low:
        return "low", ", ".join(f"{w['window']} {w['remaining_pct']:.0f}% left"
                               f" (resets {clock(w['reset_at'])})" for w in low)
    return "ok", ""


def headroom(rec: dict[str, Any] | None) -> float:
    return min((w["remaining_pct"] for w in main_windows(rec)), default=-1)


def running_counts(state: dict[str, Any]) -> Counter[str]:
    """Running sessions per account, used to spread parallel tasks across accounts."""
    out: Counter[str] = Counter()
    for rec in state["sessions"].values():
        run = rec.get("running")
        if run and running_here(run):
            out[str(run.get("account"))] += 1
    return out


def rank(cfg: dict[str, Any], quota: dict[str, Any], state: dict[str, Any], size: str,
         exclude: set[str], prefer: str | None) -> list[tuple[str, str, str]]:
    """Usable accounts in order of preference, as [(account, tier, note)].

    Sorted by:
    1. priority (an integer in the config, lower first);
    2. the account the session used last, first within its priority, to reuse its prompt cache;
    3. tier: large tasks prefer ok with unknown as fallback; small tasks use low first, saving ok accounts for large tasks;
    4. fewer running sessions first, spreading parallel tasks across accounts;
    5. more remaining quota first.
    Disabled accounts are excluded.
    """
    tiers = {"large": ("ok", "unknown"), "small": ("low", "ok", "unknown")}[size]
    busy = running_counts(state)
    rows = []
    for n in active_accounts(cfg):
        if n in exclude:
            continue
        rec = quota["accounts"].get(n)
        tier, why = classify(n, rec, state, cfg["low_pct"])
        if tier in tiers:
            rows.append((n, tier, why, rec))
    rows.sort(key=lambda r: (cfg["accounts"][r[0]]["priority"], r[0] != prefer,
                             tiers.index(r[1]), busy[r[0]], -headroom(r[3]), r[0]))
    return [(n, tier, why) for n, tier, why, _ in rows]


# --------------------------------------------------------------- display

def clock(ts: float | None) -> str:
    if not ts:
        return "?"
    return time.strftime("%m-%d %H:%M", time.localtime(ts))


def window_pct(rec: dict[str, Any] | None, key: str) -> str:
    for w in main_windows(rec):
        if w["window"] == key:
            return f"{w['remaining_pct']:.0f}%"
    return "-"


def cmd_status(cfg: dict[str, Any], args: list[str]) -> int:
    quota = get_quota(cfg, refresh="--refresh" in args)
    state = load_state()
    rows = []
    busy = running_counts(state)
    for n, acc in cfg["accounts"].items():
        rec = quota["accounts"].get(n)
        tier, why = classify(n, rec, state, cfg["low_pct"])
        if not acc["enabled"]:
            tier, why = "disabled", "disabled (pi-pool account set " + n + " --enable to enable)"
        rows.append({"account": n, "agent_dir": acc["dir"], "priority": acc["priority"],
                     "enabled": acc["enabled"], "running": busy[n],
                     "plan": (rec or {}).get("plan"), "allowed": (rec or {}).get("allowed"),
                     "remaining_5h": window_pct(rec, "5h"), "remaining_7d": window_pct(rec, "7d"),
                     "tier": tier, "note": why})
    if "--json" in args:
        print(json.dumps({"fetched_at": int(quota["fetched_at"]), "error": quota.get("error"),
                          "low_pct": cfg["low_pct"], "accounts": rows}, ensure_ascii=False))
        return 0
    age = int(time.time() - quota["fetched_at"])
    print(f"quota data {age}s old; low threshold {cfg['low_pct']}%"
          + (f"; {quota['error']}" if quota.get("error") else ""))
    if not cfg["from_file"]:
        print(f"(no config file {CONFIG_PATH}; using the default directory. "
              "pi-pool account add / set will create it)")
    print(f"{'account':<8}{'prio':>4} {'plan':<9}{'allowed':<8}{'5h':>6}{'7d':>6}{'run':>4}  "
          f"{'tier':<9}note")
    for r in rows:
        allowed = {True: "yes", False: "no", None: "?"}[r["allowed"]]
        print(f"{r['account']:<8}{r['priority']:>4} {(r['plan'] or '?'):<9}{allowed:<8}"
              f"{r['remaining_5h']:>6}{r['remaining_7d']:>6}{r['running']:>4}  "
              f"{r['tier']:<9}{r['note']}")
    return 0


# --------------------------------------------------------------- arguments

def take(args: list[str], name: str, default: str | None = None) -> str | None:
    for i, a in enumerate(args):
        if a == name:
            if i + 1 >= len(args):
                raise SystemExit(f"pi-pool: {name} needs a value")
            value = args[i + 1]
            del args[i:i + 2]
            return value
        if a.startswith(name + "="):
            del args[i]
            return a.split("=", 1)[1]
    return default


def take_flag(args: list[str], name: str) -> bool:
    if name in args:
        args.remove(name)
        return True
    return False


def session_files(prefix: str) -> dict[str, str]:
    """Find pi session files in the session directory by ID prefix; return {session ID: cwd}.

    Recovers sessions missing from state.json (for example a run killed before it was recorded).
    """
    out: dict[str, str] = {}
    try:
        names = os.listdir(SESSION_DIR)
    except FileNotFoundError:
        return out
    for name in names:
        if not name.endswith(".jsonl") or "_" not in name:
            continue
        if not name.rsplit("_", 1)[1].startswith(prefix):
            continue
        try:
            with open(os.path.join(SESSION_DIR, name), encoding="utf-8") as f:
                head = json.loads(f.readline())
        except (OSError, ValueError):
            continue
        if isinstance(head, dict) and head.get("type") == "session" and head.get("id"):
            out[str(head["id"])] = str(head.get("cwd") or "")
    return out


def resolve_session(state: dict[str, Any], sid: str) -> str:
    """Resolve a session ID or prefix to the full ID; fail rather than silently start a new session."""
    if sid in state["sessions"]:
        return sid
    hits = [s for s in state["sessions"] if s.startswith(sid)]
    if not hits:
        hits = list(session_files(sid))
    if len(hits) == 1:
        return hits[0]
    if len(hits) > 1:
        raise SystemExit(f"pi-pool: session prefix {sid} matches several sessions: {', '.join(hits[:5])}")
    raise SystemExit(f"pi-pool: no session {sid} (not in state.json or {SESSION_DIR}); "
                     "to start a new session, omit --session")


def check_size(size: str | None) -> str:
    size = size or "large"
    if size not in ("large", "small"):
        raise SystemExit("pi-pool: --size must be large or small")
    return size


def cmd_pick(cfg: dict[str, Any], args: list[str]) -> int:
    size = check_size(take(args, "--size"))
    state = load_state()
    sid = take(args, "--session")
    prefer = state["sessions"].get(resolve_session(state, sid), {}).get("last_account") if sid else None
    ranked = rank(cfg, get_quota(cfg, refresh=take_flag(args, "--refresh")), state, size,
                  set(), prefer)
    if not ranked:
        print(f"pi-pool: no account can take a {size} task", file=sys.stderr)
        return 75
    for n, tier, why in ranked:
        print(f"{n}\t{tier}\t{why}")
    return 0


# --------------------------------------------------------------- running

class RunResult:
    def __init__(self) -> None:
        self.final_text = ""
        self.stop_reason: str | None = None
        self.error: str | None = None
        self.saw_user = False
        self.turns = 0
        self.tools: Counter[str] = Counter()
        self.tool_errors = 0
        self.tool_error_streak = 0
        self.usage: Counter[str] = Counter()
        self.returncode: int | None = None
        self.stderr_tail: list[str] = []
        self.injected: Counter[str] = Counter()   # messages injected during the run, by kind
        self.bg_waited_s = 0.0                    # seconds spent waiting for background jobs after pi stopped
        self.bg_left: str | None = None           # background jobs still pending when pi exited (killed with it)


def assistant_text(msg: dict[str, Any]) -> str:
    content = msg.get("content")
    if isinstance(content, str):
        return content
    return "\n".join(b.get("text", "") for b in content or []
                     if isinstance(b, dict) and b.get("type") == "text").strip()


def progress_line(text: str) -> None:
    print(f"[pi-pool {time.strftime('%H:%M:%S')}] {text}", file=sys.stderr, flush=True)


def absorb(res: RunResult, ev: dict[str, Any], progress: bool) -> None:
    t = ev.get("type")
    if t == "message_end":
        msg = ev.get("message") or {}
        if msg.get("role") == "user":
            res.saw_user = True
        elif msg.get("role") == "assistant":
            res.turns += 1
            text = assistant_text(msg)
            if text:
                res.final_text = text
            res.stop_reason = msg.get("stopReason")
            res.error = msg.get("errorMessage") if res.stop_reason == "error" else None
            for k, v in (msg.get("usage") or {}).items():
                # only token counts are summed; cost is a nested object and skipped
                if isinstance(v, int) and not isinstance(v, bool):
                    res.usage[k] += v
            if progress:
                calls = [b.get("name") for b in msg.get("content") or []
                         if isinstance(b, dict) and b.get("type") == "toolCall"]
                progress_line(f"turn {res.turns} stop={res.stop_reason}"
                              + (f" calls {','.join(map(str, calls))}" if calls else "")
                              + (f" error: {res.error[:200]}" if res.error else ""))
    elif t == "tool_execution_end":
        res.tools[str(ev.get("toolName"))] += 1
        if ev.get("isError"):
            res.tool_errors += 1
            res.tool_error_streak += 1
            if progress and res.tool_error_streak % TOOL_ERROR_WARN == 0:
                progress_line(f"warning: {res.tool_error_streak} tool calls failed in a row"
                              f" (last: {ev.get('toolName')})")
        else:
            res.tool_error_streak = 0


ANSI_RE = re.compile(r"\x1b\[[0-9;?]*[ -/]*[@-~]|\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)")


def clean_status(text: Any) -> str | None:
    if not isinstance(text, str):
        return None
    text = " ".join(ANSI_RE.sub("", text).split())
    return text or None


# ---- inbox: steer/followup/abort write messages; the running pi-pool takes them and forwards them to pi

def inbox_dir(session: str) -> str:
    return os.path.join(INBOX_DIR, session)


def post_inbox(session: str, kind: str, message: str) -> str:
    """Write one message atomically; return its file name."""
    d = inbox_dir(session)
    os.makedirs(d, exist_ok=True)
    name = f"{time.time_ns()}-{kind}.json"
    fd, tmp = tempfile.mkstemp(dir=d, prefix=".tmp-")
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        json.dump({"kind": kind, "message": message}, f, ensure_ascii=False)
    os.replace(tmp, os.path.join(d, name))
    return name


def pending_inbox(session: str) -> list[str]:
    try:
        names = os.listdir(inbox_dir(session))
    except FileNotFoundError:
        return []
    return sorted(n for n in names if n.endswith(".json") and not n.startswith("."))


def claim_inbox(session: str, name: str) -> dict[str, Any] | None:
    """Take one message; None if it was withdrawn or already taken. rename ensures only one side gets it."""
    d = inbox_dir(session)
    claimed = os.path.join(d, ".claimed-" + name)
    try:
        os.rename(os.path.join(d, name), claimed)
    except FileNotFoundError:
        return None
    try:
        with open(claimed, encoding="utf-8") as f:
            item = json.load(f)
    except (OSError, ValueError):
        item = None
    with contextlib.suppress(FileNotFoundError):
        os.unlink(claimed)
    if not isinstance(item, dict) or item.get("kind") not in INBOX_KINDS:
        return None
    item["name"] = name
    return item


def ack_path(session: str, name: str) -> str:
    return os.path.join(inbox_dir(session), name.removesuffix(".json") + ".ack")


class PiRpc:
    """One `pi --mode rpc` run: send the task, forward inbox messages, and shut down once pi truly has nothing left.

    "Nothing left" means agent_settled has arrived and every background job pi-async-bash reports (setStatus
    key async-bash) has finished and delivered its result. While jobs remain, stdin stays open: when one
    finishes, the extension wakes the agent for a new run, which pi-pool handles as usual.

    The main thread reads pi's stdout and handles responses and events; one thread polls the inbox and one
    runs the watchdog. Writes to stdin and state changes share self.lock.
    """

    def __init__(self, proc: subprocess.Popen[bytes], session: str, res: RunResult,
                 progress: bool, stall_warn_s: float = DEFAULT_STALL_WARN) -> None:
        self.proc = proc
        self.session = session
        self.res = res
        self.progress = progress
        self.stall_warn_s = stall_warn_s
        self.lock = threading.RLock()
        self.active = False       # pi is in a run (started by us or woken by an extension)
        self.waiting_bg = False   # pi has stopped and we are waiting for background jobs
        self.wait_since = 0.0
        self.finishing = False    # shutdown decided; nothing more is forwarded
        self.abort_requested = False
        self.stdin_closed = False
        self.close_timer: threading.Timer | None = None
        self.run_prompt: str | None = None         # id of the prompt command that started the current run
        self.after_abort: list[dict[str, Any]] = []  # --now messages: sent as a new prompt after the abort
        self.callbacks: dict[str, Callable[[dict[str, Any]], None]] = {}
        self.seq = 0
        self.stop = threading.Event()
        self.bg_status: str | None = None          # background jobs currently reported by async-bash
        self.bg_at_close: str | None = None        # background jobs still pending when we decided to shut pi down
        self.last_event = time.time()
        self.last_warn = 0.0
        self.tools_running: dict[str, tuple[str, float]] = {}  # toolCallId -> (tool name, start time)

    # ---- writing to stdin

    def write(self, record: dict[str, Any]) -> None:
        with self.lock:
            if self.stdin_closed or self.proc.stdin is None:
                return
            try:
                self.proc.stdin.write(json.dumps(record, ensure_ascii=False).encode() + b"\n")
                self.proc.stdin.flush()
            except (BrokenPipeError, ValueError, OSError):
                self.stdin_closed = True

    def send(self, cmd: dict[str, Any],
             on_response: Callable[[dict[str, Any]], None] | None = None) -> str:
        with self.lock:
            self.seq += 1
            cid = f"pool-{self.seq}"
            if on_response is not None:
                self.callbacks[cid] = on_response
            self.write(dict(cmd, id=cid))
            return cid

    def close_stdin(self) -> None:
        with self.lock:
            self.cancel_close_timer()
            if self.stdin_closed or self.proc.stdin is None:
                return
            self.stdin_closed = True
            with contextlib.suppress(OSError):
                self.proc.stdin.close()

    def cancel_close_timer(self) -> None:
        with self.lock:
            if self.close_timer is not None:
                self.close_timer.cancel()
                self.close_timer = None

    def note(self, text: str) -> None:
        if self.progress:
            progress_line(text)

    # ---- runs

    def start_run(self, message: str) -> None:
        """Start a run while pi is idle. The caller holds self.lock."""
        self.cancel_close_timer()
        self.finishing = False
        self.end_bg_wait()
        self.active = True
        # pi ignores streamingBehavior when idle; if it is still wrapping up, this queues a follow-up instead of failing
        self.run_prompt = self.send({"type": "prompt", "message": message,
                                     "streamingBehavior": "followUp"}, self.on_prompt_response)

    def on_prompt_response(self, resp: dict[str, Any]) -> None:
        if resp.get("id") != self.run_prompt:
            return
        disposition = (resp.get("data") or {}).get("disposition")
        if resp.get("success") and disposition != "handled":
            return
        # no run was started, so no agent_settled will come; treat it as finished
        if not resp.get("success"):
            self.res.error = f"pi rejected the task: {resp.get('error')}"
        self.on_settled()

    def on_agent_start(self) -> None:
        """pi started a run: either our prompt or an extension waking it after a background job finished."""
        with self.lock:
            self.active = True
            if self.waiting_bg:
                self.note("background job result delivered; pi continues")
            self.end_bg_wait()
            if self.finishing:
                # pi started working again while we were shutting down (e.g. a background result just arrived): keep stdin open
                self.finishing = False
                self.cancel_close_timer()

    def end_bg_wait(self) -> None:
        if self.waiting_bg:
            self.res.bg_waited_s += time.time() - self.wait_since
        self.waiting_bg = False

    def failed(self) -> bool:
        return run_failure(self.res) is not None

    def on_settled(self) -> None:
        with self.lock:
            self.active = False
            if self.finishing:
                return
            if not self.failed():
                texts = [i["message"] for i in self.after_abort]
                self.after_abort = []
                for item in self.take_inbox():
                    if item["kind"] == "abort":
                        self.ack(item, delivered=True, note="pi has already stopped; nothing to abort")
                        continue
                    texts.append(item["message"])
                    self.res.injected["new_prompt"] += 1
                    self.ack(item, delivered=True, disposition="started",
                             note="run had ended; sent as a new prompt")
                if texts:
                    self.note(f"{len(texts)} message(s) pending when the run ended; sent as a new prompt")
                    self.start_run("\n\n".join(texts))
                    return
                if self.bg_status and not self.abort_requested:
                    if not self.waiting_bg:
                        self.waiting_bg = True
                        self.wait_since = time.time()
                        self.last_warn = time.time()
                        self.note(f"pi stopped; waiting for background jobs before finishing: {self.bg_status}")
                    return
            else:
                # quota or login failure: put --now messages back in the inbox for the run on the next account
                for item in self.after_abort:
                    post_inbox(self.session, "follow_up", item["message"])
                self.after_abort = []
            self.finish()

    def finish(self) -> None:
        """Recover messages still queued in pi, then close stdin. The caller holds self.lock."""
        self.end_bg_wait()
        self.finishing = True
        # on shutdown the extension cancels background jobs and clears the status, so record now which ones are killed
        self.bg_at_close = self.bg_status
        self.send({"type": "clear_queue"}, self.on_cleared)
        self.cancel_close_timer()
        self.close_timer = threading.Timer(CLOSE_GRACE_S, self.close_stdin)
        self.close_timer.daemon = True
        self.close_timer.start()

    def on_cleared(self, resp: dict[str, Any]) -> None:
        data = resp.get("data") or {}
        texts = [str(t) for t in (data.get("steering") or []) + (data.get("followUp") or [])]
        with self.lock:
            if self.active:
                # pi started a new run before clear_queue returned: queue the recovered messages behind it
                for t in texts:
                    self.send({"type": "follow_up", "message": t})
                return
            if not self.finishing:
                return
            if texts and not self.failed():
                self.note(f"{len(texts)} message(s) still queued in pi; sent as a new prompt")
                self.start_run("\n\n".join(texts))
                return
            for t in texts:
                post_inbox(self.session, "follow_up", t)
        self.close_stdin()

    def on_status(self, ev: dict[str, Any]) -> None:
        if ev.get("statusKey") != BG_STATUS_KEY:
            return
        text = clean_status(ev.get("statusText"))
        with self.lock:
            if text == self.bg_status:
                return
            had = self.bg_status
            self.bg_status = text
            if self.finishing:
                pass  # pi is shutting down: the status change comes from the extension cancelling jobs, not finishing them
            elif text:
                self.note(f"background jobs: {text}")
            elif had:
                self.note("all background jobs finished and their results reached the conversation")
            if not text and self.waiting_bg and not self.active and not self.finishing:
                # usually a background result wakes the agent first (on_agent_start); if not, finish directly
                self.finish()

    # ---- inbox

    def take_inbox(self) -> list[dict[str, Any]]:
        items = []
        for name in pending_inbox(self.session):
            item = claim_inbox(self.session, name)
            if item is not None:
                items.append(item)
        return items

    def ack(self, item: dict[str, Any], delivered: bool, disposition: str | None = None,
            note: str = "", error: str | None = None) -> None:
        with contextlib.suppress(OSError):
            write_json(ack_path(self.session, item["name"]),
                       {"delivered": delivered, "kind": item["kind"], "disposition": disposition,
                        "note": note, "error": error, "at": int(time.time())})

    def forward(self, item: dict[str, Any]) -> None:
        """Forward one message during a run. The caller holds self.lock."""
        kind, text = item["kind"], item.get("message", "")
        self.res.injected[kind] += 1
        if kind in ("steer", "follow_up"):
            def on_resp(resp: dict[str, Any], item: dict[str, Any] = item) -> None:
                ok = bool(resp.get("success"))
                self.ack(item, delivered=ok, disposition=(resp.get("data") or {}).get("disposition"),
                         error=None if ok else str(resp.get("error")))
            self.send({"type": kind, "message": text}, on_resp)
            self.note(f"forwarded {kind}: {' '.join(text.split())[:80]}")
        elif kind == "interrupt":
            self.after_abort.append(item)
            self.send({"type": "abort"})
            self.ack(item, delivered=True, disposition="aborting",
                     note="current step aborted; will be sent as a new prompt once pi stops")
            self.note(f"aborting the current step, then sending: {' '.join(text.split())[:80]}")
        else:
            self.abort_requested = True
            self.send({"type": "abort"})
            self.ack(item, delivered=True, disposition="aborting", note="abort sent")
            self.note("abort received; aborting the run")

    def handle_while_waiting(self, items: list[dict[str, Any]]) -> None:
        """Messages that arrive while pi is idle and waiting for background jobs. The caller holds self.lock."""
        texts = []
        for item in items:
            if item["kind"] == "abort":
                self.abort_requested = True
                self.res.stop_reason = "aborted"
                self.res.injected["abort"] += 1
                self.ack(item, delivered=True, disposition="aborting",
                         note="pi was waiting for background jobs; shutting pi down, which cancels them")
                self.note("abort received: no longer waiting for background jobs; shutting pi down")
                self.finish()
                return
            texts.append(item["message"])
            self.res.injected["new_prompt"] += 1
            self.ack(item, delivered=True, disposition="started",
                     note="pi was waiting for background jobs; sent as a new prompt")
        if texts:
            self.note(f"{len(texts)} message(s) arrived while waiting for background jobs; sent as a new prompt")
            self.start_run("\n\n".join(texts))

    def pump_inbox(self) -> None:
        while not self.stop.wait(INBOX_POLL_S):
            with self.lock:
                if self.finishing:
                    continue
                if self.waiting_bg and not self.active:
                    items = self.take_inbox()
                    if items:
                        self.handle_while_waiting(items)
                    continue
                if not self.active:
                    continue
                for item in self.take_inbox():
                    self.forward(item)

    # ---- watchdog: one notice after a long quiet period; silent during normal operation

    def watch(self) -> None:
        tick = min(WATCH_TICK_S, max(self.stall_warn_s / 4, 0.05))
        while not self.stop.wait(tick):
            if self.stall_warn_s <= 0:
                continue
            with self.lock:
                now = time.time()
                quiet = now - self.last_event
                if quiet < self.stall_warn_s or now - self.last_warn < self.stall_warn_s:
                    continue
                self.last_warn = now
                if self.waiting_bg:
                    self.note(f"still waiting for background jobs ({fmt_dur(now - self.wait_since)} so far): "
                              f"{self.bg_status}")
                elif self.tools_running:
                    name, since = min(self.tools_running.values(), key=lambda x: x[1])
                    self.note(f"no events for {fmt_dur(quiet)}; tool {name} has been running for {fmt_dur(now - since)}")
                else:
                    self.note(f"no events for {fmt_dur(quiet)}")

    # ---- main loop

    def run(self, message: str, log: Any) -> None:
        assert self.proc.stdout is not None
        with self.lock:
            self.start_run(message)
        threads = [threading.Thread(target=self.pump_inbox, daemon=True),
                   threading.Thread(target=self.watch, daemon=True)]
        for th in threads:
            th.start()
        try:
            # JSONL is split on LF only (pi docs: U+2028 and friends are not record boundaries)
            for raw in self.proc.stdout:
                log.write(raw.decode("utf-8", "replace"))
                log.flush()
                try:
                    ev = json.loads(raw)
                except ValueError:
                    continue
                if not isinstance(ev, dict):
                    continue
                self.last_event = time.time()
                t = ev.get("type")
                if t == "response":
                    cb = self.callbacks.pop(str(ev.get("id")), None)
                    if cb is not None:
                        cb(ev)
                elif t == "extension_ui_request":
                    method = ev.get("method")
                    if method in DIALOG_METHODS:
                        # nobody is watching: cancel every extension dialog, as if there were no UI
                        self.write({"type": "extension_ui_response", "id": ev.get("id"),
                                    "cancelled": True})
                    elif method == "setStatus":
                        self.on_status(ev)
                elif t == "agent_start":
                    self.on_agent_start()
                elif t == "agent_settled":
                    self.on_settled()
                else:
                    if t == "tool_execution_start":
                        self.tools_running[str(ev.get("toolCallId"))] = (
                            str(ev.get("toolName")), time.time())
                    elif t == "tool_execution_end":
                        self.tools_running.pop(str(ev.get("toolCallId")), None)
                    absorb(self.res, ev, self.progress)
        finally:
            self.stop.set()
            with self.lock:
                self.end_bg_wait()
                self.res.bg_left = self.bg_at_close if self.finishing else self.bg_status
            self.close_stdin()


def fmt_dur(seconds: float) -> str:
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m{seconds % 60:02d}s"
    return f"{seconds // 3600}h{seconds % 3600 // 60:02d}m"


def run_pi(cfg: dict[str, Any], account: str, session: str, cwd: str, pi_args: list[str],
           message: str, log_path: str, progress: bool = True) -> RunResult:
    pi = os.environ.get("PI_POOL_PI") or shutil.which("pi")
    if not pi:
        raise SystemExit("pi-pool: pi not found on PATH")
    env = dict(os.environ, PI_CODING_AGENT_DIR=agent_dir(cfg, account))
    # the child pi does not inherit the caller's session IDs; if it calls pi-pool, only its own PI_SESSION_ID remains
    for var in (*CALLER_VARS.values(), "PI_SESSION_FILE"):
        env.pop(var, None)
    cmd = [pi, "--mode", "rpc", "--session-dir", SESSION_DIR, "--session-id", session, *pi_args]
    res = RunResult()
    with open(log_path, "w", encoding="utf-8") as log, \
            open(log_path + ".stderr", "w", encoding="utf-8") as err_log:
        proc = subprocess.Popen(cmd, cwd=cwd, env=env, stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        assert proc.stdout is not None and proc.stderr is not None
        stderr_lines: list[str] = []

        def drain_stderr() -> None:
            assert proc.stderr is not None
            for raw in proc.stderr:
                line = raw.decode("utf-8", "replace")
                err_log.write(line)
                stderr_lines.append(line.rstrip("\n"))
                del stderr_lines[:-30]

        reader = threading.Thread(target=drain_stderr, daemon=True)
        reader.start()
        try:
            PiRpc(proc, session, res, progress, cfg.get("stall_warn_s", DEFAULT_STALL_WARN)).run(
                message, log)
            try:
                res.returncode = proc.wait(timeout=60)
            except subprocess.TimeoutExpired:
                proc.terminate()
                res.returncode = proc.wait(timeout=10)
        except BaseException:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
            raise
        reader.join(timeout=5)
        res.stderr_tail = stderr_lines
    return res


def run_failure(res: RunResult) -> tuple[str, str] | None:
    """Did this run fail because of the account itself: ("quota", error) for exhausted quota, ("auth", error) for an expired login.

    Both can continue on another account; other errors (model errors, tool failures) go back to the caller.
    """
    texts: list[str] = []
    if res.stop_reason == "error" and res.error:
        texts.append(res.error)
    if res.returncode not in (0, None) and res.stop_reason != "stop":
        texts.extend(reversed(res.stderr_tail))
    for text in texts:
        if QUOTA_ERROR_RE.search(text):
            return "quota", text
    for text in texts:
        if AUTH_ERROR_RE.search(text):
            return "auth", text
    return None


def quota_failure(res: RunResult) -> str | None:
    failure = run_failure(res)
    return failure[1] if failure and failure[0] == "quota" else None


def cooldown_until(text: str, rec: dict[str, Any] | None) -> float:
    m = TRY_AGAIN_RE.search(text)
    if m:
        return time.time() + int(m.group(1)) * 60 + 60
    resets = [w["reset_at"] for w in main_windows(rec)
              if w.get("reset_at") and w["remaining_pct"] < 1]
    if resets:
        return max(resets)
    return time.time() + DEFAULT_COOLDOWN


def read_message(prompt: str | None, task_file: str | None) -> str:
    if (prompt is None) == (task_file is None):
        raise SystemExit("pi-pool run: give exactly one of --prompt and --task-file")
    if task_file is not None:
        try:
            with open(task_file, encoding="utf-8") as f:
                prompt = f.read()
        except OSError as e:
            raise SystemExit(f"pi-pool: cannot read task file {task_file}: {e}") from e
    assert prompt is not None
    if not prompt.strip():
        raise SystemExit("pi-pool: the task is empty")
    return prompt


def cmd_run(cfg: dict[str, Any], args: list[str]) -> int:
    pi_args: list[str] = []
    if "--" in args:
        i = args.index("--")
        args, pi_args = args[:i], args[i + 1:]
    bad = [a for a in pi_args if a.split("=", 1)[0] in FORBIDDEN_PI_ARGS]
    if bad:
        raise SystemExit(f"pi-pool: these pi arguments are managed by pi-pool and cannot be passed through: {' '.join(bad)}")
    message = read_message(take(args, "--prompt"), take(args, "--task-file"))
    account_opt = take(args, "--account", "auto") or "auto"
    size = check_size(take(args, "--size"))
    sid_opt = take(args, "--session")
    cwd_opt = take(args, "--cwd")
    max_switch = int(take(args, "--max-switch", "3") or 3)
    refresh = take_flag(args, "--refresh")
    if args:
        raise SystemExit(f"pi-pool run: unknown arguments {' '.join(args)} (put pi arguments after --)")
    if account_opt != "auto" and account_opt not in cfg["accounts"]:
        raise SystemExit(f"pi-pool: no account {account_opt}; choose from {', '.join(cfg['accounts'])}")
    if not active_accounts(cfg):
        raise SystemExit("pi-pool: no enabled accounts; add one with pi-pool account add")

    ensure_dirs()
    state = load_state()
    session = resolve_session(state, sid_opt) if sid_opt else str(uuid.uuid4())
    srec = state["sessions"].get(session)
    # pi looks up sessions by working directory: a resumed session must return to its first directory
    first_cwd = (srec or {}).get("cwd") or (session_files(session).get(session) if sid_opt else None)
    cwd = os.path.realpath(cwd_opt or first_cwd or os.getcwd())
    if first_cwd and cwd != os.path.realpath(first_cwd):
        raise SystemExit(f"pi-pool: session {session} belongs to directory {first_cwd}, not {cwd}")
    if not os.path.isdir(cwd):
        raise SystemExit(f"pi-pool: working directory does not exist: {cwd}")
    if srec and running_here(srec.get("running")):
        raise SystemExit(f"pi-pool: session {session} is running (pid {srec['running']['pid']}); "
                         "use pi-pool steer / followup to message it")
    try:
        return run_loop(cfg, session, cwd, message, pi_args, account_opt, size, srec,
                        state, max_switch, refresh)
    finally:
        clear_running(session)


def run_loop(cfg: dict[str, Any], session: str, cwd: str, message: str, pi_args: list[str],
             account_opt: str, size: str, srec: dict[str, Any] | None, state: dict[str, Any],
             max_switch: int, refresh: bool) -> int:
    tried: list[dict[str, Any]] = []
    caller = caller_session()
    prefer = (srec or {}).get("last_account")
    quota = get_quota(cfg, refresh=refresh)
    current_message = message
    res = RunResult()
    account = None
    while True:
        if account_opt != "auto" and not tried:
            account = account_opt
            tier, why = classify(account, quota["accounts"].get(account), state, cfg["low_pct"])
            if tier == "blocked" or (size == "large" and tier == "low"):
                print(f"[pi-pool] note: requested account {account} is {tier} ({why}); using it anyway",
                      file=sys.stderr)
        else:
            ranked = rank(cfg, quota, load_state(), size, {t["account"] for t in tried}, prefer)
            if not ranked:
                print(f"[pi-pool] no account left that can take a {size} task; see pi-pool status",
                      file=sys.stderr)
                save_session(session, cwd, message, tried, None, res)
                return 75
            account = ranked[0][0]
        stamp = time.strftime("%Y%m%d-%H%M%S")
        log_path = os.path.join(LOG_DIR, f"{session}-{stamp}-{account}.jsonl")
        print(f"[pi-pool] session={session} account={account} size={size} cwd={cwd}\n"
              f"[pi-pool] log={log_path}", file=sys.stderr, flush=True)
        mark_running(session, cwd, message, account, log_path)
        started = time.time()
        res = run_pi(cfg, account, session, cwd, pi_args, current_message, log_path)
        failure = run_failure(res)
        tried.append({"account": account, "log": log_path, "started": int(started),
                      "elapsed_s": int(time.time() - started), "stop": res.stop_reason,
                      "returncode": res.returncode,
                      "quota_error": failure[1][:300] if failure and failure[0] == "quota" else None,
                      "auth_error": failure[1][:300] if failure and failure[0] == "auth" else None,
                      "bg_waited_s": round(res.bg_waited_s, 1),
                      "caller": caller})
        if not failure:
            with locked_state() as st:
                st["auth_bad"].pop(account, None)
            break
        kind, text = failure
        if kind == "quota":
            until = cooldown_until(text, quota["accounts"].get(account))
            with locked_state() as st:
                st["cooldown"][account] = {"until": until, "reason": text[:120]}
            print(f"[pi-pool] account {account} refused for quota ({text[:160]}); cooling down until {clock(until)}",
                  file=sys.stderr, flush=True)
        else:
            until = time.time() + cfg.get("auth_cooldown_s", DEFAULT_AUTH_COOLDOWN)
            with locked_state() as st:
                st["auth_bad"][account] = {"until": until, "reason": text[:120], "at": int(time.time())}
            print(f"[pi-pool] account {account} login expired ({text[:160]}); skipping it. "
                  f"Log in again: pi-pool account login {account}", file=sys.stderr, flush=True)
        invalidate_quota()
        if len(tried) > max_switch:
            print(f"[pi-pool] switched accounts {max_switch} times; stopping", file=sys.stderr)
            save_session(session, cwd, message, tried, account, res)
            return 75
        quota = get_quota(cfg)
        old = account
        nxt = rank(cfg, quota, load_state(), size, {t["account"] for t in tried}, None)
        new = nxt[0][0] if nxt else "?"
        # if this turn's task already reached the session, send only a continuation prompt; otherwise resend the task
        if res.saw_user:
            current_message = CONTINUE_PROMPT.format(
                old=old, new=new, why="a quota limit" if kind == "quota" else "an expired login")
            if res.bg_left:
                current_message += BG_LOST_NOTE.format(status=res.bg_left)
        else:
            current_message = message
        prefer = None

    save_session(session, cwd, message, tried, account, res)
    print_summary(session, tried, res)
    if res.returncode == 0 and res.stop_reason not in ("error", "aborted", None):
        return 0
    return 1


def caller_session() -> dict[str, str] | None:
    """The caller's session IDs, per client; None if there are none."""
    found = {client: os.environ[var] for client, var in CALLER_VARS.items()
             if os.environ.get(var)}
    return found or None


def new_record(cwd: str, message: str) -> dict[str, Any]:
    return {"cwd": cwd, "created": int(time.time()),
            "task": " ".join(message.split())[:120], "runs": []}


def mark_running(session: str, cwd: str, message: str, account: str, log_path: str) -> None:
    """Record the session and the running process before starting, so --session and steer find it even if killed."""
    with locked_state() as st:
        rec = st["sessions"].setdefault(session, new_record(cwd, message))
        rec["running"] = {"pid": os.getpid(), "host": socket.gethostname(), "account": account,
                          "since": int(time.time()), "log": log_path}
        rec["updated"] = int(time.time())
        rec["last_log"] = log_path


def clear_running(session: str) -> None:
    with locked_state() as st:
        rec = st["sessions"].get(session)
        if rec and (rec.get("running") or {}).get("pid") == os.getpid():
            rec.pop("running", None)


def running_here(run: dict[str, Any] | None) -> bool:
    """Is the recorded process still alive? A process on another host cannot be checked and counts as alive."""
    if not run:
        return False
    if run.get("host") != socket.gethostname():
        return True
    try:
        os.kill(int(run["pid"]), 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def save_session(session: str, cwd: str, message: str, tried: list[dict[str, Any]],
                 account: str | None, res: RunResult) -> None:
    with locked_state() as st:
        rec = st["sessions"].setdefault(session, new_record(cwd, message))
        rec.pop("running", None)
        rec["runs"].extend(tried)
        rec["runs"] = rec["runs"][-50:]
        if account and not (tried and (tried[-1].get("quota_error") or tried[-1].get("auth_error"))):
            rec["last_account"] = account
        rec["updated"] = int(time.time())
        rec["last_stop"] = res.stop_reason
        rec["last_log"] = tried[-1]["log"] if tried else rec.get("last_log")


def print_summary(session: str, tried: list[dict[str, Any]], res: RunResult) -> None:
    path = " → ".join(t["account"] + ("(quota)" if t.get("quota_error") else "")
                      + ("(login)" if t.get("auth_error") else "") for t in tried)
    tools = ",".join(f"{k}×{v}" for k, v in res.tools.most_common()) or "none"
    u = res.usage
    print(f"[pi-pool] session={session} accounts {path} exit={res.returncode} "
          f"stop={res.stop_reason} turns={res.turns} time={sum(t['elapsed_s'] for t in tried)}s")
    print(f"[pi-pool] tokens in={u['input']} out={u['output']} cacheRead={u['cacheRead']} "
          f"tools {tools}" + (f" ({res.tool_errors} failed)" if res.tool_errors else ""))
    if res.injected:
        print("[pi-pool] injected " + ",".join(f"{k}×{v}" for k, v in res.injected.items()))
    if res.bg_waited_s >= 1:
        print(f"[pi-pool] waited {fmt_dur(res.bg_waited_s)} for background jobs after pi stopped")
    if res.bg_left:
        print(f"[pi-pool] warning: background jobs were still pending when pi exited and were killed: {res.bg_left}")
    print(f"[pi-pool] log={tried[-1]['log'] if tried else '-'}")
    if res.error:
        print(f"[pi-pool] error: {res.error}")
    if res.returncode not in (0, None) and res.stderr_tail:
        print("[pi-pool] stderr tail:\n" + "\n".join(res.stderr_tail[-10:]))
    print("---")
    print(res.final_text or "(no text answer)")


# --------------------------------------------------------------- viewing

def cmd_show(cfg: dict[str, Any], args: list[str]) -> int:
    if not args:
        raise SystemExit("pi-pool show: a session ID is required")
    state = load_state()
    sid = resolve_session(state, args[0])
    rec = state["sessions"].get(sid)
    if not rec:
        print(f"pi-pool: no session {sid}", file=sys.stderr)
        return 1
    res = RunResult()
    log = rec.get("last_log")
    if log and os.path.exists(log):
        with open(log, encoding="utf-8") as f:
            for line in f:
                with contextlib.suppress(ValueError):
                    ev = json.loads(line)
                    if isinstance(ev, dict):
                        absorb(res, ev, progress=False)
    print(f"session {sid}  dir {rec['cwd']}  task: {rec.get('task', '')}")
    for r in rec.get("runs", []):
        print(f"  {clock(r['started'])} {r['account']:<5} {r['elapsed_s']:>5}s stop={r['stop']}"
              + (f"  quota: {r['quota_error'][:80]}" if r.get("quota_error") else "")
              + (f"  login expired: {r['auth_error'][:80]}" if r.get("auth_error") else ""))
    print("---")
    print(res.final_text or "(the last run gave no text answer)")
    return 0


def cmd_ls(cfg: dict[str, Any], args: list[str]) -> int:
    n = int(args[0]) if args else 10
    sessions = sorted(load_state()["sessions"].items(),
                      key=lambda kv: kv[1].get("updated", 0), reverse=True)[:n]
    for sid, rec in sessions:
        accounts = ">".join(dict.fromkeys(r["account"] for r in rec.get("runs", [])))
        stop = ("running(" + rec["running"]["account"] + ")" if running_here(rec.get("running"))
                else f"stop={rec.get('last_stop')}")
        print(f"{sid}  {clock(rec.get('updated'))}  {accounts:<12} "
              f"{stop}  {rec.get('task', '')[:60]}")
    return 0


# --------------------------------------------------------------- account management

def find_pi() -> str:
    pi = os.environ.get("PI_POOL_PI") or shutil.which("pi")
    if not pi:
        raise SystemExit("pi-pool: pi not found on PATH (npm install -g @earendil-works/pi-coding-agent)")
    return pi


def base_dir(cfg: dict[str, Any]) -> str:
    return os.path.realpath(os.path.expanduser(cfg["base_dir"]))


def link_shared(base: str, target: str) -> list[str]:
    """Symlink every entry of base except credentials into target.

    Existing links are left alone; a real file with the same name is first renamed to .bak; dangling links
    that point to files deleted from base are removed. Return the changes made.
    """
    if os.path.realpath(target) == os.path.realpath(base):
        return []
    os.makedirs(target, exist_ok=True)
    done = []
    base_real = os.path.realpath(base)
    for name in broken_links(target):
        dst = os.path.join(target, name)
        if os.path.dirname(os.path.abspath(os.path.join(target, os.readlink(dst)))) in (base, base_real):
            os.unlink(dst)
            done.append(f"{name}: dangling link removed (no longer in base)")
    for name in sorted(os.listdir(base)):
        if name in UNSHARED or name.endswith(".bak") or ".bak-" in name:
            continue
        dst = os.path.join(target, name)
        if os.path.islink(dst):
            continue
        if os.path.exists(dst):
            bak = dst + ".bak"
            if os.path.exists(bak) or os.path.islink(bak):
                bak = f"{dst}.bak-{time.strftime('%Y%m%d%H%M%S')}"
            os.rename(dst, bak)
            done.append(f"{name}: existing file renamed to {os.path.basename(bak)}")
        os.symlink(os.path.join(base, name), dst)
        done.append(f"{name}: linked")
    return done


def broken_links(d: str) -> list[str]:
    try:
        names = os.listdir(d)
    except OSError:
        return []
    return [n for n in names if os.path.islink(os.path.join(d, n))
            and not os.path.exists(os.path.join(d, n))]


def auth_check(cfg: dict[str, Any], d: str) -> str:
    """pi auth check: ready / not_ready / invalid, or why it failed. Credentials are not refreshed."""
    env = dict(os.environ, PI_CODING_AGENT_DIR=d)
    try:
        p = subprocess.run([find_pi(), "auth", "check", "--provider", cfg["provider"], "--json",
                            "--no-refresh"], capture_output=True, text=True, env=env, timeout=60,
                           check=False)
    except (OSError, subprocess.TimeoutExpired) as e:
        return f"error: {e}"
    try:
        return str(json.loads(p.stdout).get("status") or "unknown")
    except (ValueError, AttributeError):
        tail = (p.stderr or p.stdout).strip().splitlines()[-1:] or [f"exit {p.returncode}"]
        return f"error: {tail[0][:120]}"


def pi_login(cfg: dict[str, Any], d: str) -> bool:
    """Interactive login (device code, no browser launch); the user has to act in the terminal."""
    print(f"pi-pool: logging in to {cfg['provider']} in {d} (open the link in a browser and enter the code)",
          file=sys.stderr, flush=True)
    env = dict(os.environ, PI_CODING_AGENT_DIR=d)
    p = subprocess.run([find_pi(), "auth", "login", cfg["provider"], "--no-browser"], env=env,
                       check=False)
    status = auth_check(cfg, d)
    print(f"pi-pool: login exit={p.returncode}, auth check: {status}", file=sys.stderr)
    return status == "ready"


def clear_account_state(name: str) -> None:
    with locked_state() as st:
        st["auth_bad"].pop(name, None)
        st["cooldown"].pop(name, None)
    invalidate_quota()


def account_names(cfg: dict[str, Any], names: list[str]) -> list[str]:
    for n in names:
        if n not in cfg["accounts"]:
            raise SystemExit(f"pi-pool: no account {n}; choose from {', '.join(cfg['accounts'])}")
    return names or list(cfg["accounts"])


def take_int(args: list[str], name: str) -> int | None:
    v = take(args, name)
    if v is None:
        return None
    try:
        return int(v)
    except ValueError:
        raise SystemExit(f"pi-pool: {name} needs an integer") from None


def cmd_account(cfg: dict[str, Any], args: list[str]) -> int:
    sub = args.pop(0) if args else "list"
    if sub == "list":
        print(f"config {CONFIG_PATH}" + ("" if cfg["from_file"] else " (missing; showing defaults)")
              + f"; shared settings from {cfg['base_dir']}")
        print(f"{'account':<10}{'prio':>5} {'enabled':<8}{'auth.json':<10}{'broken':>7}  dir")
        for n, a in sorted(cfg["accounts"].items(), key=lambda kv: (kv[1]["priority"], kv[0])):
            d = os.path.expanduser(a["dir"])
            has_auth = "yes" if os.path.isfile(os.path.join(d, "auth.json")) else "no"
            print(f"{n:<10}{a['priority']:>5} {'yes' if a['enabled'] else 'no':<8}{has_auth:<10}"
                  f"{len(broken_links(d)):>4}  {a['dir']}")
        return 0
    if sub == "add":
        no_login = take_flag(args, "--no-login")
        d_opt = take(args, "--dir")
        prio = take_int(args, "--priority")
        if len(args) != 1:
            raise SystemExit("usage: pi-pool account add NAME [--dir DIR] [--priority N] [--no-login]")
        name = args[0]
        if name in cfg["accounts"]:
            raise SystemExit(f"pi-pool: account {name} already exists; to log in again use pi-pool account login {name}")
        d_raw = d_opt or f"~/.pi/agent-{name}"
        d = os.path.realpath(os.path.expanduser(d_raw))
        base = base_dir(cfg)
        if d != base:
            if not os.path.isdir(base):
                os.makedirs(base, exist_ok=True)
                print(f"pi-pool: shared settings directory {base} did not exist and was created; configure pi there, then run "
                      "pi-pool account sync", file=sys.stderr)
            for line in link_shared(base, d):
                print(f"  {line}")
        else:
            os.makedirs(d, exist_ok=True)
        if not no_login and not pi_login(cfg, d):
            print(f"pi-pool: login did not complete; the account was not added. Directory {d} is kept. "
                  f"Retry: pi-pool account add {name} --dir {d_raw}", file=sys.stderr)
            return 1
        tmp = normalize_config({"accounts": {name: {"dir": d_raw, "priority": prio
                                                     if prio is not None else DEFAULT_PRIORITY}}})
        cfg["accounts"][name] = tmp["accounts"][name]
        save_config(cfg)
        clear_account_state(name)
        print(f"pi-pool: added account {name} (priority {cfg['accounts'][name]['priority']}, {d_raw}); "
              f"config written to {CONFIG_PATH}")
        return 0
    if sub == "login":
        names = account_names(cfg, args)
        if len(names) != 1:
            raise SystemExit("usage: pi-pool account login NAME")
        ok = pi_login(cfg, agent_dir(cfg, names[0]))
        if ok:
            clear_account_state(names[0])
            if not cfg["from_file"]:
                save_config(cfg)
        return 0 if ok else 1
    if sub == "set":
        prio = take_int(args, "--priority")
        enable, disable = take_flag(args, "--enable"), take_flag(args, "--disable")
        d_opt = take(args, "--dir")
        if len(args) != 1 or (enable and disable) or (prio is None and not enable and not disable
                                                       and d_opt is None):
            raise SystemExit("usage: pi-pool account set NAME [--priority N] [--enable|--disable] [--dir DIR]")
        name = account_names(cfg, args)[0]
        acc = cfg["accounts"][name]
        if prio is not None:
            acc["priority"] = prio
        if enable or disable:
            acc["enabled"] = enable
        if d_opt is not None:
            acc["dir"] = d_opt
        save_config(cfg)
        print(f"pi-pool: {name} → priority {acc['priority']}, {'enabled' if acc['enabled'] else 'disabled'}, "
              f"{acc['dir']}")
        return 0
    if sub == "remove":
        if len(args) != 1:
            raise SystemExit("usage: pi-pool account remove NAME")
        name = account_names(cfg, args)[0]
        d = cfg["accounts"].pop(name)["dir"]
        save_config(cfg)
        clear_account_state(name)
        print(f"pi-pool: removed {name} from the config; directory {d} (with credentials) was not deleted")
        return 0
    if sub == "sync":
        base = base_dir(cfg)
        if not os.path.isdir(base):
            raise SystemExit(f"pi-pool: shared settings directory {base} does not exist")
        for n in account_names(cfg, args):
            changes = link_shared(base, agent_dir(cfg, n))
            print(f"{n}: " + ("no changes" if not changes else "; ".join(changes)))
        return 0
    if sub == "check":
        bad = 0
        for n in account_names(cfg, args):
            status = auth_check(cfg, agent_dir(cfg, n))
            bad += status != "ready"
            print(f"{n:<10}{status}")
        return 1 if bad else 0
    raise SystemExit(f"pi-pool account: unknown subcommand {sub} (list/add/login/set/remove/sync/check)")


def cmd_config(cfg: dict[str, Any], args: list[str]) -> int:
    if take_flag(args, "--write"):
        save_config(cfg)
        print(f"pi-pool: wrote {CONFIG_PATH}")
        return 0
    print(json.dumps(config_for_file(cfg), ensure_ascii=False, indent=1))
    return 0


def skill_link() -> str:
    return os.path.join(os.environ.get("PI_POOL_SKILLS_DIR") or os.path.join(HOME, ".claude", "skills"),
                        "pi-pool")


def cmd_doctor(cfg: dict[str, Any], args: list[str]) -> int:
    """Check whether this machine can run pi-pool; exit code 1 if anything FAILs."""
    fails = 0

    def line(level: str, text: str) -> None:
        nonlocal fails
        fails += level == "FAIL"
        print(f"[{level}] {text}")

    here = os.path.dirname(os.path.realpath(__file__))
    line("ok" if sys.version_info >= (3, 9) else "FAIL", f"Python {sys.version.split()[0]}")
    pi = os.environ.get("PI_POOL_PI") or shutil.which("pi")
    if pi:
        try:
            v = subprocess.run([pi, "--version"], capture_output=True, text=True, timeout=30,
                               check=False).stdout.strip()
        except (OSError, subprocess.TimeoutExpired) as e:
            v = f"cannot run: {e}"
        line("ok", f"pi {v} ({pi})")
    else:
        line("FAIL", "pi is not on PATH: npm install -g @earendil-works/pi-coding-agent")
    which = shutil.which("pi-pool")
    if which and os.path.realpath(which) == os.path.realpath(__file__):
        line("ok", f"command pi-pool -> {os.path.realpath(which)}")
    else:
        line("warn", f"command pi-pool {'points to ' + os.path.realpath(which) if which else 'is not on PATH'}; "
             f"run {os.path.join(here, 'install.sh')}")
    sk = skill_link()
    if os.path.realpath(sk) == here:
        line("ok", f"Claude skill {sk} → {here}")
    else:
        line("warn", f"Claude skill {sk} does not point to {here}; run install.sh")
    base = base_dir(cfg)
    settings = os.path.join(base, "settings.json")
    try:
        with open(settings, encoding="utf-8") as f:
            has_bg = "async-bash" in f.read()
    except OSError:
        has_bg = False
    line("ok" if has_bg else "warn",
         "pi-async-bash is in the shared settings" if has_bg else
         f"pi-async-bash is not in {settings}: long commands can only run in the foreground (pi install npm:@chthub/pi-async-bash)")
    line("ok" if cfg["from_file"] else "warn",
         f"config {CONFIG_PATH}" + ("" if cfg["from_file"] else " is missing; using the default directory. "
                                 "Create it with pi-pool config --write"))
    enabled = active_accounts(cfg)
    if not enabled:
        line("FAIL", "no enabled accounts: pi-pool account add NAME")
    for n, a in sorted(cfg["accounts"].items(), key=lambda kv: (kv[1]["priority"], kv[0])):
        d = os.path.expanduser(a["dir"])
        tag = f"account {n} (priority {a['priority']}{'' if a['enabled'] else ', disabled'})"
        if not os.path.isdir(d):
            line("FAIL" if a["enabled"] else "warn", f"{tag}: directory {d} does not exist")
            continue
        broken = broken_links(d)
        status = auth_check(cfg, d) if pi else "not checked"
        level = "ok" if status == "ready" else ("FAIL" if a["enabled"] else "warn")
        if level == "ok" and broken:
            level = "warn"
        line(level, f"{tag}: auth {status}"
             + (f", dangling links {', '.join(broken)} (clean up with pi-pool account sync {n})" if broken else "")
             + ("" if status == "ready" else f"; log in again: pi-pool account login {n}"))
    try:
        ensure_dirs()
        probe = os.path.join(POOL_DIR, ".doctor")
        with open(probe, "w", encoding="utf-8") as f:
            f.write("ok")
        os.unlink(probe)
        line("ok", f"runtime data directory {POOL_DIR} is writable")
    except OSError as e:
        line("FAIL", f"runtime data directory {POOL_DIR} is not writable: {e}")
    return 1 if fails else 0


# --------------------------------------------------------------- messages to running sessions

def cmd_send(cfg: dict[str, Any], args: list[str], kind: str) -> int:
    """steer / followup / abort: write to the inbox and wait for the running pi-pool to acknowledge."""
    now = take_flag(args, "--now")
    task_file = take(args, "--file")
    wait = float(take(args, "--wait", str(ACK_WAIT_S)) or ACK_WAIT_S)
    if not args:
        raise SystemExit(f"pi-pool {kind}: a session ID is required")
    sid_arg = args.pop(0)
    if kind == "abort":
        if args or task_file or now:
            raise SystemExit("pi-pool abort: takes only a session ID")
        message = ""
    else:
        message = read_message(" ".join(args) if args else None, task_file)
    if now:
        if kind != "steer":
            raise SystemExit("pi-pool: --now only works with steer")
        kind = "interrupt"
    state = load_state()
    sid = resolve_session(state, sid_arg)
    rec = state["sessions"].get(sid) or {}
    if not running_here(rec.get("running")):
        print(f"pi-pool: session {sid} is not running; to continue it use "
              f"pi-pool run --session {sid[:8]} --prompt ...", file=sys.stderr)
        return 1
    name = post_inbox(sid, kind, message)
    pending = os.path.join(inbox_dir(sid), name)
    ack = ack_path(sid, name)
    def report_ack() -> int | None:
        if not os.path.exists(ack):
            return None
        data = read_json(ack, {})
        with contextlib.suppress(FileNotFoundError):
            os.unlink(ack)
        ok = bool(data.get("delivered"))
        detail = ", ".join(str(x) for x in (data.get("disposition"), data.get("note"),
                                            data.get("error")) if x)
        print(f"pi-pool: {kind} {'delivered' if ok else 'not delivered'} ({detail})")
        return 0 if ok else 1

    deadline = time.time() + wait
    while time.time() < deadline:
        code = report_ack()
        if code is not None:
            return code
        if not os.path.exists(pending) and not running_here(
                load_state()["sessions"].get(sid, {}).get("running")):
            break
        time.sleep(0.3)
    code = report_ack()
    if code is not None:
        return code
    try:
        os.unlink(pending)
    except FileNotFoundError:
        print(f"pi-pool: {kind} was picked up, but no acknowledgement arrived within {wait:.0f}s; "
              "check the run's stderr or log", file=sys.stderr)
        return 1
    print(f"pi-pool: nobody picked the message up within {wait:.0f}s; withdrawn. The session may have ended "
          "or the pi-pool running it is stuck", file=sys.stderr)
    return 1


def main(argv: list[str]) -> int:
    if not argv or argv[0] in ("-h", "--help", "help"):
        print(__doc__)
        return 0
    cmd, args = argv[0], argv[1:]
    commands: dict[str, Callable[[dict[str, Any], list[str]], int]] = {
        "status": cmd_status, "pick": cmd_pick, "run": cmd_run, "show": cmd_show, "ls": cmd_ls,
        "account": cmd_account, "config": cmd_config, "doctor": cmd_doctor,
        "steer": lambda c, a: cmd_send(c, a, "steer"),
        "followup": lambda c, a: cmd_send(c, a, "follow_up"),
        "abort": lambda c, a: cmd_send(c, a, "abort")}
    if cmd not in commands:
        print(f"pi-pool: unknown command {cmd}; see pi-pool --help", file=sys.stderr)
        return 2
    # when killed, take the child pi process down too
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))
    try:
        return commands[cmd](load_config(), args)
    except SystemExit as e:
        if isinstance(e.code, str):
            print(e.code, file=sys.stderr)
            return 2
        raise
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
