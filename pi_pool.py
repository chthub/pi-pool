#!/usr/bin/env python3
"""pi-pool：在多个 pi 账号之间派发 pi 任务，账号由程序自己选、自己换。

按 ChatGPT/Codex 订阅额度（/wham/usage）和配置里的优先级选账号；一轮被额度拒绝或登录失效时，换下一个
账号续跑同一个会话。所有账号共用一个会话目录，所以换账号后对话上下文不丢。

任务：
  pi-pool run (--prompt TEXT | --task-file FILE) [--account auto|NAME]
              [--size large|small] [--session ID] [--cwd DIR] [--max-switch N]
              [--refresh] [-- 其他 pi 参数，如 --tools read,grep --thinking high]
  pi-pool steer SESSION (TEXT | --file FILE) [--now] [--wait S]
                              给正在运行的会话插话；--now 先中止当前操作再发
  pi-pool followup SESSION (TEXT | --file FILE) [--wait S]
                              等它把手上的事做完再发
  pi-pool abort SESSION       中止正在运行的会话（在等后台任务时：关闭 pi，后台任务随之终止）
  pi-pool show SESSION        再看一次会话最后的回答和用过的账号（ID 可写前缀）
  pi-pool ls [N]              最近 N 个会话（默认 10）

账号：
  pi-pool status [--json] [--refresh]      各账号额度、优先级、分级、在跑的会话数
  pi-pool pick [--size large|small] [--session ID]   现在会按什么顺序选号
  pi-pool account list                     配置里的账号
  pi-pool account add NAME [--dir DIR] [--priority N] [--no-login]
                              建 ~/.pi/agent-NAME（设置、扩展软链到 base_dir），
                              device code 登录，写进配置
  pi-pool account login NAME               重新登录（登录失效后）
  pi-pool account set NAME [--priority N] [--enable|--disable] [--dir DIR]
  pi-pool account remove NAME              只从配置移除，目录和凭证保留
  pi-pool account sync [NAME...]           base_dir 里新增的设置/扩展补软链
  pi-pool account check [NAME...]          pi auth check（不刷新凭证）
  pi-pool config [--write]                 打印生效的配置；--write 写成配置文件
  pi-pool doctor                           检查 pi、额度查询、账号登录、skill 安装

pi 以 `pi --mode rpc` 常驻运行。steer/followup/abort 把消息写进
~/.pi/pool/inbox/<会话>/，正在运行的 pi-pool 每 0.5 秒取一次并转给 pi：
  steer     当前这一步的工具调用执行完后、下一次调用模型前送达（不打断正在跑的命令）
  --now     先中止当前操作（包括正在跑的命令），再把消息作为新指令发出
  followup  等 pi 把手上的事做完再送达
运行刚结束时到达的消息会作为新一轮指令发出，不会丢。

pi 停下时如果 pi-async-bash 报告还有后台 bash 任务（RPC 状态 async-bash），pi-pool
不退出，等任务完成、结果送进对话、agent 被唤醒接着做完。等待期间照常收 steer/followup，
abort 会关闭 pi 并终止后台任务。没有总时长上限。

选号顺序：priority 小的先用（默认 50，旧配置 last_resort 换算成 90）；同优先级里
会话上次用的账号优先（复用提示缓存），再按分级、在跑会话数、剩余额度。
分级规则（阈值默认 5%，可在配置里改 low_pct）：
  ok       主额度各窗口剩余都 >= 阈值，可接大任务
  low      任一窗口剩余 < 阈值但服务端仍允许请求，只接小任务
  blocked  服务端 allowed=false，本机刚被额度拒绝在冷却，或登录失效
  unknown  额度没查到；排在最后，作为兜底

配置：~/.config/pi-pool/config.json（环境变量 PI_POOL_CONFIG 可改路径），例如
  {"base_dir": "~/.pi/agent", "provider": "openai-codex", "low_pct": 5,
   "accounts": {"work": {"dir": "~/.pi/agent-work", "priority": 50},
                "main": {"dir": "~/.pi/agent", "priority": 90, "enabled": true}}}
没有配置文件时，~/.pi/agent 存在就把它当作唯一账号 main。
运行数据：~/.pi/pool/（PI_POOL_DIR 可改；sessions/ 会话，logs/ 每次运行的 JSONL 事件流，
inbox/ 待转发的插话，quota.json 额度缓存，state.json 会话、冷却与登录失效记录）。

退出码：0 成功；1 pi 失败（steer 等：没送达；doctor：有 FAIL）；2 参数错误；
75 没有可用账号（都被额度拒绝或登录失效）。
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
# 调用方会话：各客户端给自己的 shell 设的会话 ID。记进每次运行，事后可据此关联父会话。
CALLER_VARS = {"claude": "CLAUDE_CODE_SESSION_ID", "pi": "PI_SESSION_ID",
               "codex": "CODEX_THREAD_ID"}

DEFAULT_BASE_DIR = "~/.pi/agent"  # pi 的默认目录；新账号的设置、扩展、skill 都软链到这里
DEFAULT_PROVIDER = "openai-codex"
DEFAULT_PRIORITY = 50            # 数字小的先用
LAST_RESORT_PRIORITY = 90        # 旧配置 last_resort 里的账号换算成的优先级
DEFAULT_LOW_PCT = 5
DEFAULT_QUOTA_TTL = 120          # 秒；额度缓存的有效期
DEFAULT_AUTH_COOLDOWN = 7 * 24 * 3600  # 秒；登录失效的账号重新登录前不再用
DEFAULT_STALL_WARN = 600         # 秒；这么久没有任何事件就在 stderr 提示一次
TOOL_ERROR_WARN = 5              # 连续这么多次工具调用出错就提示
WATCH_TICK_S = 30                # 看门狗多久检查一次
BG_STATUS_KEY = "async-bash"     # pi-async-bash 在 RPC 模式下报告后台任务用的状态键
# 新账号目录里不软链的项：凭证各自独立，会话由 pi-pool 统一放在 ~/.pi/pool/sessions
UNSHARED = {"auth.json"}
DEFAULT_COOLDOWN = 3600          # 秒；被拒绝但不知道何时恢复时的冷却
QUOTA_TIMEOUT = 20               # 秒；单个账号额度查询的超时
INBOX_POLL_S = 0.5               # 运行中的 pi-pool 多久看一次收件箱
ACK_WAIT_S = 20                  # steer 等命令默认等多久送达确认
CLOSE_GRACE_S = 15               # 收尾时等 clear_queue 回应的上限
INBOX_KINDS = ("steer", "follow_up", "interrupt", "abort")
DIALOG_METHODS = {"select", "confirm", "input", "editor"}

# 会话由 pi-pool 管理，这些 pi 参数不允许透传
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

CONTINUE_PROMPT = ("（pi-pool）上一轮在账号 {old} 上因{why}中断，现已切换到账号 {new}。"
                   "继续完成上面的任务，已经完成的部分不要重做。")
BG_LOST_NOTE = ("上一个进程里的后台 bash 任务已随进程终止，不会再有结果送回（{status}）。"
                "需要这些结果的话重新运行对应命令。")


# --------------------------------------------------------------- 配置与状态

def _number(user: dict[str, Any], key: str) -> Any:
    v = user[key]
    if not isinstance(v, (int, float)) or isinstance(v, bool):
        raise SystemExit(f"pi-pool: {key} 必须是数字")
    return v


def normalize_config(user: dict[str, Any]) -> dict[str, Any]:
    """把配置文件（或旧格式）整理成统一结构。

    accounts 的值可以是目录字符串（旧格式），也可以是 {"dir", "priority", "enabled"}。
    旧格式的 last_resort 换算成优先级 90，其余账号默认 50。
    """
    if not isinstance(user, dict):
        raise SystemExit(f"pi-pool: {CONFIG_PATH} 必须是 JSON 对象")
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
                raise SystemExit(f"pi-pool: {key} 必须是非空字符串")
            cfg[key] = user[key]
    last = user.get("last_resort", [])
    if not isinstance(last, list) or not all(isinstance(x, str) for x in last):
        raise SystemExit("pi-pool: last_resort 必须是账号名数组")
    raw = user.get("accounts")
    if raw is None:
        raw = {}
    if not isinstance(raw, dict):
        raise SystemExit("pi-pool: accounts 必须是 {名字: 目录或 {dir, priority, enabled}} 对象")
    accounts: dict[str, dict[str, Any]] = {}
    for name, v in raw.items():
        if not re.fullmatch(r"[A-Za-z0-9_.-]+", name):
            raise SystemExit(f"pi-pool: 账号名 {name!r} 只能用字母、数字、_ . -")
        if isinstance(v, str):
            v = {"dir": v}
        if not isinstance(v, dict) or not isinstance(v.get("dir"), str) or not v["dir"]:
            raise SystemExit(f"pi-pool: 账号 {name} 需要 dir（agent 目录）")
        prio = v.get("priority", LAST_RESORT_PRIORITY if name in last else DEFAULT_PRIORITY)
        if not isinstance(prio, int) or isinstance(prio, bool):
            raise SystemExit(f"pi-pool: 账号 {name} 的 priority 必须是整数")
        enabled = v.get("enabled", True)
        if not isinstance(enabled, bool):
            raise SystemExit(f"pi-pool: 账号 {name} 的 enabled 必须是 true/false")
        accounts[name] = {"dir": v["dir"], "priority": prio, "enabled": enabled}
    cfg["accounts"] = accounts
    return cfg


def load_config() -> dict[str, Any]:
    try:
        with open(CONFIG_PATH, encoding="utf-8") as f:
            user = json.load(f)
    except FileNotFoundError:
        # 没有配置：pi 的默认目录存在就把它当作唯一账号 main
        present = {"main": DEFAULT_BASE_DIR} if os.path.isdir(os.path.expanduser(DEFAULT_BASE_DIR)) else {}
        cfg = normalize_config({"accounts": present})
        cfg["from_file"] = False
        return cfg
    except (OSError, ValueError) as e:
        raise SystemExit(f"pi-pool: 读不了配置 {CONFIG_PATH}: {e}") from e
    cfg = normalize_config(user)
    cfg["from_file"] = True
    return cfg


def config_for_file(cfg: dict[str, Any]) -> dict[str, Any]:
    """写回配置文件的内容：只留用户可调的键，默认值也写出来，方便手改。"""
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
    """读改写 state.json；并行的 pi-pool 进程靠文件锁串行。"""
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


# --------------------------------------------------------------- 额度

# ChatGPT/Codex 订阅的额度接口，和 codex CLI 状态栏是同一份数据。只读，不刷新凭证。
WHAM_USAGE_URL = "https://chatgpt.com/backend-api/wham/usage"
JWT_AUTH_CLAIM = "https://api.openai.com/auth"
CODEX_CLIENT_VERSION = "0.144.0"   # UA 里报的 codex 版本，后端按它判断客户端新旧
AUTH_KEYS = ("openai-codex", "openai")


def as_dict(x: Any) -> dict[str, Any]:
    return x if isinstance(x, dict) else {}


def jwt_claims(token: Any) -> dict[str, Any]:
    """只解 JWT 的 payload 段拿套餐和账号 ID，不验签（签名归服务端验）。"""
    if not isinstance(token, str):
        return {}
    try:
        seg = token.split(".")[1]
        claims = json.loads(base64.urlsafe_b64decode(seg + "=" * (-len(seg) % 4)))
    except (ValueError, IndexError):
        return {}
    return claims if isinstance(claims, dict) else {}


def read_codex_cred(path: str) -> tuple[dict[str, Any] | None, str]:
    """从 pi 的 auth.json 取 ChatGPT OAuth 凭据；错误信息不含凭据内容。"""
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        return None, "没有 auth.json（pi-pool account login 登录）"
    except (OSError, ValueError):
        return None, "auth.json 读不了或格式损坏"
    if not isinstance(data, dict):
        return None, "auth.json 不是 JSON 对象"
    for key in AUTH_KEYS:
        cred = data.get(key)
        if not isinstance(cred, dict):
            continue
        if key == "openai" and cred.get("type") != "oauth":
            continue   # API key 不能查订阅额度
        if not isinstance(cred.get("access"), str) or not cred["access"]:
            return None, f"auth.json 里的 {key} 没有 access token"
        return cred, ""
    return None, "auth.json 里没有 openai-codex OAuth 凭据"


def window_label(seconds: Any) -> str | None:
    """窗口标签按服务端报的时长生成（5h、7d……），不假定主窗口一定是 5 小时。"""
    if not isinstance(seconds, (int, float)) or isinstance(seconds, bool) or seconds <= 0:
        return None   # 套餐关掉了这个窗口
    mins = int(seconds // 60)
    if mins % 1440 == 0:
        return f"{mins // 1440}d"
    if mins % 60 == 0:
        return f"{mins // 60}h"
    return f"{mins}m"


def parse_usage(data: Any, now: float | None = None) -> dict[str, Any]:
    """把 /wham/usage 的回应整理成分级用的记录：allowed、各窗口剩余百分比与重置时刻。"""
    now = time.time() if now is None else now
    if not isinstance(data, dict):
        return {"ok": False, "allowed": None, "buckets": [], "issues": [{"text": "额度回应不是对象"}]}
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
            "issues": [] if windows else [{"text": "额度回应里没有窗口数据"}]}


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
        hint = "（登录可能失效：pi-pool account login）" if e.code in (401, 403) else ""
        rec = {"ok": False, "allowed": None, "buckets": [], "issues": [{"text": f"额度查询 HTTP {e.code}{hint}"}]}
    except (OSError, ValueError, http.client.HTTPException) as e:
        rec = {"ok": False, "allowed": None, "buckets": [],
               "issues": [{"text": f"额度查询失败: {type(e).__name__}"}]}
    else:
        rec = parse_usage(data)
    rec["plan"] = plan
    return rec


def fetch_quota(cfg: dict[str, Any]) -> dict[str, Any]:
    """并行查询每个启用账号的额度。"""
    names = active_accounts(cfg)
    if not names:
        return {"fetched_at": time.time(), "error": "没有启用的账号", "accounts": {}}
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
    """返回 (分级, 说明)。"""
    bad = state.get("auth_bad", {}).get(name)
    if bad and bad.get("until", 0) > time.time():
        return "blocked", f"登录失效，需要 pi-pool account login {name}（{bad.get('reason', '')[:80]}）"
    until = state["cooldown"].get(name, {}).get("until", 0)
    if until > time.time():
        return "blocked", f"本机冷却到 {clock(until)}（{state['cooldown'][name].get('reason', '')}）"
    if not rec or not rec.get("ok"):
        issues = (rec or {}).get("issues") or []
        return "unknown", issues[0]["text"] if issues else "额度没查到"
    if rec.get("allowed") is False:
        return "blocked", "服务端 allowed=false"
    low = [w for w in main_windows(rec) if w["remaining_pct"] < low_pct]
    if low:
        return "low", "，".join(f"{w['window']} 剩 {w['remaining_pct']:.0f}%"
                               f"（{clock(w['reset_at'])} 重置）" for w in low)
    return "ok", ""


def headroom(rec: dict[str, Any] | None) -> float:
    return min((w["remaining_pct"] for w in main_windows(rec)), default=-1)


def running_counts(state: dict[str, Any]) -> Counter[str]:
    """各账号上正在运行的会话数，用来把并行任务分散到不同账号。"""
    out: Counter[str] = Counter()
    for rec in state["sessions"].values():
        run = rec.get("running")
        if run and running_here(run):
            out[str(run.get("account"))] += 1
    return out


def rank(cfg: dict[str, Any], quota: dict[str, Any], state: dict[str, Any], size: str,
         exclude: set[str], prefer: str | None) -> list[tuple[str, str, str]]:
    """可用账号按优先顺序排列，返回 [(账号, 分级, 说明)]。

    排序依次看：
    1. priority（配置里的整数，小的先用）；
    2. 会话上次用的账号排在同优先级第一，复用它的提示缓存；
    3. 分级：大任务 ok 优先、unknown 兜底；小任务先用 low，把 ok 账号的额度留给大任务；
    4. 正在运行的会话少的先用，并行任务分散到不同账号；
    5. 剩余额度多的先用。
    停用的账号不参与。
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


# --------------------------------------------------------------- 显示

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
            tier, why = "disabled", "已停用（pi-pool account set " + n + " --enable 启用）"
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
    print(f"额度数据 {age}s 前；low 阈值 {cfg['low_pct']}%"
          + (f"；{quota['error']}" if quota.get("error") else ""))
    if not cfg["from_file"]:
        print(f"（没有配置文件 {CONFIG_PATH}，用的是本机已有的默认目录；"
              "pi-pool account add / set 会生成它）")
    print(f"{'account':<8}{'prio':>4} {'plan':<9}{'allowed':<8}{'5h':>6}{'7d':>6}{'run':>4}  "
          f"{'tier':<9}note")
    for r in rows:
        allowed = {True: "yes", False: "no", None: "?"}[r["allowed"]]
        print(f"{r['account']:<8}{r['priority']:>4} {(r['plan'] or '?'):<9}{allowed:<8}"
              f"{r['remaining_5h']:>6}{r['remaining_7d']:>6}{r['running']:>4}  "
              f"{r['tier']:<9}{r['note']}")
    return 0


# --------------------------------------------------------------- 参数

def take(args: list[str], name: str, default: str | None = None) -> str | None:
    for i, a in enumerate(args):
        if a == name:
            if i + 1 >= len(args):
                raise SystemExit(f"pi-pool: {name} 需要一个值")
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
    """在会话目录里按 ID 前缀找 pi 的会话文件，返回 {会话 ID: cwd}。

    state.json 里没登记的会话（例如运行被 kill 前没来得及登记）靠这里找回。
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
    """把会话 ID 或前缀解析成完整 ID；找不到就报错，不会悄悄新建会话。"""
    if sid in state["sessions"]:
        return sid
    hits = [s for s in state["sessions"] if s.startswith(sid)]
    if not hits:
        hits = list(session_files(sid))
    if len(hits) == 1:
        return hits[0]
    if len(hits) > 1:
        raise SystemExit(f"pi-pool: 会话前缀 {sid} 对应多个会话: {', '.join(hits[:5])}")
    raise SystemExit(f"pi-pool: 找不到会话 {sid}（state.json 和 {SESSION_DIR} 里都没有）；"
                     "开新会话不要加 --session")


def check_size(size: str | None) -> str:
    size = size or "large"
    if size not in ("large", "small"):
        raise SystemExit("pi-pool: --size 只能是 large 或 small")
    return size


def cmd_pick(cfg: dict[str, Any], args: list[str]) -> int:
    size = check_size(take(args, "--size"))
    state = load_state()
    sid = take(args, "--session")
    prefer = state["sessions"].get(resolve_session(state, sid), {}).get("last_account") if sid else None
    ranked = rank(cfg, get_quota(cfg, refresh=take_flag(args, "--refresh")), state, size,
                  set(), prefer)
    if not ranked:
        print(f"pi-pool: 没有能接 {size} 任务的账号", file=sys.stderr)
        return 75
    for n, tier, why in ranked:
        print(f"{n}\t{tier}\t{why}")
    return 0


# --------------------------------------------------------------- 运行

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
        self.injected: Counter[str] = Counter()   # 运行中转发进去的插话，按种类计数
        self.bg_waited_s = 0.0                    # pi 停下后等后台任务用了多久（秒）
        self.bg_left: str | None = None           # pi 退出时还没完成的后台任务（会随进程终止）


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
                # 只累计 token 数；cost 是嵌套对象，跳过
                if isinstance(v, int) and not isinstance(v, bool):
                    res.usage[k] += v
            if progress:
                calls = [b.get("name") for b in msg.get("content") or []
                         if isinstance(b, dict) and b.get("type") == "toolCall"]
                progress_line(f"第 {res.turns} 轮 stop={res.stop_reason}"
                              + (f" 调用 {','.join(map(str, calls))}" if calls else "")
                              + (f" 错误: {res.error[:200]}" if res.error else ""))
    elif t == "tool_execution_end":
        res.tools[str(ev.get("toolName"))] += 1
        if ev.get("isError"):
            res.tool_errors += 1
            res.tool_error_streak += 1
            if progress and res.tool_error_streak % TOOL_ERROR_WARN == 0:
                progress_line(f"注意：连续 {res.tool_error_streak} 次工具调用出错"
                              f"（最近一次 {ev.get('toolName')}）")
        else:
            res.tool_error_streak = 0


ANSI_RE = re.compile(r"\x1b\[[0-9;?]*[ -/]*[@-~]|\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)")


def clean_status(text: Any) -> str | None:
    if not isinstance(text, str):
        return None
    text = " ".join(ANSI_RE.sub("", text).split())
    return text or None


# ---- 收件箱：steer/followup/abort 命令写入，正在运行的 pi-pool 取走并转给 pi

def inbox_dir(session: str) -> str:
    return os.path.join(INBOX_DIR, session)


def post_inbox(session: str, kind: str, message: str) -> str:
    """原子地写入一条消息，返回文件名。"""
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
    """取走一条消息；已被撤回或取走时返回 None。靠 rename 保证只有一方拿到。"""
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
    """一次 `pi --mode rpc` 运行：发任务，转发收件箱里的插话，等 pi 真正没有事可做后收尾。

    「没有事可做」= 收到 agent_settled，且 pi-async-bash 报告的后台任务（setStatus
    键 async-bash）都已完成并把结果送进对话。后台任务还在时不关 stdin：任务完成后
    扩展会唤醒 agent 开始新一轮，pi-pool 照常接着处理。

    主线程读 pi 的 stdout 并处理回应和事件；一个线程轮询收件箱，一个线程做看门狗。
    写 stdin 和改状态时共用 self.lock。
    """

    def __init__(self, proc: subprocess.Popen[bytes], session: str, res: RunResult,
                 progress: bool, stall_warn_s: float = DEFAULT_STALL_WARN) -> None:
        self.proc = proc
        self.session = session
        self.res = res
        self.progress = progress
        self.stall_warn_s = stall_warn_s
        self.lock = threading.RLock()
        self.active = False       # pi 正在跑一次运行（我们发起的，或扩展唤醒的）
        self.waiting_bg = False   # pi 已停下，在等后台任务
        self.wait_since = 0.0
        self.finishing = False    # 已决定收尾，不再转发
        self.abort_requested = False
        self.stdin_closed = False
        self.close_timer: threading.Timer | None = None
        self.run_prompt: str | None = None         # 发起当前运行的 prompt 命令 id
        self.after_abort: list[dict[str, Any]] = []  # --now 的消息：中止后作为新指令发
        self.callbacks: dict[str, Callable[[dict[str, Any]], None]] = {}
        self.seq = 0
        self.stop = threading.Event()
        self.bg_status: str | None = None          # async-bash 当前报告的后台任务
        self.bg_at_close: str | None = None        # 决定关闭 pi 时还没完成的后台任务
        self.last_event = time.time()
        self.last_warn = 0.0
        self.tools_running: dict[str, tuple[str, float]] = {}  # toolCallId -> (工具名, 开始时间)

    # ---- 写 stdin

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

    # ---- 运行

    def start_run(self, message: str) -> None:
        """在 pi 空闲时发起一次运行。调用方持有 self.lock。"""
        self.cancel_close_timer()
        self.finishing = False
        self.end_bg_wait()
        self.active = True
        # pi 空闲时忽略 streamingBehavior；万一它还在收尾，就排成 follow-up 而不是报错
        self.run_prompt = self.send({"type": "prompt", "message": message,
                                     "streamingBehavior": "followUp"}, self.on_prompt_response)

    def on_prompt_response(self, resp: dict[str, Any]) -> None:
        if resp.get("id") != self.run_prompt:
            return
        disposition = (resp.get("data") or {}).get("disposition")
        if resp.get("success") and disposition != "handled":
            return
        # 没有启动运行，也就不会有 agent_settled，按已结束处理
        if not resp.get("success"):
            self.res.error = f"pi 拒绝了任务: {resp.get('error')}"
        self.on_settled()

    def on_agent_start(self) -> None:
        """pi 开始一次运行：可能是我们发的 prompt，也可能是后台任务完成后扩展唤醒的。"""
        with self.lock:
            self.active = True
            if self.waiting_bg:
                self.note("后台任务的结果已送达，pi 继续运行")
            self.end_bg_wait()
            if self.finishing:
                # 收尾途中 pi 又开始干活（例如后台结果刚好到达）：不要关 stdin
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
                        self.ack(item, delivered=True, note="pi 已经停下，无需中止")
                        continue
                    texts.append(item["message"])
                    self.res.injected["new_prompt"] += 1
                    self.ack(item, delivered=True, disposition="started",
                             note="运行已结束，作为新一轮指令发出")
                if texts:
                    self.note(f"运行结束时有 {len(texts)} 条待发消息，作为新一轮指令发出")
                    self.start_run("\n\n".join(texts))
                    return
                if self.bg_status and not self.abort_requested:
                    if not self.waiting_bg:
                        self.waiting_bg = True
                        self.wait_since = time.time()
                        self.last_warn = time.time()
                        self.note(f"pi 已停下，等待后台任务完成后继续：{self.bg_status}")
                    return
            else:
                # 额度或登录失败：--now 的消息放回收件箱，换号后的运行会转发
                for item in self.after_abort:
                    post_inbox(self.session, "follow_up", item["message"])
                self.after_abort = []
            self.finish()

    def finish(self) -> None:
        """取回 pi 里还没送达的排队消息，然后关 stdin。调用方持有 self.lock。"""
        self.end_bg_wait()
        self.finishing = True
        # pi 关闭时扩展会取消后台任务并清除状态，所以在这里记下被终止的是哪些
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
                # clear_queue 回来之前 pi 已经开始新一轮：取回的消息排到它后面
                for t in texts:
                    self.send({"type": "follow_up", "message": t})
                return
            if not self.finishing:
                return
            if texts and not self.failed():
                self.note(f"pi 里还有 {len(texts)} 条排队消息，作为新一轮指令发出")
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
                pass  # pi 正在关闭：状态变化来自扩展取消任务，不是任务完成
            elif text:
                self.note(f"后台任务：{text}")
            elif had:
                self.note("后台任务已全部结束，结果都已送进对话")
            if not text and self.waiting_bg and not self.active and not self.finishing:
                # 一般后台结果会先唤醒 agent（on_agent_start）；没唤醒就直接收尾
                self.finish()

    # ---- 收件箱

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
        """运行中转发一条消息。调用方持有 self.lock。"""
        kind, text = item["kind"], item.get("message", "")
        self.res.injected[kind] += 1
        if kind in ("steer", "follow_up"):
            def on_resp(resp: dict[str, Any], item: dict[str, Any] = item) -> None:
                ok = bool(resp.get("success"))
                self.ack(item, delivered=ok, disposition=(resp.get("data") or {}).get("disposition"),
                         error=None if ok else str(resp.get("error")))
            self.send({"type": kind, "message": text}, on_resp)
            self.note(f"转发 {kind}: {' '.join(text.split())[:80]}")
        elif kind == "interrupt":
            self.after_abort.append(item)
            self.send({"type": "abort"})
            self.ack(item, delivered=True, disposition="aborting",
                     note="已中止当前操作，停下后作为新指令发出")
            self.note(f"中止当前操作，之后发出: {' '.join(text.split())[:80]}")
        else:
            self.abort_requested = True
            self.send({"type": "abort"})
            self.ack(item, delivered=True, disposition="aborting", note="已发送中止")
            self.note("收到 abort，中止运行")

    def handle_while_waiting(self, items: list[dict[str, Any]]) -> None:
        """pi 空闲、在等后台任务时收到的消息。调用方持有 self.lock。"""
        texts = []
        for item in items:
            if item["kind"] == "abort":
                self.abort_requested = True
                self.res.stop_reason = "aborted"
                self.res.injected["abort"] += 1
                self.ack(item, delivered=True, disposition="aborting",
                         note="pi 在等后台任务；关闭 pi，后台任务随之终止")
                self.note("收到 abort：不再等后台任务，关闭 pi")
                self.finish()
                return
            texts.append(item["message"])
            self.res.injected["new_prompt"] += 1
            self.ack(item, delivered=True, disposition="started",
                     note="pi 在等后台任务，作为新一轮指令发出")
        if texts:
            self.note(f"等待后台任务期间收到 {len(texts)} 条消息，作为新一轮指令发出")
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

    # ---- 看门狗：长时间没有事件时提示一次，正常运行时不输出

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
                    self.note(f"仍在等待后台任务（已等 {fmt_dur(now - self.wait_since)}）："
                              f"{self.bg_status}")
                elif self.tools_running:
                    name, since = min(self.tools_running.values(), key=lambda x: x[1])
                    self.note(f"{fmt_dur(quiet)} 没有新事件；工具 {name} 已运行 {fmt_dur(now - since)}")
                else:
                    self.note(f"{fmt_dur(quiet)} 没有新事件")

    # ---- 主循环

    def run(self, message: str, log: Any) -> None:
        assert self.proc.stdout is not None
        with self.lock:
            self.start_run(message)
        threads = [threading.Thread(target=self.pump_inbox, daemon=True),
                   threading.Thread(target=self.watch, daemon=True)]
        for th in threads:
            th.start()
        try:
            # JSONL 只按 LF 切分（pi 文档：U+2028 等不是记录边界）
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
                        # 没有人在看：扩展弹出的对话框一律取消，等同于没有界面
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
        raise SystemExit("pi-pool: PATH 上找不到 pi")
    env = dict(os.environ, PI_CODING_AGENT_DIR=agent_dir(cfg, account))
    # 子 pi 不继承调用方的会话 ID，它再调 pi-pool 时只留下自己的 PI_SESSION_ID
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
    """这一轮是不是因为账号本身失败：("quota", 错误) 额度用尽，("auth", 错误) 登录失效。

    这两种换个账号就能继续；其他错误（模型报错、工具出错）照常返回给调用方。
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
        raise SystemExit("pi-pool run: --prompt 和 --task-file 必须且只能给一个")
    if task_file is not None:
        try:
            with open(task_file, encoding="utf-8") as f:
                prompt = f.read()
        except OSError as e:
            raise SystemExit(f"pi-pool: 读不了任务文件 {task_file}: {e}") from e
    assert prompt is not None
    if not prompt.strip():
        raise SystemExit("pi-pool: 任务内容为空")
    return prompt


def cmd_run(cfg: dict[str, Any], args: list[str]) -> int:
    pi_args: list[str] = []
    if "--" in args:
        i = args.index("--")
        args, pi_args = args[:i], args[i + 1:]
    bad = [a for a in pi_args if a.split("=", 1)[0] in FORBIDDEN_PI_ARGS]
    if bad:
        raise SystemExit(f"pi-pool: 这些 pi 参数由 pi-pool 管理，不能透传: {' '.join(bad)}")
    message = read_message(take(args, "--prompt"), take(args, "--task-file"))
    account_opt = take(args, "--account", "auto") or "auto"
    size = check_size(take(args, "--size"))
    sid_opt = take(args, "--session")
    cwd_opt = take(args, "--cwd")
    max_switch = int(take(args, "--max-switch", "3") or 3)
    refresh = take_flag(args, "--refresh")
    if args:
        raise SystemExit(f"pi-pool run: 不认识的参数 {' '.join(args)}（pi 参数请放在 -- 之后）")
    if account_opt != "auto" and account_opt not in cfg["accounts"]:
        raise SystemExit(f"pi-pool: 没有账号 {account_opt}，可选 {', '.join(cfg['accounts'])}")
    if not active_accounts(cfg):
        raise SystemExit("pi-pool: 没有启用的账号；用 pi-pool account add 添加")

    ensure_dirs()
    state = load_state()
    session = resolve_session(state, sid_opt) if sid_opt else str(uuid.uuid4())
    srec = state["sessions"].get(session)
    # pi 按工作目录查找会话：续接时必须回到第一次的目录
    first_cwd = (srec or {}).get("cwd") or (session_files(session).get(session) if sid_opt else None)
    cwd = os.path.realpath(cwd_opt or first_cwd or os.getcwd())
    if first_cwd and cwd != os.path.realpath(first_cwd):
        raise SystemExit(f"pi-pool: 会话 {session} 属于目录 {first_cwd}，不能换到 {cwd}")
    if not os.path.isdir(cwd):
        raise SystemExit(f"pi-pool: 工作目录不存在 {cwd}")
    if srec and running_here(srec.get("running")):
        raise SystemExit(f"pi-pool: 会话 {session} 正在运行（pid {srec['running']['pid']}）；"
                         "要给它发消息用 pi-pool steer / followup")
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
                print(f"[pi-pool] 注意: 指定账号 {account} 分级 {tier}（{why}），仍按指定使用",
                      file=sys.stderr)
        else:
            ranked = rank(cfg, quota, load_state(), size, {t["account"] for t in tried}, prefer)
            if not ranked:
                print(f"[pi-pool] 没有能接 {size} 任务的账号了；运行 pi-pool status 查看",
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
            print(f"[pi-pool] 账号 {account} 被额度拒绝（{text[:160]}），冷却到 {clock(until)}",
                  file=sys.stderr, flush=True)
        else:
            until = time.time() + cfg.get("auth_cooldown_s", DEFAULT_AUTH_COOLDOWN)
            with locked_state() as st:
                st["auth_bad"][account] = {"until": until, "reason": text[:120], "at": int(time.time())}
            print(f"[pi-pool] 账号 {account} 登录失效（{text[:160]}），先不用它；"
                  f"重新登录：pi-pool account login {account}", file=sys.stderr, flush=True)
        invalidate_quota()
        if len(tried) > max_switch:
            print(f"[pi-pool] 已切换 {max_switch} 次，停止", file=sys.stderr)
            save_session(session, cwd, message, tried, account, res)
            return 75
        quota = get_quota(cfg)
        old = account
        nxt = rank(cfg, quota, load_state(), size, {t["account"] for t in tried}, None)
        new = nxt[0][0] if nxt else "?"
        # 这一轮的任务已进入会话就只发续接提示，否则重发原任务
        if res.saw_user:
            current_message = CONTINUE_PROMPT.format(
                old=old, new=new, why="额度限制" if kind == "quota" else "登录失效")
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
    """调用方的会话 ID，按客户端分开记；一个都没有时为 None。"""
    found = {client: os.environ[var] for client, var in CALLER_VARS.items()
             if os.environ.get(var)}
    return found or None


def new_record(cwd: str, message: str) -> dict[str, Any]:
    return {"cwd": cwd, "created": int(time.time()),
            "task": " ".join(message.split())[:120], "runs": []}


def mark_running(session: str, cwd: str, message: str, account: str, log_path: str) -> None:
    """开跑前就登记会话和运行中的进程，被 kill 时 --session 也能找回、steer 能找到它。"""
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
    """登记的进程是否还活着。别的节点上的进程查不了，按活着算。"""
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
    path = " → ".join(t["account"] + ("(额度)" if t.get("quota_error") else "")
                      + ("(登录失效)" if t.get("auth_error") else "") for t in tried)
    tools = ",".join(f"{k}×{v}" for k, v in res.tools.most_common()) or "无"
    u = res.usage
    print(f"[pi-pool] session={session} 账号 {path} exit={res.returncode} "
          f"stop={res.stop_reason} 轮数={res.turns} 用时={sum(t['elapsed_s'] for t in tried)}s")
    print(f"[pi-pool] tokens in={u['input']} out={u['output']} cacheRead={u['cacheRead']} "
          f"工具 {tools}" + (f"（{res.tool_errors} 次出错）" if res.tool_errors else ""))
    if res.injected:
        print("[pi-pool] 运行中插话 " + ",".join(f"{k}×{v}" for k, v in res.injected.items()))
    if res.bg_waited_s >= 1:
        print(f"[pi-pool] pi 停下后等后台任务 {fmt_dur(res.bg_waited_s)}")
    if res.bg_left:
        print(f"[pi-pool] 注意：pi 退出时仍有后台任务，已随进程终止：{res.bg_left}")
    print(f"[pi-pool] log={tried[-1]['log'] if tried else '-'}")
    if res.error:
        print(f"[pi-pool] 错误: {res.error}")
    if res.returncode not in (0, None) and res.stderr_tail:
        print("[pi-pool] stderr 末尾:\n" + "\n".join(res.stderr_tail[-10:]))
    print("---")
    print(res.final_text or "（没有文本回答）")


# --------------------------------------------------------------- 查看

def cmd_show(cfg: dict[str, Any], args: list[str]) -> int:
    if not args:
        raise SystemExit("pi-pool show: 需要会话 ID")
    state = load_state()
    sid = resolve_session(state, args[0])
    rec = state["sessions"].get(sid)
    if not rec:
        print(f"pi-pool: 没有会话 {sid}", file=sys.stderr)
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
    print(f"会话 {sid}  目录 {rec['cwd']}  任务: {rec.get('task', '')}")
    for r in rec.get("runs", []):
        print(f"  {clock(r['started'])} {r['account']:<5} {r['elapsed_s']:>5}s stop={r['stop']}"
              + (f"  额度: {r['quota_error'][:80]}" if r.get("quota_error") else "")
              + (f"  登录失效: {r['auth_error'][:80]}" if r.get("auth_error") else ""))
    print("---")
    print(res.final_text or "（最后一次运行没有文本回答）")
    return 0


def cmd_ls(cfg: dict[str, Any], args: list[str]) -> int:
    n = int(args[0]) if args else 10
    sessions = sorted(load_state()["sessions"].items(),
                      key=lambda kv: kv[1].get("updated", 0), reverse=True)[:n]
    for sid, rec in sessions:
        accounts = ">".join(dict.fromkeys(r["account"] for r in rec.get("runs", [])))
        stop = ("运行中(" + rec["running"]["account"] + ")" if running_here(rec.get("running"))
                else f"stop={rec.get('last_stop')}")
        print(f"{sid}  {clock(rec.get('updated'))}  {accounts:<12} "
              f"{stop}  {rec.get('task', '')[:60]}")
    return 0


# --------------------------------------------------------------- 账号管理

def find_pi() -> str:
    pi = os.environ.get("PI_POOL_PI") or shutil.which("pi")
    if not pi:
        raise SystemExit("pi-pool: PATH 上找不到 pi（npm install -g @earendil-works/pi-coding-agent）")
    return pi


def base_dir(cfg: dict[str, Any]) -> str:
    return os.path.realpath(os.path.expanduser(cfg["base_dir"]))


def link_shared(base: str, target: str) -> list[str]:
    """把 base 里除凭证外的每一项软链进 target（和旧的 ~/.pi/pi-link.sh 一样）。

    已经是软链的不动；同名的真实文件先改名成 .bak；指向 base 里已删除文件的失效软链
    清掉。返回做了哪些改动。
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
            done.append(f"{name}: 失效软链已删除（base 里已没有这个文件）")
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
            done.append(f"{name}: 原文件改名为 {os.path.basename(bak)}")
        os.symlink(os.path.join(base, name), dst)
        done.append(f"{name}: 已软链")
    return done


def broken_links(d: str) -> list[str]:
    try:
        names = os.listdir(d)
    except OSError:
        return []
    return [n for n in names if os.path.islink(os.path.join(d, n))
            and not os.path.exists(os.path.join(d, n))]


def auth_check(cfg: dict[str, Any], d: str) -> str:
    """pi auth check：ready / not_ready / invalid，或出错原因。不刷新凭证。"""
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
    """交互登录（device code，不开浏览器）；需要用户在终端里操作。"""
    print(f"pi-pool: 登录 {cfg['provider']} 到 {d}（按提示在浏览器里打开链接并输入代码）",
          file=sys.stderr, flush=True)
    env = dict(os.environ, PI_CODING_AGENT_DIR=d)
    p = subprocess.run([find_pi(), "auth", "login", cfg["provider"], "--no-browser"], env=env,
                       check=False)
    status = auth_check(cfg, d)
    print(f"pi-pool: 登录结果 exit={p.returncode}，auth check: {status}", file=sys.stderr)
    return status == "ready"


def clear_account_state(name: str) -> None:
    with locked_state() as st:
        st["auth_bad"].pop(name, None)
        st["cooldown"].pop(name, None)
    invalidate_quota()


def account_names(cfg: dict[str, Any], names: list[str]) -> list[str]:
    for n in names:
        if n not in cfg["accounts"]:
            raise SystemExit(f"pi-pool: 没有账号 {n}，可选 {', '.join(cfg['accounts'])}")
    return names or list(cfg["accounts"])


def take_int(args: list[str], name: str) -> int | None:
    v = take(args, name)
    if v is None:
        return None
    try:
        return int(v)
    except ValueError:
        raise SystemExit(f"pi-pool: {name} 需要整数") from None


def cmd_account(cfg: dict[str, Any], args: list[str]) -> int:
    sub = args.pop(0) if args else "list"
    if sub == "list":
        print(f"配置 {CONFIG_PATH}" + ("" if cfg["from_file"] else "（不存在，下面是默认值）")
              + f"；共享设置来自 {cfg['base_dir']}")
        print(f"{'account':<10}{'prio':>5} {'enabled':<8}{'auth.json':<10}{'坏链':>4}  dir")
        for n, a in sorted(cfg["accounts"].items(), key=lambda kv: (kv[1]["priority"], kv[0])):
            d = os.path.expanduser(a["dir"])
            has_auth = "有" if os.path.isfile(os.path.join(d, "auth.json")) else "无"
            print(f"{n:<10}{a['priority']:>5} {'yes' if a['enabled'] else 'no':<8}{has_auth:<10}"
                  f"{len(broken_links(d)):>4}  {a['dir']}")
        return 0
    if sub == "add":
        no_login = take_flag(args, "--no-login")
        d_opt = take(args, "--dir")
        prio = take_int(args, "--priority")
        if len(args) != 1:
            raise SystemExit("用法: pi-pool account add NAME [--dir DIR] [--priority N] [--no-login]")
        name = args[0]
        if name in cfg["accounts"]:
            raise SystemExit(f"pi-pool: 账号 {name} 已存在；重新登录用 pi-pool account login {name}")
        d_raw = d_opt or f"~/.pi/agent-{name}"
        d = os.path.realpath(os.path.expanduser(d_raw))
        base = base_dir(cfg)
        if d != base:
            if not os.path.isdir(base):
                os.makedirs(base, exist_ok=True)
                print(f"pi-pool: 共享设置目录 {base} 不存在，已新建；在里面配好 pi 后运行 "
                      "pi-pool account sync", file=sys.stderr)
            for line in link_shared(base, d):
                print(f"  {line}")
        else:
            os.makedirs(d, exist_ok=True)
        if not no_login and not pi_login(cfg, d):
            print(f"pi-pool: 登录没有完成，账号没有加入配置。目录 {d} 保留；"
                  f"重试：pi-pool account add {name} --dir {d_raw}", file=sys.stderr)
            return 1
        tmp = normalize_config({"accounts": {name: {"dir": d_raw, "priority": prio
                                                     if prio is not None else DEFAULT_PRIORITY}}})
        cfg["accounts"][name] = tmp["accounts"][name]
        save_config(cfg)
        clear_account_state(name)
        print(f"pi-pool: 已添加账号 {name}（priority {cfg['accounts'][name]['priority']}，{d_raw}），"
              f"配置写入 {CONFIG_PATH}")
        return 0
    if sub == "login":
        names = account_names(cfg, args)
        if len(names) != 1:
            raise SystemExit("用法: pi-pool account login NAME")
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
            raise SystemExit("用法: pi-pool account set NAME [--priority N] [--enable|--disable] [--dir DIR]")
        name = account_names(cfg, args)[0]
        acc = cfg["accounts"][name]
        if prio is not None:
            acc["priority"] = prio
        if enable or disable:
            acc["enabled"] = enable
        if d_opt is not None:
            acc["dir"] = d_opt
        save_config(cfg)
        print(f"pi-pool: {name} → priority {acc['priority']}，{'启用' if acc['enabled'] else '停用'}，"
              f"{acc['dir']}")
        return 0
    if sub == "remove":
        if len(args) != 1:
            raise SystemExit("用法: pi-pool account remove NAME")
        name = account_names(cfg, args)[0]
        d = cfg["accounts"].pop(name)["dir"]
        save_config(cfg)
        clear_account_state(name)
        print(f"pi-pool: 已从配置移除 {name}；目录 {d}（含凭证）没有删除")
        return 0
    if sub == "sync":
        base = base_dir(cfg)
        if not os.path.isdir(base):
            raise SystemExit(f"pi-pool: 共享设置目录 {base} 不存在")
        for n in account_names(cfg, args):
            changes = link_shared(base, agent_dir(cfg, n))
            print(f"{n}: " + ("无需改动" if not changes else "；".join(changes)))
        return 0
    if sub == "check":
        bad = 0
        for n in account_names(cfg, args):
            status = auth_check(cfg, agent_dir(cfg, n))
            bad += status != "ready"
            print(f"{n:<10}{status}")
        return 1 if bad else 0
    raise SystemExit(f"pi-pool account: 未知子命令 {sub}（list/add/login/set/remove/sync/check）")


def cmd_config(cfg: dict[str, Any], args: list[str]) -> int:
    if take_flag(args, "--write"):
        save_config(cfg)
        print(f"pi-pool: 已写入 {CONFIG_PATH}")
        return 0
    print(json.dumps(config_for_file(cfg), ensure_ascii=False, indent=1))
    return 0


def skill_link() -> str:
    return os.path.join(os.environ.get("PI_POOL_SKILLS_DIR") or os.path.join(HOME, ".claude", "skills"),
                        "pi-pool")


def cmd_doctor(cfg: dict[str, Any], args: list[str]) -> int:
    """检查这台机器能不能跑 pi-pool；有 FAIL 时退出码 1。"""
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
            v = f"无法运行: {e}"
        line("ok", f"pi {v}（{pi}）")
    else:
        line("FAIL", "PATH 上没有 pi：npm install -g @earendil-works/pi-coding-agent")
    which = shutil.which("pi-pool")
    if which and os.path.realpath(which) == os.path.realpath(__file__):
        line("ok", f"命令 pi-pool → {os.path.realpath(which)}")
    else:
        line("warn", f"命令 pi-pool {'指向 ' + os.path.realpath(which) if which else '不在 PATH 上'}；"
             f"运行 {os.path.join(here, 'install.sh')}")
    sk = skill_link()
    if os.path.realpath(sk) == here:
        line("ok", f"Claude skill {sk} → {here}")
    else:
        line("warn", f"Claude skill {sk} 没有指向 {here}；运行 install.sh")
    base = base_dir(cfg)
    settings = os.path.join(base, "settings.json")
    try:
        with open(settings, encoding="utf-8") as f:
            has_bg = "async-bash" in f.read()
    except OSError:
        has_bg = False
    line("ok" if has_bg else "warn",
         "pi-async-bash 已在共享设置里" if has_bg else
         f"{settings} 里没有 pi-async-bash：长命令只能前台跑（pi install npm:@chthub/pi-async-bash）")
    line("ok" if cfg["from_file"] else "warn",
         f"配置 {CONFIG_PATH}" + ("" if cfg["from_file"] else " 不存在，用的是默认目录；"
                                 "pi-pool config --write 生成"))
    enabled = active_accounts(cfg)
    if not enabled:
        line("FAIL", "没有启用的账号：pi-pool account add NAME")
    for n, a in sorted(cfg["accounts"].items(), key=lambda kv: (kv[1]["priority"], kv[0])):
        d = os.path.expanduser(a["dir"])
        tag = f"账号 {n}（priority {a['priority']}{'' if a['enabled'] else '，已停用'}）"
        if not os.path.isdir(d):
            line("FAIL" if a["enabled"] else "warn", f"{tag}: 目录 {d} 不存在")
            continue
        broken = broken_links(d)
        status = auth_check(cfg, d) if pi else "未检查"
        level = "ok" if status == "ready" else ("FAIL" if a["enabled"] else "warn")
        if level == "ok" and broken:
            level = "warn"
        line(level, f"{tag}: auth {status}"
             + (f"，失效软链 {', '.join(broken)}（pi-pool account sync {n} 清理）" if broken else "")
             + ("" if status == "ready" else f"；重新登录：pi-pool account login {n}"))
    try:
        ensure_dirs()
        probe = os.path.join(POOL_DIR, ".doctor")
        with open(probe, "w", encoding="utf-8") as f:
            f.write("ok")
        os.unlink(probe)
        line("ok", f"运行数据目录 {POOL_DIR} 可写")
    except OSError as e:
        line("FAIL", f"运行数据目录 {POOL_DIR} 不可写: {e}")
    return 1 if fails else 0


# --------------------------------------------------------------- 插话

def cmd_send(cfg: dict[str, Any], args: list[str], kind: str) -> int:
    """steer / followup / abort：写进收件箱，等正在运行的 pi-pool 确认送达。"""
    now = take_flag(args, "--now")
    task_file = take(args, "--file")
    wait = float(take(args, "--wait", str(ACK_WAIT_S)) or ACK_WAIT_S)
    if not args:
        raise SystemExit(f"pi-pool {kind}: 需要会话 ID")
    sid_arg = args.pop(0)
    if kind == "abort":
        if args or task_file or now:
            raise SystemExit("pi-pool abort: 只接受会话 ID")
        message = ""
    else:
        message = read_message(" ".join(args) if args else None, task_file)
    if now:
        if kind != "steer":
            raise SystemExit("pi-pool: --now 只能用于 steer")
        kind = "interrupt"
    state = load_state()
    sid = resolve_session(state, sid_arg)
    rec = state["sessions"].get(sid) or {}
    if not running_here(rec.get("running")):
        print(f"pi-pool: 会话 {sid} 没有在运行；续接用 "
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
        detail = "，".join(str(x) for x in (data.get("disposition"), data.get("note"),
                                            data.get("error")) if x)
        print(f"pi-pool: {kind} {'已送达' if ok else '没送达'}（{detail}）")
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
        print(f"pi-pool: {kind} 已被取走，但 {wait:.0f}s 内没收到确认；"
              "看运行的 stderr 或日志", file=sys.stderr)
        return 1
    print(f"pi-pool: {wait:.0f}s 内没人取走，已撤回；会话可能已结束或运行它的 pi-pool 卡住了",
          file=sys.stderr)
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
        print(f"pi-pool: 未知命令 {cmd}；pi-pool --help 查看用法", file=sys.stderr)
        return 2
    # 被 kill 时也要把 pi 子进程一起收掉
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
