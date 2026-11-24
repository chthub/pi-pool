#!/usr/bin/env python3
"""测试用的假 pi（`--mode rpc`）：按 PI_CODING_AGENT_DIR 决定成功还是报额度错误，不联网。

从 stdin 读 RPC 命令（prompt/steer/follow_up/abort/clear_queue/extension_ui_response），
往 stdout 写回应和事件；stdin 关闭后退出。

FAKE_PI_FAIL：逗号分隔的 agent 目录名，这些账号返回 usage limit 错误。
FAKE_PI_CALLS：启动时追加一行 {"account","argv","cwd"}，之后每条命令追加 {"account","cmd"}。
FAKE_PI_DELAY：每次工具调用耗时（秒），期间可以被 abort 打断；默认 0。
FAKE_PI_DIALOG：设了就在每次运行开始时弹一个 confirm 对话框，等客户端回应。
FAKE_PI_AUTH_FAIL：逗号分隔的 agent 目录名，这些账号返回登录失效错误。
FAKE_PI_BG：秒数。第一次运行开始时像 pi-async-bash 一样报告一个后台任务（setStatus
  async-bash），这么多秒后任务完成：唤醒 agent（agent_start、async_bash_result、回答）
  并清除状态。进程退出时任务还没完成就记一行 {"bg": "killed"}。
FAKE_PI_PENDING_ON_CLEAR：第一次 clear_queue 时返回这条排队的 follow-up 消息。

也支持 `auth login/check` 和 `--version`，给账号管理命令测试用：login 在
PI_CODING_AGENT_DIR 里写 auth.json（FAKE_PI_LOGIN_FAIL 设了就失败），check 看它在不在。
"""

import json
import os
import queue
import sys
import threading

agent_dir = os.environ.get("PI_CODING_AGENT_DIR", "")
account = os.path.basename(agent_dir)

if sys.argv[1:2] == ["--version"]:
    print("0.0.0-fake")
    sys.exit(0)
if sys.argv[1:2] == ["auth"]:
    auth_path = os.path.join(agent_dir, "auth.json")
    if sys.argv[2:3] == ["login"]:
        if os.environ.get("FAKE_PI_LOGIN_FAIL"):
            print("login cancelled", file=sys.stderr)
            sys.exit(1)
        with open(auth_path, "w", encoding="utf-8") as f:
            json.dump({sys.argv[3]: {"type": "oauth", "access": "x", "refresh": "y"}}, f)
        print("Logged in")
        sys.exit(0)
    if sys.argv[2:3] == ["check"]:
        ready = os.path.isfile(auth_path)
        print(json.dumps({"status": "ready" if ready else "not_ready"}))
        sys.exit(0 if ready else 1)
    sys.exit(2)
calls = os.environ.get("FAKE_PI_CALLS")
delay = float(os.environ.get("FAKE_PI_DELAY") or 0)
fail = account in os.environ.get("FAKE_PI_FAIL", "").split(",")
auth_fail = account in os.environ.get("FAKE_PI_AUTH_FAIL", "").split(",")
bg_delay = float(os.environ.get("FAKE_PI_BG") or 0)
bg = {"started": False, "done": False}
pending_on_clear = [os.environ["FAKE_PI_PENDING_ON_CLEAR"]] if os.environ.get(
    "FAKE_PI_PENDING_ON_CLEAR") else []

lock = threading.Lock()
out_lock = threading.Lock()
steering = []
follow = []
prompts = queue.Queue()
abort_evt = threading.Event()
idle = threading.Event()
idle.set()
dialog_answered = threading.Event()
busy = False


def log(rec):
    if calls:
        with lock, open(calls, "a", encoding="utf-8") as f:
            f.write(json.dumps(dict(rec, account=account), ensure_ascii=False) + "\n")


def emit(ev):
    with out_lock:
        sys.stdout.write(json.dumps(ev, ensure_ascii=False) + "\n")
        sys.stdout.flush()


def respond(cmd, data=None):
    ev = {"type": "response", "command": cmd["type"], "success": True}
    if cmd.get("id"):
        ev["id"] = cmd["id"]
    if data is not None:
        ev["data"] = data
    emit(ev)


def user(text):
    emit({"type": "message_end", "message": {"role": "user", "content": text}})


def assistant(stop, content, **extra):
    emit({"type": "message_end", "message": dict({
        "role": "assistant", "stopReason": stop, "content": content,
        "usage": {"input": 100, "output": 5, "cacheRead": 0}}, **extra)})


def settle(aborted=False):
    global busy
    emit({"type": "agent_end", "messages": [], "willRetry": False})
    # 和真 pi 一样：发出 agent_settled 时已经空闲，紧接着的 prompt 会开新一轮
    with lock:
        busy = False
        idle.set()
    emit({"type": "agent_settled", "aborted": aborted})


def run(message):
    emit({"type": "agent_start"})
    if os.environ.get("FAKE_PI_DIALOG"):
        dialog_answered.clear()
        emit({"type": "extension_ui_request", "id": "dlg-1", "method": "confirm",
              "title": "Allow?", "message": "?"})
        dialog_answered.wait(5)
    user(message)
    if bg_delay and not bg["started"]:
        bg["started"] = True
        emit({"type": "extension_ui_request", "id": "st-1", "method": "setStatus",
              "statusKey": "async-bash",
              "statusText": "Background bash · 1 running: bg-0000abcd sleep 300"})
        log({"bg": "started"})
        timer = threading.Timer(bg_delay, lambda: prompts.put(("wake", None)))
        timer.daemon = True
        timer.start()
    if auth_fail:
        assistant("error", [], errorMessage="401 Unauthorized: your refresh token has expired, "
                  "please log in again.")
        settle()
        return
    if fail:
        assistant("error", [], errorMessage="You have hit your ChatGPT usage limit "
                  "(plus plan). Try again in ~30 min.")
        settle()
        return
    last = message
    while True:
        assistant("toolUse", [{"type": "toolCall", "id": "1", "name": "read", "arguments": {}}])
        if abort_evt.wait(delay):
            assistant("aborted", [])
            settle(aborted=True)
            return
        emit({"type": "tool_execution_end", "toolCallId": "1", "toolName": "read",
              "isError": False})
        with lock:
            queued = steering[:]
            steering.clear()
        if queued:
            for m in queued:
                user(m)
            last = queued[-1]
            continue
        assistant("stop", [{"type": "text", "text": f"done by {account} (last: {last})"}])
        with lock:
            nxt = follow.pop(0) if follow else None
        if nxt is None:
            break
        user(nxt)
        last = nxt
    settle()


def wake():
    """后台任务完成：像 pi-async-bash 的 triggerTurn 一样唤醒 agent。"""
    global busy
    with lock:
        busy = True
        idle.clear()
    emit({"type": "agent_start"})
    emit({"type": "message_end", "message": {"role": "custom", "customType": "async_bash_result",
                                             "content": "Background bash result: done",
                                             "details": {"job_id": "bg-0000abcd"}}})
    assistant("stop", [{"type": "text", "text": f"bg result handled by {account}"}])
    emit({"type": "extension_ui_request", "id": "st-2", "method": "setStatus",
          "statusKey": "async-bash"})
    bg["done"] = True
    log({"bg": "done"})
    settle()


def reader():
    global busy
    for line in sys.stdin:
        cmd = json.loads(line)
        log({"cmd": cmd})
        t = cmd.get("type")
        if t == "prompt":
            with lock:
                if busy:
                    (follow if cmd.get("streamingBehavior") == "followUp" else steering).append(
                        cmd["message"])
                    disposition = "queued"
                else:
                    busy = True
                    idle.clear()
                    prompts.put(cmd["message"])
                    disposition = "started"
            respond(cmd, {"disposition": disposition})
        elif t in ("steer", "follow_up"):
            with lock:
                (steering if t == "steer" else follow).append(cmd["message"])
            respond(cmd, {"disposition": "queued"})
        elif t == "abort":
            abort_evt.set()
            idle.wait(10)
            abort_evt.clear()
            respond(cmd)
        elif t == "clear_queue":
            with lock:
                data = {"steering": steering[:], "followUp": follow[:] + pending_on_clear}
                pending_on_clear.clear()
                steering.clear()
                follow.clear()
            respond(cmd, data)
        elif t == "extension_ui_response":
            dialog_answered.set()
    prompts.put(None)


log({"argv": sys.argv[1:], "cwd": os.getcwd(),
     "session_env": sorted(v for v in ("CLAUDE_CODE_SESSION_ID", "PI_SESSION_ID",
                                       "PI_SESSION_FILE", "CODEX_THREAD_ID")
                           if v in os.environ)})
threading.Thread(target=reader, daemon=True).start()
while True:
    msg = prompts.get()
    if msg is None:
        break
    if isinstance(msg, tuple):
        wake()
        continue
    run(msg)
if bg["started"] and not bg["done"]:
    # 和 pi-async-bash 一样：关闭时取消任务，状态先变成「待送达」，再清除
    emit({"type": "extension_ui_request", "id": "st-3", "method": "setStatus",
          "statusKey": "async-bash", "statusText": "Background bash · 1 result pending delivery"})
    emit({"type": "extension_ui_request", "id": "st-4", "method": "setStatus",
          "statusKey": "async-bash"})
    log({"bg": "killed"})
