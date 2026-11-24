#!/usr/bin/env python3
"""Fake pi for tests (`--mode rpc`): succeeds or reports a quota error depending on PI_CODING_AGENT_DIR; no network.

Reads RPC commands from stdin (prompt/steer/follow_up/abort/clear_queue/extension_ui_response),
writes responses and events to stdout, and exits when stdin closes.

FAKE_PI_FAIL: comma-separated agent directory names whose accounts return a usage-limit error.
FAKE_PI_CALLS: appends {"account","argv","cwd"} at start, then {"account","cmd"} for every command.
FAKE_PI_DELAY: seconds each tool call takes; abort can interrupt it. Default 0.
FAKE_PI_DIALOG: if set, opens a confirm dialog at the start of each run and waits for the client.
FAKE_PI_AUTH_FAIL: comma-separated agent directory names whose accounts return an expired-login error.
FAKE_PI_BG: seconds. Like pi-async-bash, the first run reports one background job (setStatus
  async-bash); after this many seconds the job finishes: the agent is woken (agent_start, async_bash_result,
  an answer) and the status is cleared. If the job is still pending at exit, {"bg": "killed"} is logged.
FAKE_PI_PENDING_ON_CLEAR: the first clear_queue returns this queued follow-up message.

Also supports `auth login/check` and `--version` for the account-management tests: login writes auth.json
into PI_CODING_AGENT_DIR (fails if FAKE_PI_LOGIN_FAIL is set); check reports whether it exists.
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
    # like real pi: idle by the time agent_settled is emitted, so an immediate prompt starts a new run
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
    """The background job finished: wake the agent the way pi-async-bash's triggerTurn does."""
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
    # like pi-async-bash: on shutdown jobs are cancelled; the status turns into "pending delivery", then clears
    emit({"type": "extension_ui_request", "id": "st-3", "method": "setStatus",
          "statusKey": "async-bash", "statusText": "Background bash · 1 result pending delivery"})
    emit({"type": "extension_ui_request", "id": "st-4", "method": "setStatus",
          "statusKey": "async-bash"})
    log({"bg": "killed"})
