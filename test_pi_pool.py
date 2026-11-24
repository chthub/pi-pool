"""Offline tests for pi-pool: quota data is written by hand and pi is replaced by fake_pi.py; no network requests."""
# fixtures are created in setUp, as is usual for unittest
# pyright: reportUninitializedInstanceVariable=false

import contextlib
import io
import json
import os
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

import pi_pool  # pyright: ignore[reportImplicitRelativeImport]

HERE = os.path.dirname(os.path.abspath(__file__))
FAKE_PI = os.path.join(HERE, "fake_pi.py")
FUTURE = int(time.time()) + 3600


def rec(windows, allowed=True, ok=True):
    return {"ok": ok, "allowed": allowed, "plan": "plus", "issues": [],
            "buckets": [{"name": "codex", "windows": [
                {"window": w, "remaining_pct": r, "reset_at": FUTURE} for w, r in windows]}]
            if ok else []}


QUOTA = {
    "main": rec([("7d", 21)]),
    "alpha": rec([("5h", 100), ("7d", 0)]),
    "beta": rec([("5h", 3), ("7d", 50)]),
    "gamma": rec([("5h", 82), ("7d", 67)]),
}


class PoolCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = self.tmp.name
        # legacy format (directory strings + last_resort) via normalize_config: main gets priority 90, the rest 50
        self.cfg = pi_pool.normalize_config(
            {"accounts": {n: os.path.join(root, f"agent-{n}") for n in QUOTA},
             "low_pct": 5, "quota_ttl_s": 120, "last_resort": ["main"],
             "base_dir": os.path.join(root, "agent-base")})
        self.cfg["from_file"] = True
        self.quota = {"fetched_at": time.time(), "error": None, "accounts": dict(QUOTA)}
        pool = os.path.join(root, "pool")
        self.patches = [
            patch.object(pi_pool, "POOL_DIR", pool),
            patch.object(pi_pool, "SESSION_DIR", os.path.join(pool, "sessions")),
            patch.object(pi_pool, "LOG_DIR", os.path.join(pool, "logs")),
            patch.object(pi_pool, "INBOX_DIR", os.path.join(pool, "inbox")),
            patch.object(pi_pool, "QUOTA_CACHE", os.path.join(pool, "quota.json")),
            patch.object(pi_pool, "STATE_PATH", os.path.join(pool, "state.json")),
            patch.object(pi_pool, "STATE_LOCK", os.path.join(pool, "state.lock")),
            patch.object(pi_pool, "CONFIG_PATH", os.path.join(root, "config", "config.json")),
            patch.object(pi_pool, "fetch_quota", side_effect=lambda cfg: dict(
                self.quota, fetched_at=time.time())),
        ]
        for p in self.patches:
            p.start()
        self.calls = os.path.join(root, "calls.jsonl")
        self.env = patch.dict(os.environ, {
            "PI_POOL_PI": FAKE_PI, "FAKE_PI_CALLS": self.calls, "FAKE_PI_FAIL": "",
            "FAKE_PI_DELAY": "0", "FAKE_PI_DIALOG": ""})
        self.env.start()
        # the tests themselves may run inside some client's shell; patch.dict restores the environment in tearDown
        for var in (*pi_pool.CALLER_VARS.values(), "PI_SESSION_FILE"):
            os.environ.pop(var, None)
        os.chmod(FAKE_PI, 0o755)

    def tearDown(self):
        self.env.stop()
        for p in self.patches:
            p.stop()
        self.tmp.cleanup()

    def state(self):
        return pi_pool.load_state()

    def run_pool(self, *args):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = pi_pool.cmd_run(self.cfg, list(args))
        return code, out.getvalue(), err.getvalue()

    def call_log(self):
        with open(self.calls, encoding="utf-8") as f:
            return [json.loads(line) for line in f]

    def starts(self):
        """Every start of the fake pi: account, argv, cwd."""
        return [c for c in self.call_log() if "argv" in c]

    def commands(self, kind=None):
        return [c["cmd"] | {"account": c["account"]} for c in self.call_log()
                if "cmd" in c and (kind is None or c["cmd"]["type"] == kind)]

    def run_in_thread(self, *args):
        box = {}

        def target():
            box["result"] = self.run_pool(*args)

        t = threading.Thread(target=target)
        t.start()
        return t, box

    def send(self, kind, *args):
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            return pi_pool.cmd_send(self.cfg, list(args), kind), err.getvalue()

    def wait_running(self, timeout=10):
        deadline = time.time() + timeout
        while time.time() < deadline:
            for sid, rec in self.state()["sessions"].items():
                if rec.get("running"):
                    return sid
            time.sleep(0.05)
        self.fail("run was not recorded as running")


class ClassifyTest(PoolCase):
    def test_tiers(self):
        st = self.state()
        tiers = {n: pi_pool.classify(n, QUOTA[n], st, 5)[0] for n in QUOTA}
        self.assertEqual(tiers, {"main": "ok", "alpha": "low", "beta": "low", "gamma": "ok"})

    def test_allowed_false_and_unknown(self):
        st = self.state()
        self.assertEqual(pi_pool.classify("x", rec([("7d", 50)], allowed=False), st, 5)[0],
                         "blocked")
        self.assertEqual(pi_pool.classify("x", rec([], ok=False), st, 5)[0], "unknown")
        self.assertEqual(pi_pool.classify("x", None, st, 5)[0], "unknown")

    def test_cooldown_blocks(self):
        st = {"sessions": {}, "cooldown": {"gamma": {"until": FUTURE, "reason": "t"}}}
        self.assertEqual(pi_pool.classify("gamma", QUOTA["gamma"], st, 5)[0], "blocked")

    def test_rank_large_prefers_headroom(self):
        ranked = pi_pool.rank(self.cfg, self.quota, self.state(), "large", set(), None)
        self.assertEqual([r[0] for r in ranked], ["gamma", "main"])

    def test_rank_small_uses_low_first(self):
        ranked = pi_pool.rank(self.cfg, self.quota, self.state(), "small", set(), None)
        self.assertEqual([r[0] for r in ranked], ["beta", "alpha", "gamma", "main"])

    def test_rank_sticks_to_previous_account(self):
        quota = dict(self.quota, accounts=dict(QUOTA, alpha=rec([("5h", 90), ("7d", 30)])))
        ranked = pi_pool.rank(self.cfg, quota, self.state(), "large", set(), "alpha")
        self.assertEqual([r[0] for r in ranked], ["alpha", "gamma", "main"])
        # the previous account is not kept when its tier no longer fits
        ranked = pi_pool.rank(self.cfg, self.quota, self.state(), "large", set(), "alpha")
        self.assertEqual(ranked[0][0], "gamma")

    def test_main_is_last_even_with_more_headroom_or_sticky(self):
        quota = dict(self.quota, accounts=dict(QUOTA, main=rec([("7d", 99)]),
                                               gamma=rec([("5h", 10), ("7d", 10)])))
        ranked = pi_pool.rank(self.cfg, quota, self.state(), "large", set(), "main")
        self.assertEqual([r[0] for r in ranked], ["gamma", "main"])

    def test_priority_decides_order_before_headroom(self):
        cfg = pi_pool.normalize_config({"accounts": {
            "main": {"dir": "/a", "priority": 10}, "gamma": {"dir": "/b", "priority": 20},
            "alpha": {"dir": "/c"}, "beta": {"dir": "/d", "enabled": False}}})
        quota = dict(self.quota, accounts=dict(QUOTA, alpha=rec([("5h", 99), ("7d", 99)])))
        ranked = pi_pool.rank(cfg, quota, self.state(), "large", set(), None)
        self.assertEqual([r[0] for r in ranked], ["main", "gamma", "alpha"], "disabled beta is excluded")

    def test_parallel_runs_spread_within_same_priority(self):
        quota = dict(self.quota, accounts=dict(QUOTA, alpha=rec([("5h", 99), ("7d", 99)])))
        with pi_pool.locked_state() as st:
            st["sessions"]["s1"] = {"running": {"pid": os.getpid(), "host": pi_pool.socket.gethostname(),
                                                "account": "alpha"}}
        ranked = pi_pool.rank(self.cfg, quota, self.state(), "large", set(), None)
        self.assertEqual([r[0] for r in ranked][:2], ["gamma", "alpha"], "alpha has more quota but already runs a session")

    def test_auth_bad_account_is_blocked(self):
        with pi_pool.locked_state() as st:
            st["auth_bad"]["gamma"] = {"until": time.time() + 60, "reason": "401"}
        tier, why = pi_pool.classify("gamma", QUOTA["gamma"], self.state(), 5)
        self.assertEqual(tier, "blocked")
        self.assertIn("pi-pool account login gamma", why)


class QuotaParseTest(unittest.TestCase):
    """Built-in /wham/usage parsing; no requests."""

    def test_windows_and_remaining(self):
        rec = pi_pool.parse_usage({"rate_limit": {"allowed": True, "primary_window": {
            "limit_window_seconds": 18000, "used_percent": 25, "reset_at": 2000},
            "secondary_window": {"limit_window_seconds": 604800, "used_percent": 100.0,
                                 "reset_after_seconds": 60}}}, now=1000)
        self.assertTrue(rec["ok"])
        self.assertTrue(rec["allowed"])
        w = rec["buckets"][0]["windows"]
        self.assertEqual([(x["window"], x["remaining_pct"], x["reset_at"]) for x in w],
                         [("5h", 75.0, 2000), ("7d", 0.0, 1060)])

    def test_disabled_window_limit_reached_and_bad_input(self):
        rec = pi_pool.parse_usage({"rate_limit": {"allowed": True, "limit_reached": True,
                                                  "primary_window": {"limit_window_seconds": 0},
                                                  "secondary_window": {"limit_window_seconds": 604800,
                                                                       "used_percent": "x"}}})
        self.assertFalse(rec["allowed"], "limit_reached counts as not allowed")
        self.assertEqual([x["window"] for x in rec["buckets"][0]["windows"]], ["7d"], "a 0-second window is turned off")
        self.assertEqual(rec["buckets"][0]["windows"][0]["remaining_pct"], 100.0, "non-numeric means 0% used")
        for bad in (None, [], "<html>", {"rate_limit": "x"}, {}):
            rec = pi_pool.parse_usage(bad)
            self.assertFalse(rec["ok"])
            self.assertTrue(rec["issues"])

    def test_credentials(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "auth.json")
            self.assertIn("no auth.json", pi_pool.read_codex_cred(path)[1])
            for content, expect in (("{bad", "malformed"), ("[]", "not a JSON object"),
                                    ('{"openai": {"type": "api_key", "key": "k"}}', "no openai-codex"),
                                    ('{"openai-codex": {"type": "oauth"}}', "no access token")):
                with open(path, "w", encoding="utf-8") as f:
                    f.write(content)
                cred, err = pi_pool.read_codex_cred(path)
                self.assertIsNone(cred)
                self.assertIn(expect, err)
                self.assertNotIn('"k"', err, "error messages never carry credential contents")
            with open(path, "w", encoding="utf-8") as f:
                f.write('{"openai": {"type": "oauth", "access": "a.b.c"}}')
            self.assertEqual(pi_pool.read_codex_cred(path)[0], {"type": "oauth", "access": "a.b.c"})
        self.assertEqual(pi_pool.jwt_claims("x.e30.y"), {})
        self.assertEqual(pi_pool.jwt_claims("not-a-jwt"), {})
        self.assertEqual(pi_pool.window_label(90 * 60), "90m")


class ConfigTest(unittest.TestCase):
    def test_legacy_strings_and_last_resort(self):
        cfg = pi_pool.normalize_config({"accounts": {"main": "~/.pi/agent", "alpha": "~/.pi/agent-alpha"},
                                        "last_resort": ["main"], "low_pct": 7})
        self.assertEqual(cfg["accounts"]["main"], {"dir": "~/.pi/agent", "priority": 90, "enabled": True})
        self.assertEqual(cfg["accounts"]["alpha"]["priority"], 50)
        self.assertEqual(cfg["low_pct"], 7)
        self.assertEqual(cfg["base_dir"], "~/.pi/agent")

    def test_invalid_entries_rejected(self):
        for bad in ({"accounts": {"x": {"priority": 1}}}, {"accounts": {"a b": "/x"}},
                    {"accounts": {"x": {"dir": "/x", "priority": "1"}}},
                    {"accounts": {"x": {"dir": "/x", "enabled": "yes"}}}, {"low_pct": "5"}):
            with self.assertRaises(SystemExit):
                pi_pool.normalize_config(bad)

    def test_file_roundtrip(self):
        cfg = pi_pool.normalize_config({"accounts": {"alpha": {"dir": "/g", "priority": 3}}})
        again = pi_pool.normalize_config(json.loads(json.dumps(pi_pool.config_for_file(cfg))))
        self.assertEqual(again["accounts"], cfg["accounts"])


class FailureDetectionTest(unittest.TestCase):
    def test_quota_error_detected(self):
        res = pi_pool.RunResult()
        res.stop_reason, res.error = "error", "You have hit your ChatGPT usage limit."
        self.assertIsNotNone(pi_pool.quota_failure(res))

    def test_real_codex_limit_message(self):
        # the errorMessage pi actually gave when a plus account ran out of its 7d quota
        res = pi_pool.RunResult()
        res.stop_reason, res.error = "error", "Codex error: The usage limit has been reached"
        self.assertIsNotNone(pi_pool.quota_failure(res))

    def test_other_error_not_quota(self):
        res = pi_pool.RunResult()
        res.stop_reason, res.error, res.returncode = "error", "context length exceeded", 1
        self.assertIsNone(pi_pool.quota_failure(res))

    def test_request_id_digits_are_not_429(self):
        res = pi_pool.RunResult()
        res.stop_reason, res.error = "error", "server error id=14290"
        self.assertIsNone(pi_pool.quota_failure(res))

    def test_cooldown_uses_try_again_hint(self):
        until = pi_pool.cooldown_until("Try again in ~30 min.", None)
        self.assertAlmostEqual(until - time.time(), 31 * 60, delta=5)


class RunTest(PoolCase):
    def test_run_success_records_session(self):
        code, out, _ = self.run_pool("--prompt", "hi", "--", "--tools", "read")
        self.assertEqual(code, 0)
        self.assertIn("done by agent-gamma (last: hi)", out)
        self.assertIn("tools read×1", out)
        argv = self.starts()[0]["argv"]
        self.assertEqual(argv, ["--mode", "rpc", "--session-dir", pi_pool.SESSION_DIR,
                                "--session-id", argv[5], "--tools", "read"])
        self.assertEqual([c["message"] for c in self.commands("prompt")], ["hi"])
        st = self.state()
        ((_sid, srec),) = st["sessions"].items()
        self.assertEqual(srec["last_account"], "gamma")
        self.assertEqual(srec["cwd"], os.path.realpath(os.getcwd()))
        self.assertNotIn("running", srec)
        self.assertIsNone(srec["runs"][0]["caller"])

    def test_caller_session_recorded_and_not_inherited(self):
        os.environ.update(CLAUDE_CODE_SESSION_ID="claude-parent", PI_SESSION_FILE="/x.jsonl")
        os.environ["FAKE_PI_FAIL"] = "agent-gamma"
        code, _, err = self.run_pool("--prompt", "hi")
        self.assertEqual(code, 0, err)
        ((_sid, srec),) = self.state()["sessions"].items()
        self.assertEqual([r["caller"] for r in srec["runs"]],
                         [{"claude": "claude-parent"}] * 2)
        self.assertEqual([s["session_env"] for s in self.starts()], [[], []])

    def test_quota_failure_switches_and_continues_same_session(self):
        os.environ["FAKE_PI_FAIL"] = "agent-gamma"
        code, out, err = self.run_pool("--prompt", "hi")
        self.assertEqual(code, 0, err)
        starts = self.starts()
        self.assertEqual([c["account"] for c in starts], ["agent-gamma", "agent-main"])
        sid = starts[0]["argv"][5]
        self.assertEqual(starts[1]["argv"][5], sid)
        prompts = self.commands("prompt")
        self.assertEqual(prompts[0]["message"], "hi")
        self.assertIn("previous turn on account gamma was interrupted by a quota limit", prompts[1]["message"])
        self.assertIn("gamma(quota) → main", out)
        st = self.state()
        self.assertGreater(st["cooldown"]["gamma"]["until"], time.time() + 25 * 60)
        self.assertEqual(st["sessions"][sid]["last_account"], "main")

    def test_all_accounts_exhausted_returns_75(self):
        os.environ["FAKE_PI_FAIL"] = "agent-gamma,agent-main"
        code, _, err = self.run_pool("--prompt", "hi")
        self.assertEqual(code, 75)
        self.assertIn("no account left that can take a large task", err)

    def test_small_task_can_use_low_account(self):
        code, out, _ = self.run_pool("--prompt", "hi", "--size", "small")
        self.assertEqual(code, 0)
        self.assertIn("done by agent-beta", out)

    def test_resume_keeps_cwd_and_account(self):
        self.run_pool("--prompt", "hi")
        sid = self.starts()[0]["argv"][5]
        code, _, _ = self.run_pool("--prompt", "again", "--session", sid[:8])
        self.assertEqual(code, 0)
        second = self.starts()[1]
        self.assertEqual(second["argv"][5], sid)
        self.assertEqual(second["account"], "agent-gamma")
        with self.assertRaises(SystemExit):
            self.run_pool("--prompt", "x", "--session", sid, "--cwd", self.tmp.name)

    def test_pinned_account_is_used(self):
        code, out, _ = self.run_pool("--prompt", "hi", "--account", "alpha")
        self.assertEqual(code, 0)
        self.assertIn("done by agent-alpha", out)

    def test_forbidden_pi_args(self):
        with self.assertRaises(SystemExit):
            self.run_pool("--prompt", "hi", "--", "--session-id", "x")
        with self.assertRaises(SystemExit):
            self.run_pool("--prompt", "hi", "--task-file", "/nonexistent")


class SessionResolveTest(PoolCase):
    def test_unknown_prefix_is_an_error(self):
        with self.assertRaises(SystemExit):
            self.run_pool("--prompt", "x", "--session", "deadbeef")
        self.assertFalse(os.path.exists(self.calls))

    def test_unregistered_session_found_from_file(self):
        # a session killed before it reached state.json: recover its ID and cwd from the session file header
        sid = "abcdef12-3456-4789-8abc-def012345678"
        os.makedirs(pi_pool.SESSION_DIR)
        with open(os.path.join(pi_pool.SESSION_DIR, f"2026-01-01T00-00-00-000Z_{sid}.jsonl"),
                  "w", encoding="utf-8") as f:
            f.write(json.dumps({"type": "session", "id": sid, "cwd": self.tmp.name}) + "\n")
        code, _, err = self.run_pool("--prompt", "go on", "--session", "abcdef12")
        self.assertEqual(code, 0, err)
        start = self.starts()[0]
        self.assertEqual(start["argv"][5], sid)
        self.assertEqual(start["cwd"], os.path.realpath(self.tmp.name))


class SteerTest(PoolCase):

    def test_steer_reaches_running_session(self):
        os.environ["FAKE_PI_DELAY"] = "1"
        t, box = self.run_in_thread("--prompt", "hi")
        sid = self.wait_running()
        code, err = self.send("steer", sid[:8], "switch to", "B")
        t.join(20)
        self.assertEqual(code, 0, err)
        rc, out, _ = box["result"]
        self.assertEqual(rc, 0)
        self.assertIn("(last: switch to B)", out)
        self.assertIn("injected steer×1", out)
        self.assertEqual([c["message"] for c in self.commands("steer")], ["switch to B"])
        self.assertNotIn("running", self.state()["sessions"][sid])
        self.assertEqual(pi_pool.pending_inbox(sid), [])

    def test_followup_runs_after_current_work(self):
        os.environ["FAKE_PI_DELAY"] = "1"
        t, box = self.run_in_thread("--prompt", "hi")
        sid = self.wait_running()
        code, err = self.send("follow_up", sid, "--file", self.write_file("also do C"))
        t.join(20)
        self.assertEqual(code, 0, err)
        self.assertIn("(last: also do C)", box["result"][1])
        self.assertEqual(len(self.commands("follow_up")), 1)

    def test_now_aborts_long_tool_then_sends(self):
        os.environ["FAKE_PI_DELAY"] = "30"
        started = time.time()
        t, box = self.run_in_thread("--prompt", "hi")
        sid = self.wait_running()
        code, err = self.send("steer", sid, "--now", "stop and do D")
        self.assertEqual(code, 0, err)
        # sent as a new prompt after the abort; its tool call takes 30 s again, so abort once it has been sent
        deadline = time.time() + 10
        while len(self.commands("prompt")) < 2 and time.time() < deadline:
            time.sleep(0.05)
        self.assertEqual([c["message"] for c in self.commands("prompt")], ["hi", "stop and do D"])
        self.assertEqual(self.send("abort", sid)[0], 0)
        t.join(20)
        self.assertFalse(t.is_alive())
        self.assertLess(time.time() - started, 20)
        self.assertIn("interrupt×1", box["result"][1])

    def test_abort_stops_run(self):
        os.environ["FAKE_PI_DELAY"] = "30"
        t, box = self.run_in_thread("--prompt", "hi")
        sid = self.wait_running()
        code, err = self.send("abort", sid)
        t.join(20)
        self.assertFalse(t.is_alive())
        self.assertEqual(code, 0, err)
        rc, out, _ = box["result"]
        self.assertEqual(rc, 1)
        self.assertIn("stop=aborted", out)

    def test_message_arriving_at_settle_becomes_new_run(self):
        # a message is already in the inbox and the run ends before the first poll: it is sent as a new prompt
        sid = "11111111-2222-3333-4444-555555555555"
        os.makedirs(pi_pool.SESSION_DIR)
        with open(os.path.join(pi_pool.SESSION_DIR, f"x_{sid}.jsonl"), "w",
                  encoding="utf-8") as f:
            f.write(json.dumps({"type": "session", "id": sid, "cwd": os.getcwd()}) + "\n")
        pi_pool.post_inbox(sid, "follow_up", "late")
        code, out, _ = self.run_pool("--prompt", "hi", "--session", sid)
        self.assertEqual(code, 0)
        self.assertEqual([c["message"] for c in self.commands("prompt")], ["hi", "late"])
        self.assertIn("(last: late)", out)

    def test_dialogs_are_cancelled(self):
        os.environ["FAKE_PI_DIALOG"] = "1"
        code, _, _ = self.run_pool("--prompt", "hi")
        self.assertEqual(code, 0)
        (resp,) = self.commands("extension_ui_response")
        self.assertEqual((resp["id"], resp["cancelled"]), ("dlg-1", True))

    def test_send_to_finished_session_fails(self):
        self.run_pool("--prompt", "hi")
        sid = self.starts()[0]["argv"][5]
        code, err = self.send("steer", sid, "x")
        self.assertEqual(code, 1)
        self.assertIn("is not running", err)
        self.assertEqual(pi_pool.pending_inbox(sid), [])

    def write_file(self, text):
        path = os.path.join(self.tmp.name, "msg.txt")
        with open(path, "w", encoding="utf-8") as f:
            f.write(text)
        return path


class FailureSwitchTest(PoolCase):
    def test_auth_failure_switches_and_marks_account(self):
        os.environ["FAKE_PI_AUTH_FAIL"] = "agent-gamma"
        code, out, err = self.run_pool("--prompt", "hi")
        self.assertEqual(code, 0, err)
        self.assertIn("gamma(login) → main", out)
        self.assertIn("pi-pool account login gamma", err)
        self.assertIn("gamma", self.state()["auth_bad"])
        prompts = self.commands("prompt")
        self.assertEqual(prompts[1]["account"], "agent-main")
        self.assertIn("an expired login", prompts[1]["message"])
        # the next run skips gamma right away
        self.run_pool("--prompt", "again")
        self.assertEqual(self.commands("prompt")[-1]["account"], "agent-main")

    def test_absorb_warns_on_tool_error_streak(self):
        res = pi_pool.RunResult()
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            for _ in range(pi_pool.TOOL_ERROR_WARN):
                pi_pool.absorb(res, {"type": "tool_execution_end", "toolName": "bash", "isError": True}, True)
        self.assertIn(f"{pi_pool.TOOL_ERROR_WARN} tool calls failed in a row", err.getvalue())
        pi_pool.absorb(res, {"type": "tool_execution_end", "toolName": "bash", "isError": False}, True)
        self.assertEqual(res.tool_error_streak, 0)


class BackgroundTest(PoolCase):
    """When pi-async-bash reports background jobs, pi stopping does not end the run; it waits for the results."""

    def bg_log(self):
        return [c["bg"] for c in self.call_log() if "bg" in c]

    def wait_for(self, pred, timeout=10):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if pred():
                return
            time.sleep(0.05)
        self.fail("timed out waiting")

    def test_waits_for_background_job_then_finishes(self):
        os.environ["FAKE_PI_BG"] = "1.3"  # the summary only reports waits of at least 1 s
        code, out, err = self.run_pool("--prompt", "hi")
        self.assertEqual(code, 0, err)
        self.assertIn("background jobs: Background bash · 1 running: bg-0000abcd sleep 300", err)
        self.assertIn("pi stopped; waiting for background jobs before finishing", err)
        self.assertIn("background job result delivered", err)
        self.assertIn("all background jobs finished", err)
        self.assertIn("bg result handled by agent-gamma", out)
        self.assertIn("for background jobs after pi stopped", out)
        self.assertNotIn("were killed", out)
        self.assertEqual(self.bg_log(), ["started", "done"])
        self.assertEqual(len(self.commands("prompt")), 1, "the wake-up comes from the extension, not a prompt from pi-pool")
        self.assertGreaterEqual(self.state()["sessions"].popitem()[1]["runs"][0]["bg_waited_s"], 0)

    def test_abort_while_waiting_kills_background(self):
        os.environ["FAKE_PI_BG"] = "60"
        t, box = self.run_in_thread("--prompt", "hi")
        sid = self.wait_running()
        self.wait_for(lambda: any(c.get("cmd", {}).get("type") == "prompt" for c in self.call_log())
                      and self.bg_log() == ["started"])
        time.sleep(0.8)  # let pi stop and pi-pool start waiting
        code, err = self.send("abort", sid)
        self.assertEqual(code, 0, err)
        t.join(20)
        self.assertFalse(t.is_alive())
        rc, out, run_err = box["result"]
        self.assertEqual(rc, 1)
        self.assertNotIn("results reached the conversation", run_err, "clearing the status at shutdown is not completion")
        self.assertIn("stop=aborted", out)
        self.assertIn("were killed: Background bash · 1 running: bg-0000abcd", out)
        self.assertEqual(self.bg_log(), ["started", "killed"])

    def test_steer_while_waiting_starts_new_run(self):
        os.environ["FAKE_PI_BG"] = "1.5"
        t, box = self.run_in_thread("--prompt", "hi")
        sid = self.wait_running()
        time.sleep(0.6)
        code, err = self.send("steer", sid, "meanwhile do E")
        self.assertEqual(code, 0, err)
        t.join(20)
        rc, out, _ = box["result"]
        self.assertEqual(rc, 0, out)
        self.assertEqual([c["message"] for c in self.commands("prompt")], ["hi", "meanwhile do E"])
        self.assertEqual(self.bg_log(), ["started", "done"])

    def test_quota_switch_tells_next_account_background_was_lost(self):
        os.environ["FAKE_PI_BG"] = "60"
        os.environ["FAKE_PI_FAIL"] = "agent-gamma"
        code, _, err = self.run_pool("--prompt", "hi")
        self.assertEqual(code, 0, err)
        prompts = self.commands("prompt")
        self.assertEqual(prompts[1]["account"], "agent-main")
        self.assertIn("were killed with it and will not report back", prompts[1]["message"])
        self.assertIn("bg-0000abcd sleep 300", prompts[1]["message"])

    def test_queue_returned_at_close_keeps_stdin_open(self):
        # clear_queue recovers a message and starts a new run: the shutdown timer must be cancelled, or a steer in that run cannot get in
        os.environ["FAKE_PI_PENDING_ON_CLEAR"] = "late"
        os.environ["FAKE_PI_DELAY"] = "1.2"
        with patch.object(pi_pool, "CLOSE_GRACE_S", 0.2):
            t, box = self.run_in_thread("--prompt", "hi")
            sid = self.wait_running()
            self.wait_for(lambda: len(self.commands("prompt")) >= 2, timeout=15)
            time.sleep(0.5)  # past the shutdown timer
            code, err = self.send("steer", sid, "late steer")
            t.join(30)
        self.assertEqual(code, 0, err)
        self.assertEqual([c["message"] for c in self.commands("steer")], ["late steer"])
        self.assertIn("(last: late steer)", box["result"][1])

    def test_watchdog_reports_quiet_period(self):
        os.environ["FAKE_PI_DELAY"] = "1.5"
        self.cfg["stall_warn_s"] = 0.4
        code, _, err = self.run_pool("--prompt", "hi")
        self.assertEqual(code, 0)
        self.assertIn("no events for", err)


class AccountTest(PoolCase):
    def setUp(self):
        super().setUp()
        base = self.cfg["base_dir"]
        os.makedirs(os.path.join(base, "extensions"))
        for name, text in (("settings.json", "{}"), ("auth.json", "{}"), ("AGENTS.md", "x")):
            with open(os.path.join(base, name), "w", encoding="utf-8") as f:
                f.write(text)

    def account(self, *args):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            try:
                code = pi_pool.cmd_account(self.cfg, list(args))
            except SystemExit as e:
                code = e.code
        return code, out.getvalue(), err.getvalue()

    def test_add_links_shared_items_logs_in_and_saves(self):
        d = os.path.join(self.tmp.name, "agent-new")
        os.makedirs(d)
        with open(os.path.join(d, "settings.json"), "w", encoding="utf-8") as f:
            f.write("old")
        code, _, err = self.account("add", "new", "--dir", d, "--priority", "5")
        self.assertEqual(code, 0, err)
        base = self.cfg["base_dir"]
        for name in ("settings.json", "extensions", "AGENTS.md"):
            self.assertEqual(os.readlink(os.path.join(d, name)), os.path.join(os.path.realpath(base), name))
        self.assertFalse(os.path.islink(os.path.join(d, "auth.json")), "credentials are not shared")
        self.assertTrue(os.path.isfile(os.path.join(d, "auth.json")), "the fake login wrote credentials")
        self.assertTrue(os.path.isfile(os.path.join(d, "settings.json.bak")))
        with open(pi_pool.CONFIG_PATH, encoding="utf-8") as f:
            saved = pi_pool.normalize_config(json.load(f))
        self.assertEqual(saved["accounts"]["new"], {"dir": d, "priority": 5, "enabled": True})
        self.assertIn("gamma", saved["accounts"], "existing accounts are written to the config too")
        # adding the same account again is refused
        self.assertNotEqual(self.account("add", "new", "--dir", d)[0], 0)

    def test_failed_login_does_not_save(self):
        os.environ["FAKE_PI_LOGIN_FAIL"] = "1"
        code, _, err = self.account("add", "bad", "--dir", os.path.join(self.tmp.name, "agent-bad"))
        self.assertEqual(code, 1)
        self.assertIn("the account was not added", err)
        self.assertFalse(os.path.exists(pi_pool.CONFIG_PATH))

    def test_set_remove_sync_check(self):
        self.assertEqual(self.account("set", "main", "--priority", "1", "--disable")[0], 0)
        self.assertEqual(self.cfg["accounts"]["main"], {"dir": self.cfg["accounts"]["main"]["dir"],
                                                        "priority": 1, "enabled": False})
        self.assertNotIn("main", pi_pool.active_accounts(self.cfg))
        d = os.path.join(self.tmp.name, "agent-s")
        self.assertEqual(self.account("add", "s", "--dir", d, "--no-login")[0], 0)
        with open(os.path.join(self.cfg["base_dir"], "models.json"), "w", encoding="utf-8") as f:
            f.write("{}")
        code, out, _ = self.account("sync", "s")
        self.assertEqual(code, 0)
        self.assertIn("models.json: linked", out)
        self.assertIn("no changes", self.account("sync", "s")[1])
        os.unlink(os.path.join(self.cfg["base_dir"], "models.json"))
        self.assertIn("models.json: dangling link removed", self.account("sync", "s")[1])
        self.assertFalse(os.path.lexists(os.path.join(d, "models.json")))
        code, out, _ = self.account("check", "s")
        self.assertEqual((code, out.split()), (1, ["s", "not_ready"]))
        with pi_pool.locked_state() as st:
            st["auth_bad"]["s"] = {"until": time.time() + 60}
        self.assertEqual(self.account("login", "s")[0], 0)
        self.assertNotIn("s", self.state()["auth_bad"], "a new login clears the expired-login record")
        self.assertEqual(self.account("check", "s")[0], 0)
        self.assertEqual(self.account("remove", "s")[0], 0)
        self.assertNotIn("s", self.cfg["accounts"])
        self.assertTrue(os.path.isdir(d), "remove does not delete the directory")

    def test_doctor_reports_failures(self):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = pi_pool.cmd_doctor(self.cfg, [])
        self.assertEqual(code, 1, out.getvalue())  # none of the test account directories exist
        self.assertIn("[FAIL] account gamma", out.getvalue())
        self.assertIn("pi 0.0.0-fake", out.getvalue())


class StatusTest(PoolCase):
    def test_status_json(self):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            pi_pool.cmd_status(self.cfg, ["--json"])
        data = json.loads(out.getvalue())
        self.assertEqual({a["account"]: a["tier"] for a in data["accounts"]},
                         {"main": "ok", "alpha": "low", "beta": "low", "gamma": "ok"})


if __name__ == "__main__":
    unittest.main()
