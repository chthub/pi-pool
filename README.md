# pi-pool

Let Claude Code delegate work to other models.

Claude Code plans, writes the task brief, and reviews the result. Execution goes to another model running in [pi](https://www.npmjs.com/package/@earendil-works/pi-coding-agent), for example GPT through a ChatGPT/Codex subscription. pi-pool is the dispatch layer between them:

- It picks an account by priority and remaining quota across several subscription accounts.
- When an account runs out of quota or its login expires, it switches accounts and continues **the same session**, so no context is lost.
- You can steer, queue follow-ups, or abort a running session.
- When pi moves long commands to the background (with [pi-async-bash](https://www.npmjs.com/package/@chthub/pi-async-bash)), it waits for their results instead of shutting pi down and killing them.

This directory is also a Claude Code skill (`SKILL.md`, `assets/`). The skill tells Claude when to delegate, how to write a task brief, and how to review the result. The installer links it to `~/.claude/skills/pi-pool`.

pi-pool is a single Python file (3.9+, standard library only). Quota lookup currently supports `openai-codex` (ChatGPT subscription) accounts only.

## Install

```bash
npm install -g @earendil-works/pi-coding-agent   # pi
git clone https://github.com/chthub/pi-pool.git && cd pi-pool
bash install.sh --with-async-bash
```

`install.sh` is safe to re-run. It:
1. links `~/.local/bin/pi-pool` to `pi_pool.py` (an existing file that points elsewhere is left alone);
2. links `~/.claude/skills/pi-pool` to this directory (an existing real directory is moved to `~/.claude/pi-pool-skill.bak-<time>`);
3. with `--with-async-bash`, runs `pi install npm:@chthub/pi-async-bash` (use `--with-async-bash=<path>` to install a local checkout instead);
4. writes the default configuration to `~/.config/pi-pool/config.json` if it does not exist;
5. runs `pi-pool doctor`.

## Set up a new machine

```bash
pi                                    # configure pi once in ~/.pi/agent (settings, extensions), then quit
pi-pool account add main --dir ~/.pi/agent --priority 90
pi-pool account add work              # once per account: creates ~/.pi/agent-work and logs in by device code
pi-pool account add team --priority 40
pi-pool doctor
```

- Log in on each machine separately. Do not copy `auth.json` between machines: once one machine refreshes the OAuth token, the copy on the other machine may stop working.
- Each machine keeps its own runtime data (`~/.pi/pool/`) and cooldown records. Quota is queried live, so nothing needs to be synced.
- To move to another machine, copy `~/.config/pi-pool/config.json`. Then, for each account, run `pi-pool account sync NAME` (creates the directory and its links) and `pi-pool account login NAME`.

## Using it from Claude Code

Once installed, ask Claude something like "hand this to GPT" or "have pi look into …". The skill loads automatically. Claude then:
1. writes a task brief to its scratchpad (template: `assets/task_template.md`);
2. runs `pi-pool run --task-file … --cwd …` in the background;
3. reads the summary when the run finishes;
4. reruns the acceptance commands and reviews the diff itself, and if the result does not pass, sends the failure back with `--session`.

## Usage

```bash
pi-pool run (--prompt TEXT | --task-file FILE) [--account auto|NAME] [--size large|small]
            [--session ID] [--cwd DIR] [--max-switch N] [--refresh] [-- extra pi args]
pi-pool steer SESSION (TEXT | --file FILE) [--now] [--wait S]
pi-pool followup SESSION (TEXT | --file FILE) [--wait S]
pi-pool abort SESSION
pi-pool show SESSION
pi-pool ls [N]

pi-pool status [--json] [--refresh]
pi-pool pick [--size large|small] [--session ID]
pi-pool account list | add NAME [--dir DIR] [--priority N] [--no-login] | login NAME
                | set NAME [--priority N] [--enable|--disable] [--dir DIR] | remove NAME
                | sync [NAME...] | check [NAME...]
pi-pool config [--write]
pi-pool doctor
```

See `pi-pool --help` for details.

## Accounts and configuration

`~/.config/pi-pool/config.json` (set `PI_POOL_CONFIG` to use another path):

```json
{"base_dir": "~/.pi/agent", "provider": "openai-codex", "low_pct": 5, "quota_ttl_s": 120,
 "stall_warn_s": 600, "auth_cooldown_s": 604800,
 "accounts": {"work": {"dir": "~/.pi/agent-work", "priority": 50, "enabled": true},
              "main": {"dir": "~/.pi/agent",      "priority": 90, "enabled": true}}}
```

- Each account is a pi agent directory, selected through `PI_CODING_AGENT_DIR`. Each directory holds its own `auth.json`.
- `base_dir` is where shared settings come from. `account add` symlinks every entry of `base_dir` except `auth.json` into the new directory, so extensions, skills and settings are maintained in one place. If the new directory already has a real file with the same name, that file is renamed to `.bak` first.
- `account sync` links entries that have appeared in `base_dir` since, and removes dangling links that point to files deleted from `base_dir`.
- `account add` and `account login` run `pi auth login <provider> --no-browser` (device code), so you have to complete them yourself in a terminal. The account is written to the configuration only after `pi auth check` reports ready.
- `account remove` only removes the account from the configuration. Its directory and credentials are kept.
- If there is no configuration file and `~/.pi/agent` exists, that directory is used as the only account, named `main`.
- The older format is still read: `"accounts": {"main": "~/.pi/agent"}` plus `"last_resort": [...]`. Accounts listed in `last_resort` get priority 90 and all others get 50.

## How an account is chosen

Accounts are ordered by:
1. `priority`, lower first;
2. the account the session used last, which comes first within its priority because the prompt cache only works within one account;
3. tier (see the table below);
4. the number of sessions already running on the account, fewer first, so parallel tasks spread across accounts;
5. remaining quota, more first.

Disabled accounts are never chosen.

| Tier | Condition | large task | small task |
|---|---|---|---|
| ok | every window has at least `low_pct` left | used | used, after low |
| low | some window has less than `low_pct` left, but requests are still allowed | not used | preferred |
| blocked | the server refuses requests, a local cooldown is active, or the login has expired | not used | not used |
| unknown | quota could not be read | fallback | fallback |

Quota comes from `https://chatgpt.com/backend-api/wham/usage`, the same data the codex CLI status line shows. Only `auth.json` is read; credentials are never refreshed by the query. Results are cached for `quota_ttl_s` seconds, and the cache is dropped after any account switch.

## Switching accounts without losing context

- All accounts share one session directory, `~/.pi/pool/sessions` (passed as `--session-dir`), so the session ID stays the same across accounts.
- pi looks up sessions by working directory. pi-pool records the cwd of each session's first run and returns to it when resuming.
- **Quota refusal**: the last assistant message has `stopReason=error` and the error text looks like a quota error (usage limit, rate limit, 429, quota). The cooldown lasts until the time given by "Try again in ~N min" in the error; failing that, until the reset time of the exhausted window; failing that, one hour.
- **Expired login**: the error text looks like 401, unauthorized, invalid_grant, refresh token, or log in again. The account is skipped for `auth_cooldown_s`, or until `account login` succeeds.
- After a switch, pi-pool sends only a short continuation prompt if the task already reached the session; otherwise it resends the original task. Background jobs still running in the old process are killed with that process, and the continuation prompt names those commands.

## Waiting for background jobs

From version 0.1.6, pi-async-bash reports background bash jobs in RPC mode through `setStatus` under the key `async-bash`. The status covers commands that are still running and finished jobs whose results have not reached the conversation yet. Foreground bash never appears in it. pi-pool prints each status change to stderr as a `background jobs: …` line.

`agent_settled` alone does not tell you that pi is finished. It only means pi has no retries, compaction, or queued messages left to process; it knows nothing about background jobs held by extensions. If pi-pool closed stdin at that point, pi would shut down and the extension would cancel every background job. So, while the `async-bash` status is set, pi-pool keeps stdin open and waits. When a job finishes, the extension wakes the agent for a new run, which pi-pool handles as usual. pi-pool shuts pi down only once pi has stopped and no background jobs remain.

- A `steer` or `followup` that arrives while waiting is sent as a new prompt.
- An `abort` that arrives while waiting shuts pi down, which cancels the background jobs, and the run ends with `stop=aborted`.
- There is no overall time limit.

Without pi-async-bash, or with a version that does not report this status, pi-pool shuts pi down as soon as pi stops.

## Progress messages

In normal operation stderr gets one progress line per turn, so the dispatching agent does not need to watch the run. One extra line is written only in these cases:
- nothing has happened for `stall_warn_s` seconds (default 600): how long the run has been quiet and which tool is running, or, while waiting for background jobs, how long it has been waiting;
- five tool calls in a row have failed;
- the background-job status has changed.

## Messages to a running session

pi runs as `pi --mode rpc`, and tasks reach it as `prompt` commands on stdin. `steer`, `followup` and `abort` write their message to `~/.pi/pool/inbox/<session>/`. The running pi-pool picks it up within 0.5 s, forwards it to pi, and writes back an acknowledgement. Each of these commands waits up to `--wait` seconds (default 20); if nobody picks the message up, it is withdrawn and the command exits 1.

| Command | RPC command sent to pi | Takes effect |
|---|---|---|
| `steer` | `steer` | after the current tool call finishes, before the next model call; a running command is not interrupted |
| `steer --now` | `abort`, then a new `prompt` | immediately; the current operation, including a running bash command, is aborted |
| `followup` | `follow_up` | after pi finishes what it is doing |
| `abort` | `abort` | immediately; the run ends with `stop=aborted` |

- A message that arrives just after a run ends, while pi is still up, is sent as a new prompt. While shutting down, pi-pool also calls `clear_queue` to recover queued messages pi never delivered, and starts a new run with them.
- Messages not yet delivered when a quota refusal triggers an account switch stay in the inbox. The run on the next account forwards them.
- Extension dialogs (select/confirm/input/editor) are always answered with `cancelled`, as if there were no UI.
- Each session is recorded in `state.json` when its run starts (pid, host, account). A killed run can therefore still be resumed with `--session`.
- Each run records the caller's session ID in `runs[].caller`, taken from whichever of `CLAUDE_CODE_SESSION_ID`, `PI_SESSION_ID` and `CODEX_THREAD_ID` is set. This lets you trace a child session back to its parent. These variables are removed from the environment of the child pi.

## Output and data

- stdout: a summary (account path, exit, stop, turns, time, tokens, tool-call counts, injected messages, time spent waiting for background jobs, background jobs killed at exit, log path), then `---` and the final answer.
- stderr: the session, account and log path at start, one progress line per turn, and the extra lines described above.
- `~/.pi/pool/` (set `PI_POOL_DIR` to move it):
  - `sessions/`: sessions;
  - `logs/`: raw JSONL event stream and stderr of every run;
  - `inbox/`: messages waiting to be forwarded;
  - `quota.json`: quota cache;
  - `state.json`: session records, cooldowns, and expired logins.

Exit codes: 0 success; 1 pi failed (for steer and friends: not delivered; for doctor: a FAIL was reported); 2 bad arguments; 75 no usable account.

## Limitations

- There is no sandbox. pi runs with your user's permissions. Use a git worktree when you need isolation.
- Quota refusals are detected using a real error message (`Codex error: The usage limit has been reached`). Expired-login detection is based on common wording and has not been checked against a real error yet. If an error is not recognized, look at `errorMessage` in the run log and adjust `QUOTA_ERROR_RE` / `AUTH_ERROR_RE`.
- `/wham/usage` is not a public API, so its fields may change. Parsing tolerates missing or malformed fields; an account whose quota cannot be read is treated as unknown.
- Used with pi 1.5.0 and 1.6.0.

## Tests

```bash
python3 -m unittest -q test_pi_pool
```

The tests replace pi with `fake_pi.py`, which implements a subset of RPC mode, the background-job status, and `auth login/check`. Quota data is written by hand, and the tests make no network requests.

## License

MIT
