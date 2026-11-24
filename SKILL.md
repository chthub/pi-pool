---
name: pi-pool
description: Let Claude Code delegate work to other models. Hands execution work through pi to another model (for example GPT on a ChatGPT/Codex subscription) as a subagent. pi-pool chooses accounts by priority and quota, and on quota exhaustion or an expired login it switches accounts and continues the same session — no manual account management. Read when the user asks to have GPT/pi/codex/another model do work, delegate to pi, use pi as a subagent, check pi account quota, or add or adjust pi accounts. Triggers: pi-pool, pi subagent, delegate to pi, let GPT do it, codex worker, gpt worker, pi account, add account, switch account, quota rotation, 派给 pi, 让 GPT 做, 让 codex 做, pi 子代理, pi 账号, 加账号, 换账号, 额度轮换.
---

# pi-pool: let Claude Code delegate work to other models

Roles: Claude plans, writes the task brief, reviews the result, and reports. The model in pi executes. The `pi-pool` program chooses and switches accounts and waits for background jobs. The command is `pi-pool`. Its source lives in the same directory as this skill; run `pi-pool --help` for full usage.

## 1. When to delegate

- Delegate only when the user explicitly asks for another model to do the work, or has said in this session that execution work goes to pi by default. Otherwise do the work yourself.
- Good to delegate: searching many files or logs, writing code and tests to a clear spec, bulk mechanical edits, independent reviews.
- Keep for yourself: defining scope and conventions, interpreting results, reporting to the user, changes of a few lines, and anything irreversible (submitting or cancelling cluster jobs, deleting or overwriting outputs, git push).
- Never delegate work that needs aesthetic judgment (page styling, visual design, layout, color). Claude does that.

## 2. Dispatching

```bash
pi-pool run --task-file <brief> --cwd <repo root>            # default --size large
pi-pool run --prompt "short question" --size small -- --tools read,grep,find,ls   # read-only quick query
pi-pool run --session <ID prefix> --prompt "Acceptance command X failed, output: … please fix"
```

- Leave accounts alone: without `--account`, pi-pool picks by priority, quota, and running sessions, and on refusal switches accounts and continues the same session. Add `--account NAME` only when the user names an account.
- `--size`: use `small` for one question, one file, or a few turns (it prefers accounts that are nearly out of quota); otherwise leave the default, `large`. If unsure, omit it.
- Write the brief to this session's scratchpad. The template is `assets/task_template.md` in this directory. pi cannot see this conversation, so give it the full background.
- `--cwd` decides which AGENTS.md files pi reads. Cite project rules by location rather than copying them. A resumed session automatically returns to its first directory.
- pi loads the user's extensions and skills by default. For read-only tasks add `-- --tools read,grep,find,ls`.
- Model choice: for writing code, searching, and analysis, use the account's default model and add no flags. For planning decisions and reasoning on complex tasks, add `-- --model openai-codex/gpt-6.2-astra --thinking high`; do not use it for ordinary implementation.
- Start tasks expected to take more than about 5 minutes with Bash `run_in_background`. You will be notified when they finish; do not poll.
- Independent tasks can run at the same time. Each one is its own session, and pi-pool spreads them across accounts. Never let two pi runs edit the same files.
- Read first, then act: when a fix or implementation follows a review, investigation, or diagnosis, continue the original session with `run --session <original>` rather than starting a new one that rereads the same files. Say at the top of the brief whether the earlier restrictions (for example read-only) still apply.

## 3. While it runs

- Do not watch the run. Normally the background output gets one progress line per turn. Look only when one of these lines appears:
  - `… 没有新事件；工具 X 已运行 …`: nothing has happened for a while and the run may be stuck.
  - `注意：连续 N 次工具调用出错`: N tool calls in a row have failed.
  - `后台任务：…` or `pi 已停下，等待后台任务完成后继续`: pi is waiting for a command it moved to the background (for example a cluster job). This is normal. pi-pool will not exit, and there is no overall time limit.
- To change direction or add information, use `steer`/`followup`; do not kill the process:
  ```bash
  pi-pool steer <ID prefix> "Do X instead…"          # delivered after the current tool call
  pi-pool steer <ID prefix> --now "Stop, do X"       # aborts the current operation first
  pi-pool followup <ID prefix> "Afterwards, do Y"
  pi-pool abort <ID prefix>                          # stops the run; while waiting, also kills background jobs
  ```
  For long messages, write them to a file and pass `--file`. A message counts as sent only when the command reports `已送达` (delivered). Exit code 1 means the run has ended; use `run --session` instead.
- `pi-pool ls` lists recent sessions; running ones show as `运行中(account)`. `pi-pool show <ID>` shows the answer so far and the account used for each run.

## 4. Results

From the stdout summary, read only: the account path (for example `acct1(额度) → acct2`, where `额度` marks a quota refusal), exit, stop, turns, time, tool-call counts, time spent waiting for background jobs, the log path, and then the final answer after `---`. The raw event stream is at the log path; open it only for debugging.

Exit codes: 0 success; 1 pi failed (see the error in the summary); 2 bad arguments; 75 no usable account.

- Exit code 75: tell the user what `pi-pool status` shows. Do not quietly take over a large task yourself.
- If the summary or stderr says `登录失效，需要 pi-pool account login NAME` (login expired), pi-pool has already switched accounts and continued. Ask the user to run `! pi-pool account login NAME` in their terminal; device-code login needs the user.

## 5. Review

pi's final answer is only a lead.

- Rerun the acceptance commands yourself and read the real output.
- Go through `git diff` hunk by hunk and send back anything outside the task. Run the project's lint and type checks on changed code as usual.
- Check every number and list it reports with a command.
- If it does not pass, follow up with `--session` and paste the failing command and its output verbatim. If two or three rounds do not fix it, take over.
- When reporting to the user, say which parts pi did, which account it used, and what you verified yourself.

## 6. Accounts (only when the user asks)

```bash
pi-pool status                            # priority, 5h/7d remaining, tier, running sessions per account
pi-pool account list                      # accounts in the configuration
pi-pool account add NAME [--priority N]   # create the directory, link shared settings, log in by device code, save
pi-pool account set NAME --priority N | --enable | --disable
pi-pool account login NAME                # log in again after the login expired
pi-pool doctor                            # can this machine run it: pi, account logins, skill install
```

- Lower priority numbers are used first.
- `account add` and `account login` need the user to finish the login in a browser. Give the user the command to run with the `! ` prefix; do not run it yourself.
- New machine: clone the repository, run `install.sh`, then follow "Set up a new machine" in the README to add each account.

## 7. Limits

- **No sandbox.** pi runs with the user's permissions, and its bash can do anything. The "Do not" section of the brief must list files that must not change and commands that must not run (submitting or cancelling jobs, deleting outputs, git push). Use a git worktree when isolation is needed.
- pi's bash runs on the current machine and shares CPU and memory with this session. Keep heavy computation out of delegated tasks.
- After an account switch, the first turn on the new account recomputes the whole context and costs more quota. pi-pool therefore keeps the previous account whenever it can.
