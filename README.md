# pi-pool

让 Claude Code 指挥其他模型干活。

Claude Code 负责定计划、写任务单、验收；执行交给 [pi](https://www.npmjs.com/package/@earendil-works/pi-coding-agent) 上的另一个模型，比如走 ChatGPT/Codex 订阅的 GPT。pi-pool 是两者之间的调度层：
- 在多个订阅账号之间按优先级和剩余额度选号；
- 某个账号额度用尽或登录失效时，自动换号，在同一个会话里续跑，上下文不丢；
- 运行中可以插话（steer）、追加任务（followup）、中止（abort）；
- pi 把长命令放到后台时（配合 [pi-async-bash](https://www.npmjs.com/package/@chthub/pi-async-bash)），等后台结果送达再结束，不会提前退出把任务杀掉。

本目录同时是一个 Claude Code skill（`SKILL.md`、`assets/`），告诉 Claude 什么时候派活、怎么写任务单、怎么验收。安装脚本把它挂到 `~/.claude/skills/pi-pool`。

单文件 Python（3.9+，只用标准库）。额度查询目前只支持 `openai-codex`（ChatGPT 订阅）账号。

## 安装

```bash
npm install -g @earendil-works/pi-coding-agent   # pi
git clone https://github.com/chthub/pi-pool.git && cd pi-pool
bash install.sh --with-async-bash
```

`install.sh` 可以重复运行，它会做这几件事：
1. `~/.local/bin/pi-pool` 软链到 `pi_pool.py`（已存在且指向别处时不覆盖）；
2. `~/.claude/skills/pi-pool` 软链到本目录（原来同名的真实目录会移到 `~/.claude/pi-pool-skill.bak-<时间>`）；
3. 加了 `--with-async-bash` 时运行 `pi install npm:@chthub/pi-async-bash`；也可以写成 `--with-async-bash=<本地路径>`；
4. 没有配置文件时写出默认配置 `~/.config/pi-pool/config.json`；
5. 运行 `pi-pool doctor` 自检。

## 部署到新机器

```bash
pi                                    # 先在 ~/.pi/agent 里把 pi 配好（设置、扩展），退出
pi-pool account add main --dir ~/.pi/agent --priority 90
pi-pool account add work              # 每个账号一次：建 ~/.pi/agent-work，device code 登录
pi-pool account add team --priority 40
pi-pool doctor
```

- 每台机器单独登录，不要复制 `auth.json`：OAuth 刷新令牌在一台机器上刷新后，另一台上的副本可能失效。
- 运行数据（`~/.pi/pool/`）和冷却记录是每台机器自己的；额度每次实时查询，不需要同步。
- 要迁移的只有配置文件 `~/.config/pi-pool/config.json`，以及可选的 `notes.md`（见下文）。复制过去后，对每个账号运行 `pi-pool account sync NAME`（建目录、补软链）和 `pi-pool account login NAME`。

## 让 Claude Code 用起来

装好后直接对 Claude 说「把这个派给 GPT 做」「让 pi 去查……」即可，skill 会被自动加载。Claude 会这样做：
1. 把任务单写到 scratchpad（模板见 `assets/task_template.md`）；
2. 在后台运行 `pi-pool run --task-file … --cwd …`；
3. 收到完成通知后读摘要；
4. 自己复跑验收命令、逐块看 diff，不通过就用 `--session` 追问。

本机特有的规则写在 `~/.config/pi-pool/notes.md`（不在仓库里），比如偏好的模型参数、哪些活不派、集群上的注意事项。skill 会先读它，两者冲突时以它为准。

## 用法

```bash
pi-pool run (--prompt TEXT | --task-file FILE) [--account auto|NAME] [--size large|small]
            [--session ID] [--cwd DIR] [--max-switch N] [--refresh] [-- 其他 pi 参数]
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

`pi-pool --help` 有完整说明。

## 账号与配置

`~/.config/pi-pool/config.json`（环境变量 `PI_POOL_CONFIG` 可改路径）：

```json
{"base_dir": "~/.pi/agent", "provider": "openai-codex", "low_pct": 5, "quota_ttl_s": 120,
 "stall_warn_s": 600, "auth_cooldown_s": 604800,
 "accounts": {"work": {"dir": "~/.pi/agent-work", "priority": 50, "enabled": true},
              "main": {"dir": "~/.pi/agent",      "priority": 90, "enabled": true}}}
```

- 每个账号是一个 pi agent 目录，由 `PI_CODING_AGENT_DIR` 指定，每个目录有自己的一份 `auth.json`。
- `base_dir` 是共享设置的来源。`account add` 建新目录时，把 `base_dir` 里除 `auth.json` 以外的每一项都软链进去，这样扩展、skill、设置只需维护一份；新目录里已有的同名真实文件会先改名成 `.bak`。
- `account sync` 为 `base_dir` 里新出现的项补上软链，并删掉指向 `base_dir` 中已删除文件的失效软链。
- `account add` 和 `account login` 运行 `pi auth login <provider> --no-browser`（device code 登录），需要本人在终端里操作。登录成功（`pi auth check` 为 ready）后才写入配置。
- `account remove` 只从配置里移除，目录和凭证保留。
- 没有配置文件时，如果 `~/.pi/agent` 存在，就把它当作唯一账号 main。
- 也能读旧格式：`"accounts": {"main": "~/.pi/agent"}` 加 `"last_resort": [...]`。`last_resort` 里的账号换算成优先级 90，其余为 50。

## 选号规则

依次比较下面几项：
1. `priority`：数字小的先用；
2. 会话上次用的账号排在同优先级的第一位，因为提示缓存只在同一账号内有效；
3. 分级（见下表）；
4. 正在运行的会话少的先用，让并行任务分散到不同账号；
5. 剩余额度多的先用。

停用的账号不参与选号。

| 分级 | 条件 | large 任务 | small 任务 |
|---|---|---|---|
| ok | 各窗口剩余都 >= `low_pct` | 用 | 用，排在 low 之后 |
| low | 任一窗口剩余 < `low_pct`，服务端仍允许请求 | 不用 | 优先用 |
| blocked | 服务端不允许请求、本机冷却中、或登录失效 | 不用 | 不用 |
| unknown | 额度没查到 | 兜底 | 兜底 |

额度来自 `https://chatgpt.com/backend-api/wham/usage`，和 codex CLI 状态栏显示的是同一份数据。查询时只读 `auth.json`，不刷新凭证。结果缓存 `quota_ttl_s` 秒，换号后立即失效。

## 换账号怎么做到不丢上下文

- 所有账号共用会话目录 `~/.pi/pool/sessions`（通过 `--session-dir` 指定），会话 ID 不变。
- pi 按工作目录查找会话，所以 pi-pool 记下每个会话第一次的 cwd，续接时自动回到那里。
- **额度被拒**：最后一条 assistant 消息 `stopReason=error`，且错误文本像额度错误（usage limit、rate limit、429、quota）。冷却时间依次取：错误里的「Try again in ~N min」，已耗尽窗口的重置时间，都没有就 1 小时。
- **登录失效**：错误文本像 401、unauthorized、invalid_grant、refresh token、log in again。在 `auth_cooldown_s` 时间内，或 `account login` 成功之前，不再用这个账号。
- 换号后：如果这一轮的任务已经写进会话，就只发续接提示，否则重发原任务。旧进程里还没完成的后台任务会随进程终止，续接提示里会写明是哪些命令。

## 等待后台任务

pi-async-bash（0.1.6 起）在 RPC 模式下用 `setStatus`（键 `async-bash`）报告后台 bash 任务：正在运行的命令，以及已经完成、但结果还没送进对话的任务。前台 bash 不会出现在里面。pi-pool 读到后，在 stderr 写一行 `后台任务：…`。

只看 `agent_settled` 不能判断 pi 是否真的做完了。这个事件只表示 pi 没有重试、压缩或排队消息要处理，不管扩展里还在跑的后台任务。这时如果关掉 stdin，pi 退出，扩展会把后台任务全部终止。所以收到 `agent_settled` 时，只要 `async-bash` 状态还在，pi-pool 就不关 stdin，而是继续等待。任务完成后，扩展会唤醒 agent 开始新一轮，pi-pool 照常处理，直到 pi 停下、并且没有后台任务为止。

- 等待期间收到 `steer`/`followup`：作为新一轮指令发出。
- 等待期间收到 `abort`：关闭 pi，后台任务随之终止，运行以 `stop=aborted` 结束。
- 没有总时长上限。

没有装 pi-async-bash，或者版本太旧没有这个状态时，pi 停下就退出。

## 运行中的提示

正常运行时，stderr 每轮只写一行进度。只有下面几种情况会额外写一行，调度方平时不需要看中间过程：
- 超过 `stall_warn_s`（默认 600 秒）没有任何事件：写出静默了多久，以及正在运行的工具；在等后台任务时，写已经等了多久；
- 连续 5 次工具调用出错；
- 后台任务状态变化。

## 给运行中的会话发消息

pi 以 `pi --mode rpc` 常驻运行，任务通过 stdin 的 `prompt` 命令发进去。`steer`、`followup`、`abort` 先把消息写进 `~/.pi/pool/inbox/<会话>/`，正在运行的 pi-pool 每 0.5 秒取一次并转给 pi，再写回送达确认。这三个命令最多等 `--wait` 秒（默认 20），没人取走就撤回消息并返回 1。

| 命令 | 转给 pi 的 RPC 命令 | 什么时候生效 |
|---|---|---|
| `steer` | `steer` | 当前这一步的工具调用执行完后、下一次调用模型前；不打断正在跑的命令 |
| `steer --now` | `abort`，停下后发新的 `prompt` | 立即中止当前操作（包括正在跑的 bash 命令） |
| `followup` | `follow_up` | pi 把手上的事做完之后 |
| `abort` | `abort` | 立即中止，运行以 `stop=aborted` 结束 |

- 运行刚结束、pi 还没退出时到达的消息，作为新一轮 `prompt` 发出。收尾时用 `clear_queue` 取回 pi 里没送达的排队消息，同样开新一轮。
- 额度被拒换号时，没送达的消息留在收件箱，换号后的运行接着转发。
- 扩展弹出的对话框（select/confirm/input/editor）一律回 `cancelled`，等同于没有界面。
- 会话在开跑时就登记进 `state.json`（含 pid、主机、账号），运行被 kill 后也能用 `--session` 找回。
- 每次运行在 `runs[].caller` 里记下调用方的会话 ID，取自 `CLAUDE_CODE_SESSION_ID`、`PI_SESSION_ID`、`CODEX_THREAD_ID` 中存在的那个，便于事后把子会话关联回父会话。启动子 pi 时会去掉这些变量。

## 输出与数据

- stdout：摘要（账号路径、exit、stop、轮数、用时、token、工具调用计数、插话计数、等后台任务的时长、退出时被终止的后台任务、日志路径），`---` 之后是最终回答。
- stderr：开始时的 session、账号、日志路径，每轮一行进度，以及上一节说的提示。
- `~/.pi/pool/`（`PI_POOL_DIR` 可改路径）：
  - `sessions/`：会话；
  - `logs/`：每次运行的原始 JSONL 事件流和 stderr；
  - `inbox/`：待转发的消息；
  - `quota.json`：额度缓存；
  - `state.json`：会话记录、冷却与登录失效记录。

退出码：0 成功；1 pi 出错（steer 等命令：没送达；doctor：有 FAIL）；2 参数错误；75 没有可用账号。

## 限制

- 没有沙箱，pi 以用户权限运行。需要隔离时自己准备 git worktree。
- 额度被拒的识别规则用真实报错核对过（`Codex error: The usage limit has been reached`）。登录失效的识别规则是按常见措辞写的，还没有用真实报错核对。遇到没识别出来的错误时，看日志里的 `errorMessage`，再调整 `QUOTA_ERROR_RE` / `AUTH_ERROR_RE`。
- `/wham/usage` 不是公开 API，字段可能变化。解析时对缺失和异常字段做了容错，查不到时这个账号按 unknown 处理。
- 在 pi 1.5.0 和 1.6.0 上用过。

## 测试

```bash
python3 -m unittest -q test_pi_pool
```

测试用 `fake_pi.py` 代替 pi，它实现了 RPC 模式的一个子集、后台任务状态，以及 `auth login/check`。额度数据是手写的，测试不发任何网络请求。

## License

MIT
