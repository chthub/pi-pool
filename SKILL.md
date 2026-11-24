---
name: pi-pool
description: 让 Claude Code 指挥其他模型干活：把执行类工作通过 pi 派给另一个模型（例如走 ChatGPT/Codex 订阅的 GPT）当 subagent。账号由 pi-pool 按优先级和额度自动选，额度用尽或登录失效时自动换号、在同一会话里续跑，不需要手动管理。用户要求让 GPT/pi/codex/其他模型干活、派给 pi、用 pi 当子代理、查 pi 账号额度、添加或调整 pi 账号时读。触发词：pi-pool、pi 子代理、派给 pi、让 GPT 做、让 codex 做、gpt worker、pi 账号、加账号、换账号、额度轮换、delegate to pi。
---

# pi-pool：让 Claude Code 指挥其他模型干活

分工：Claude 定计划、写任务单、验收、汇报；pi 上的模型执行；选账号、换账号、等后台任务由 `pi-pool` 程序负责。命令是 `pi-pool`，源码和本 skill 在同一个目录（`pi-pool --help` 有完整用法）。

**先读本机规则**：如果 `~/.config/pi-pool/notes.md` 存在（配置放在别处时，是 `$PI_POOL_CONFIG` 同目录的 `notes.md`），先读它。那是这台机器的用户补充规则，与本文件冲突时以它为准。

## 1. 什么时候派

- 用户明确要求让其他模型做，或本会话里说过「执行类工作默认交给 pi」时才派；否则照常自己做。
- 适合派：在大量文件或日志里查找、按清楚的规格写代码和测试、批量机械改动、独立的审查。
- 自己做：定口径和协议、解读结果、向用户汇报、几行就能改完的小事、不可逆操作（提交或取消集群作业、删除或覆盖产物、git push）。

## 2. 派活

```bash
pi-pool run --task-file <任务单> --cwd <仓库根>            # 默认 --size large
pi-pool run --prompt "短问题" --size small -- --tools read,grep,find,ls   # 只读小查询
pi-pool run --session <ID前缀> --prompt "验收命令 X 失败，输出如下：……请修复"
```

- 账号不用管：不加 `--account`，pi-pool 会按优先级、额度、在跑会话数自己选，被拒绝就换号在同一会话里续跑。只有用户点名某个账号时才加 `--account NAME`。
- `--size`：一个问题、看一个文件、几轮就能答完 → `small`（优先用快用完额度的账号）；其余默认 `large`。拿不准不加。
- 任务单写到本会话 scratchpad，模板见本目录的 `assets/task_template.md`。pi 看不到对话，背景要写全。
- `--cwd` 决定 pi 读到哪些 AGENTS.md。项目规则写出处即可，不必复制。续接会话时自动回到第一次的目录。
- pi 默认加载用户的扩展和 skill。只读任务加 `-- --tools read,grep,find,ls`；换模型用 `-- --model <provider/model> --thinking <level>`。
- 预计超过约 5 分钟的任务，用 Bash 的 `run_in_background` 启动，跑完会收到通知，不要轮询。
- 多个互不依赖的任务可以同时派（各自是独立会话，pi-pool 会把它们分散到不同账号）。不要让两个 pi 同时改同一批文件。
- 先读/查、再动手：审查、调研、诊断之后接着做修复或实现时，优先用 `run --session <原会话>` 续接，不要新开会话重读同一批文件。任务单开头写明上一轮的只读等限制是否还适用。

## 3. 运行中

- 不看中间过程。正常运行时后台输出里每轮只有一行进度；出现下面这些行才需要看一眼：
  - `N 没有新事件；工具 X 已运行 …`：可能卡住了；
  - `注意：连续 N 次工具调用出错`；
  - `后台任务：…` / `pi 已停下，等待后台任务完成后继续`：pi 在等自己放到后台的命令（例如等集群作业），pi-pool 不会退出，也没有总时长上限，这是正常状态。
- 改方向、补信息用 `steer`/`followup`，不要 kill 进程：
  ```bash
  pi-pool steer <ID前缀> "改做 X……"          # 当前工具调用跑完后送达
  pi-pool steer <ID前缀> --now "停下，改做 X"  # 先中止当前操作再发
  pi-pool followup <ID前缀> "做完后再做 Y"
  pi-pool abort <ID前缀>                      # 中止；在等后台任务时会连后台任务一起终止
  ```
  消息长就写文件，用 `--file`。返回「已送达」才算发出去；返回 1 说明运行已结束，改用 `run --session`。
- `pi-pool ls` 看最近会话（`运行中(账号)`）；`pi-pool show <ID>` 看已有的回答和每轮用的账号。

## 4. 结果

stdout 摘要只读这些：账号路径（如 `acct1(额度) → acct2`）、exit、stop、轮数、用时、工具调用计数、等后台任务的时长、日志路径，然后是 `---` 和最终回答。原始事件流在摘要给出的日志路径里，排查时才看。

退出码：0 成功；1 pi 出错（看摘要里的错误）；2 参数错误；75 没有可用账号。

- 退出码 75：把 `pi-pool status` 的结果告诉用户，不要自己悄悄接手大任务。
- 摘要或 stderr 出现「登录失效，需要 pi-pool account login NAME」：程序已经换号继续了；告诉用户在终端运行 `! pi-pool account login NAME` 重新登录（device code，需要用户本人操作）。

## 5. 验收

pi 的最终回答只是线索。

- 自己复跑验收命令，看真实输出。
- `git diff` 逐块看改动，超出任务范围就退回。改过的代码照常跑项目的 lint 和类型检查。
- 它报的数字和清单用命令复核。
- 不通过就 `--session` 追问，把失败命令和输出原样发回。两三轮还不过就自己接手。
- 向用户汇报时说明哪些部分由 pi 做、用了哪个账号、自己复核了什么。

## 6. 账号（用户要求时才动）

```bash
pi-pool status                         # 各账号优先级、5h/7d 剩余、分级、在跑会话数
pi-pool account list                   # 配置里的账号
pi-pool account add NAME [--priority N]   # 建目录、软链共享设置、device code 登录、写配置
pi-pool account set NAME --priority N | --enable | --disable
pi-pool account login NAME             # 登录失效后重新登录
pi-pool doctor                         # 这台机器能不能跑：pi、各账号登录、skill 安装
```

- 优先级数字小的先用。
- `account add` 和 `account login` 要用户在浏览器里完成登录：给出命令，让用户用 `! ` 前缀自己运行，不要自己跑。
- 新机器部署：克隆仓库后运行 `install.sh`，再按 README「部署到新机器」一节逐个 `account add`。

## 7. 限制

- **没有沙箱**：pi 以用户权限运行，bash 能做任何事。任务单的「禁止」一节要写明不能改的文件和不能跑的命令（提交/取消作业、删除产物、git push）。需要隔离时用 git worktree 给它一份副本。
- pi 的 bash 命令跑在当前机器上，和会话共用 CPU 与内存；重计算不要放进任务里。
- 换账号后，第一轮要在新账号上重新计算整段上下文，消耗更多额度；pi-pool 已优先沿用上次的账号。
