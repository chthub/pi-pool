#!/usr/bin/env bash
# 安装 pi-pool：命令软链到 ~/.local/bin，Claude skill 软链到 ~/.claude/skills/pi-pool，
# 两者都指回本目录，改源码立即生效。可重复运行。
#
#   bash install.sh                        # 命令 + skill + 配置文件 + 自检
#   bash install.sh --with-async-bash      # 另外给 pi 装 pi-async-bash（npm 版）
#   bash install.sh --with-async-bash=/path/to/pi-async-bash   # 装本地检出
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC="$HERE/pi_pool.py"
BIN="${PI_POOL_BIN_DIR:-$HOME/.local/bin}"
SKILLS="${PI_POOL_SKILLS_DIR:-$HOME/.claude/skills}"
ASYNC_BASH=""
for arg in "$@"; do
  case "$arg" in
    --with-async-bash) ASYNC_BASH="npm:@chthub/pi-async-bash" ;;
    --with-async-bash=*) ASYNC_BASH="${arg#*=}" ;;
    -h|--help) sed -n '2,8p' "$0"; exit 0 ;;
    *) echo "未知参数: $arg" >&2; exit 2 ;;
  esac
done

command -v python3 >/dev/null || { echo "需要 python3（3.9+）" >&2; exit 1; }
python3 -c 'import sys; sys.exit(sys.version_info < (3, 9))' || { echo "需要 Python 3.9+" >&2; exit 1; }
command -v pi >/dev/null || echo "注意: PATH 上没有 pi（npm install -g @earendil-works/pi-coding-agent）"

# 1. 命令。已存在且不是我们的软链就别动，免得盖掉同名的别的东西
mkdir -p "$BIN"
chmod +x "$SRC"
if [ -e "$BIN/pi-pool" ] || [ -L "$BIN/pi-pool" ]; then
  if [ -L "$BIN/pi-pool" ] && [ "$(readlink -f "$BIN/pi-pool")" = "$(readlink -f "$SRC")" ]; then
    echo "已安装: $BIN/pi-pool -> $SRC"
  else
    echo "跳过: $BIN/pi-pool 已存在且指向别处（$(readlink -f "$BIN/pi-pool" || true)），没有覆盖"
  fi
else
  ln -s "$SRC" "$BIN/pi-pool"
  echo "已安装: $BIN/pi-pool -> $SRC"
fi
case ":$PATH:" in
  *":$BIN:"*) ;;
  *) echo "注意: $BIN 不在 PATH 里，加一句 export PATH=\"$BIN:\$PATH\"" ;;
esac

# 2. Claude skill。旧的真实目录移到 skills 目录外备份（放在里面会被当成第二个同名 skill）
mkdir -p "$SKILLS"
LINK="$SKILLS/pi-pool"
if [ -L "$LINK" ]; then
  if [ "$(readlink -f "$LINK")" = "$HERE" ]; then
    echo "已安装: skill $LINK -> $HERE"
  else
    echo "跳过: $LINK 指向别处（$(readlink -f "$LINK" || true)），没有覆盖"
  fi
else
  if [ -e "$LINK" ]; then
    BAK="$(dirname "$SKILLS")/pi-pool-skill.bak-$(date +%Y%m%d%H%M%S)"
    mv "$LINK" "$BAK"
    echo "旧的 skill 目录已移到 $BAK"
  fi
  ln -s "$HERE" "$LINK"
  echo "已安装: skill $LINK -> $HERE"
fi

# 3. pi-async-bash（可选）：让 pi 能把长命令放到后台，pi-pool 会等它们完成
if [ -n "$ASYNC_BASH" ]; then
  if command -v pi >/dev/null; then
    pi install "$ASYNC_BASH"
  else
    echo "跳过 pi-async-bash：PATH 上没有 pi"
  fi
fi

# 4. 配置文件：不存在就把当前生效的默认值写出来，之后用 pi-pool account ... 管理
if [ ! -f "${PI_POOL_CONFIG:-$HOME/.config/pi-pool/config.json}" ]; then
  python3 "$SRC" config --write
fi

echo
echo "== pi-pool doctor"
python3 "$SRC" doctor || echo "（doctor 有 FAIL 项，见上；常见的是还没添加账号：pi-pool account add NAME）"
