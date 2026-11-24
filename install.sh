#!/usr/bin/env bash
# Install pi-pool: link the command into ~/.local/bin and the Claude skill into
# ~/.claude/skills/pi-pool. Both point back to this directory, so source edits take
# effect immediately. Safe to re-run.
#
#   bash install.sh                        # command + skill + config file + self-check
#   bash install.sh --with-async-bash      # also install pi-async-bash for pi (npm)
#   bash install.sh --with-async-bash=/path/to/pi-async-bash   # install a local checkout
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
    -h|--help) sed -n '2,9p' "$0"; exit 0 ;;
    *) echo "unknown argument: $arg" >&2; exit 2 ;;
  esac
done

command -v python3 >/dev/null || { echo "python3 (3.9+) is required" >&2; exit 1; }
python3 -c 'import sys; sys.exit(sys.version_info < (3, 9))' || { echo "Python 3.9+ is required" >&2; exit 1; }
command -v pi >/dev/null || echo "note: pi is not on PATH (npm install -g @earendil-works/pi-coding-agent)"

# 1. Command. Leave an existing file alone unless it is our own link, so nothing else gets overwritten.
mkdir -p "$BIN"
chmod +x "$SRC"
if [ -e "$BIN/pi-pool" ] || [ -L "$BIN/pi-pool" ]; then
  if [ -L "$BIN/pi-pool" ] && [ "$(readlink -f "$BIN/pi-pool")" = "$(readlink -f "$SRC")" ]; then
    echo "installed: $BIN/pi-pool -> $SRC"
  else
    echo "skipped: $BIN/pi-pool already exists and points elsewhere ($(readlink -f "$BIN/pi-pool" || true)); not overwritten"
  fi
else
  ln -s "$SRC" "$BIN/pi-pool"
  echo "installed: $BIN/pi-pool -> $SRC"
fi
case ":$PATH:" in
  *":$BIN:"*) ;;
  *) echo "note: $BIN is not on PATH; add export PATH=\"$BIN:\$PATH\"" ;;
esac

# 2. Claude skill. Move an old real directory out of the skills directory (inside it, it would load as a second skill with the same name).
mkdir -p "$SKILLS"
LINK="$SKILLS/pi-pool"
if [ -L "$LINK" ]; then
  if [ "$(readlink -f "$LINK")" = "$HERE" ]; then
    echo "installed: skill $LINK -> $HERE"
  else
    echo "skipped: $LINK points elsewhere ($(readlink -f "$LINK" || true)); not overwritten"
  fi
else
  if [ -e "$LINK" ]; then
    BAK="$(dirname "$SKILLS")/pi-pool-skill.bak-$(date +%Y%m%d%H%M%S)"
    mv "$LINK" "$BAK"
    echo "moved the old skill directory to $BAK"
  fi
  ln -s "$HERE" "$LINK"
  echo "installed: skill $LINK -> $HERE"
fi

# 3. pi-async-bash (optional): lets pi move long commands to the background; pi-pool waits for them.
if [ -n "$ASYNC_BASH" ]; then
  if command -v pi >/dev/null; then
    pi install "$ASYNC_BASH"
  else
    echo "skipped pi-async-bash: pi is not on PATH"
  fi
fi

# 4. Config file: if missing, write the effective defaults; manage it later with pi-pool account ...
if [ ! -f "${PI_POOL_CONFIG:-$HOME/.config/pi-pool/config.json}" ]; then
  python3 "$SRC" config --write
fi

echo
echo "== pi-pool doctor"
python3 "$SRC" doctor || echo "(doctor reported FAIL items above; usually no account has been added yet: pi-pool account add NAME)"
