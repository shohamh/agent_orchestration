#!/usr/bin/env bash
# Installs this PC as an execution worker for the agent orchestration server, as the current user.
# Generated for one install link: it works once and expires. Nothing here needs sudo or an open port.
#   AGENT_INSTALL_PREFIX     install folder (default ~/.agent-worker)
#   AGENT_INSTALL_NO_SERVICE set to 1 to skip the startup entry and the registration (testing)
#   AGENT_INSTALL_PYTHON     the Python to use (default python3)
#   AGENT_INSTALL_PORT       the worker's local port (default 8096; it listens on 127.0.0.1 only)
main() {
  set -euo pipefail
  local SERVER=@@SERVER@@ CODE=@@CODE@@ NAME=@@NAME@@ SLOTS=@@SLOTS@@ TARGET_OS=@@OS@@
  local PREFIX="${AGENT_INSTALL_PREFIX:-$HOME/.agent-worker}" STEP=check PORT="${AGENT_INSTALL_PORT:-8096}"
  local PYTHON="${AGENT_INSTALL_PYTHON:-python3}"

  report() {  # report <step> [state] [message]: tell the server where the install is (best effort)
    local message="${3:-}"; message="${message//\\/ }"; message="${message//\"/ }"
    curl -fsS -m 10 -X POST -H 'Content-Type: application/json' \
      -d "{\"step\":\"$1\",\"state\":\"${2:-running}\",\"message\":\"$message\"}" "$SERVER/install/$CODE/progress" >/dev/null 2>&1 </dev/null || true
  }
  step() { STEP="$1"; echo "==> $2"; report "$1"; }
  fail() { echo "ERROR: $*" >&2; report "$STEP" failed "$*"; exit 1; }

  step check "Checking this PC"
  case "$(uname -s)" in
    Linux) local detected=linux ;;
    Darwin) local detected=macos ;;
    *) fail "This script is for Linux and macOS; use the PowerShell command on Windows" ;;
  esac
  [ "$detected" = "$TARGET_OS" ] || echo "Note: the form said $TARGET_OS but this PC is $detected; continuing"
  if [ "$(id -u)" = 0 ]; then fail "Run this as a normal user, not root or sudo: the worker installs for one user"; fi
  command -v curl >/dev/null || fail "curl is required"
  command -v tar >/dev/null || fail "tar is required"
  local hint="install Python 3.10+ (for example: sudo apt install python3 python3-venv, or: brew install python)"
  command -v "$PYTHON" >/dev/null || fail "Python 3.10 or newer is required; $hint"
  "$PYTHON" -c 'import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)' || fail "Python 3.10 or newer is required; $hint"
  "$PYTHON" -c 'import venv, ensurepip' 2>/dev/null || fail "Python's venv module is missing; install it (for example: sudo apt install python3-venv)"
  command -v node >/dev/null && command -v npm >/dev/null || fail "Node.js 18+ and npm are required (for example: sudo apt install nodejs npm, or: brew install node)"
  node -e 'process.exit(Number(process.versions.node.split(".")[0]) >= 18 ? 0 : 1)' || fail "Node.js 18 or newer is required"

  step download "Downloading the worker from $SERVER"
  mkdir -p "$PREFIX"
  umask 077
  curl -fsSL "$SERVER/install/$CODE/bundle.tgz" -o "$PREFIX/bundle.tgz" </dev/null || fail "Could not download the worker from $SERVER (is the link expired or the server unreachable?)"
  tar -xzf "$PREFIX/bundle.tgz" -C "$PREFIX" || fail "The downloaded worker is damaged"
  rm -f "$PREFIX/bundle.tgz"

  step python "Installing Python packages (from PyPI)"
  "$PYTHON" -m venv "$PREFIX/venv" || fail "Could not create the Python environment"
  PIP_DISABLE_PIP_VERSION_CHECK=1 "$PREFIX/venv/bin/python" -m pip install --quiet -r "$PREFIX/agents/requirements-worker.txt" </dev/null || fail "pip could not install the worker's packages (is PyPI reachable?)"

  step npm "Installing the coding agent adapters (from npm)"
  npm ci --prefix "$PREFIX/agents" --no-audit --no-fund --loglevel=error </dev/null || fail "npm could not install the agent adapters (is the npm registry reachable?)"

  step token "Creating this PC's token"
  local token
  token="$("$PREFIX/venv/bin/python" -c 'import secrets; print(secrets.token_urlsafe(32))')"
  ( umask 077; printf '%s' "$token" > "$PREFIX/token" )
  chmod 600 "$PREFIX/token"
  printf '{"id": "%s", "slots": %s, "server": "%s", "port": %s}\n' "$NAME" "$SLOTS" "$SERVER" "$PORT" > "$PREFIX/worker.json"

  local run=("$PREFIX/venv/bin/python" -u "$PREFIX/agents/worker_runner.py" --home "$PREFIX" --log "$PREFIX/worker.log")
  if [ "${AGENT_INSTALL_NO_SERVICE:-0}" = 1 ]; then
    echo "Installed in $PREFIX (AGENT_INSTALL_NO_SERVICE=1: no startup entry, not registered)."
    echo "Start it with: ${run[*]}"
    return 0
  fi

  step service "Starting the worker"
  start_service "${run[@]}"
  local waited=0
  until curl -fsS -m 3 -H "Authorization: Bearer $token" "http://127.0.0.1:$PORT/health" >/dev/null 2>&1 </dev/null; do
    waited=$((waited + 1)); [ "$waited" -le 40 ] || fail "The worker did not start; see $PREFIX/worker.log"
    sleep 1
  done

  step register "Registering this PC with the server"
  local hostname_value; hostname_value="$(hostname 2>/dev/null || echo unknown)"
  local reply
  reply="$(curl -sS -m 30 -X POST -H 'Content-Type: application/json' \
    -d "{\"token\":\"$token\",\"hostname\":\"$hostname_value\"}" "$SERVER/install/$CODE/complete" </dev/null)" || fail "Could not reach the server to register"
  case "$reply" in *'"ok": true'*|*'"ok":true'*) ;; *) fail "The server refused the registration: $reply" ;; esac

  step connect "Waiting for the connection"
  waited=0
  until curl -fsS -m 5 "$SERVER/install/$CODE/state" </dev/null 2>/dev/null | grep -q '"connected": *true'; do
    waited=$((waited + 1)); [ "$waited" -le 60 ] || fail "Registered, but the worker is not connected yet; see $PREFIX/worker.log"
    sleep 1
  done
  echo
  echo "Done. $NAME is connected and will start again when you log in."
  echo "Remove it later with: $PREFIX/uninstall.sh"
}

start_service() {
  local prefix="${AGENT_INSTALL_PREFIX:-$HOME/.agent-worker}"
  local unit="agent-worker-@@NAME_RAW@@"
  cat > "$prefix/uninstall.sh" <<UNINSTALL
#!/usr/bin/env bash
systemctl --user disable --now $unit.service 2>/dev/null || true
launchctl bootout gui/\$(id -u) "\$HOME/Library/LaunchAgents/$unit.plist" 2>/dev/null || true
rm -f "\$HOME/.config/systemd/user/$unit.service" "\$HOME/Library/LaunchAgents/$unit.plist"
rm -rf "$prefix"
UNINSTALL
  chmod 700 "$prefix/uninstall.sh"
  if [ "$(uname -s)" = Darwin ]; then
    mkdir -p "$HOME/Library/LaunchAgents"
    local args=""; for a in "$@"; do args="$args<string>$a</string>"; done
    cat > "$HOME/Library/LaunchAgents/$unit.plist" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
<key>Label</key><string>$unit</string>
<key>ProgramArguments</key><array>$args</array>
<key>RunAtLoad</key><true/><key>KeepAlive</key><true/>
</dict></plist>
PLIST
    launchctl bootout "gui/$(id -u)" "$HOME/Library/LaunchAgents/$unit.plist" 2>/dev/null || true
    launchctl bootstrap "gui/$(id -u)" "$HOME/Library/LaunchAgents/$unit.plist"
  elif command -v systemctl >/dev/null && systemctl --user show-environment >/dev/null 2>&1; then
    mkdir -p "$HOME/.config/systemd/user"
    local exec_line=""; for a in "$@"; do exec_line="$exec_line \"$a\""; done
    cat > "$HOME/.config/systemd/user/$unit.service" <<UNIT
[Unit]
Description=Agent orchestration execution worker (@@NAME_RAW@@)
After=network-online.target

[Service]
ExecStart=$exec_line
Restart=always
RestartSec=5

[Install]
WantedBy=default.target
UNIT
    systemctl --user daemon-reload
    systemctl --user enable --now "$unit.service"
    echo "Note: to keep it running when nobody is logged in, run once: sudo loginctl enable-linger $USER"
  else
    echo "Note: no per-user service manager here, so it runs in the background and will not restart after a reboot."
    nohup "$@" >/dev/null 2>&1 </dev/null &
  fi
}

main "$@"
