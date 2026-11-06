#!/usr/bin/env bash
# Installs this PC as an execution worker for the agent orchestration server, as the current user.
# Generated for one install link: it works once and expires. Nothing here needs sudo or an open port.
#   AGENT_INSTALL_PREFIX     install folder (default ~/.agent-worker)
#   AGENT_INSTALL_NO_SERVICE set to 1 to skip the startup entry and the registration (testing)
#   AGENT_INSTALL_PYTHON     the Python to use (default python3)
#   AGENT_INSTALL_PORT       the worker's local port (default 8096; it listens on 127.0.0.1 only)
# Afterwards `agent-worker` manages it: status, start, stop, restart, update, uninstall, logs.
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
  # One worker per PC: a finished install (or its startup entry) means this must be an update or an uninstall, never a second install.
  local existing=""
  for candidate in "$PREFIX" "$HOME/.agent-worker"; do
    [ -f "$candidate/installed.json" ] && { existing="$candidate"; break; }
  done
  if [ -z "$existing" ]; then
    for entry in "$HOME"/.config/systemd/user/agent-worker-*.service "$HOME"/Library/LaunchAgents/agent-worker-*.plist; do
      [ -e "$entry" ] && { existing="$entry"; break; }
    done
  fi
  if [ -n "$existing" ]; then
    fail "A worker is already installed on this PC ($existing). Update it with: agent-worker update. Remove it with: agent-worker uninstall (then run this command again)."
  fi
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
  "$PREFIX/venv/bin/python" "$PREFIX/agents/worker_ctl.py" --home "$PREFIX" version --compute > "$PREFIX/version"

  # The agent-worker command: a wrapper next to the install and a link on the usual per-user PATH.
  mkdir -p "$PREFIX/bin" "$HOME/.local/bin"
  printf '#!/bin/sh\nexec "%s/venv/bin/python" "%s/agents/worker_ctl.py" --home "%s" "$@"\n' "$PREFIX" "$PREFIX" "$PREFIX" > "$PREFIX/bin/agent-worker"
  chmod 755 "$PREFIX/bin/agent-worker"
  ln -sf "$PREFIX/bin/agent-worker" "$HOME/.local/bin/agent-worker"
  local cli_links="[\"$HOME/.local/bin/agent-worker\"]"

  write_config() {  # write_config <service kind> <unit name>
    printf '{"id": "%s", "slots": %s, "server": "%s", "port": %s, "service": "%s", "unit": "%s", "cli_links": %s}\n' \
      "$NAME" "$SLOTS" "$SERVER" "$PORT" "$1" "$2" "$cli_links" > "$PREFIX/worker.json"
  }
  local run=("$PREFIX/venv/bin/python" -u "$PREFIX/agents/worker_runner.py" --home "$PREFIX" --log "$PREFIX/worker.log")
  local unit="agent-worker-@@NAME_RAW@@"
  if [ "${AGENT_INSTALL_NO_SERVICE:-0}" = 1 ]; then
    write_config process "$unit"
    printf '{"id": "%s", "at": %s, "service": "none"}\n' "$NAME" "$(date +%s)" > "$PREFIX/installed.json"
    echo "Installed in $PREFIX (AGENT_INSTALL_NO_SERVICE=1: no startup entry, not registered)."
    echo "Start it with: agent-worker start    (or: ${run[*]})"
    return 0
  fi

  step service "Starting the worker"
  local kind
  kind="$(install_service "$unit" "${run[@]}")"
  write_config "$kind" "$unit"
  case "$kind" in
    systemd) systemctl --user enable --now "$unit.service" ;;
    launchd) launchctl bootstrap "gui/$(id -u)" "$HOME/Library/LaunchAgents/$unit.plist"; launchctl kickstart "gui/$(id -u)/$unit" ;;
    *) "$PREFIX/bin/agent-worker" start ;;
  esac
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
  printf '{"id": "%s", "at": %s, "service": "%s"}\n' "$NAME" "$(date +%s)" "$kind" > "$PREFIX/installed.json"
  echo
  echo "Done. $NAME is connected and starts again when you log in (or at boot, with systemd lingering)."
  echo "Manage it with: agent-worker status | stop | start | restart | update | uninstall | logs"
  case ":$PATH:" in *":$HOME/.local/bin:"*) ;; *) echo "Note: add $HOME/.local/bin to your PATH to run agent-worker by name, or use $PREFIX/bin/agent-worker" ;; esac
}

# install_service <unit> <command...>: create the per-user startup entry (not started here); prints its kind
install_service() {
  local unit="$1"; shift
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
    echo launchd
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
KillMode=control-group

[Install]
WantedBy=default.target
UNIT
    systemctl --user daemon-reload
    if [ -z "$(loginctl show-user "$USER" -p Linger --value 2>/dev/null | grep -i yes)" ]; then
      echo "Note: it starts at login. To start at boot, before anyone logs in, run once: sudo loginctl enable-linger $USER" >&2
    fi
    echo systemd
  else
    echo "Note: no per-user service manager here, so it runs as a background process and will not restart after a reboot or a crash." >&2
    echo process
  fi
}

main "$@"
