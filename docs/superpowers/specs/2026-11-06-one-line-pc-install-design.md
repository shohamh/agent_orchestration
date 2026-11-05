# One-line PC install with an outbound-only relay

Date: 2026-11-06. Status: awaiting approval.

## Intent

Registering an execution PC in the Fleet tab is a four-part manual procedure today: copy the
repository, build and carry a wheelhouse, move a token by hand, set environment variables, then
type the PC's endpoint back into a form. It also needs an open inbound port on the PC, which on
Windows means an administrator and a firewall rule.

The goal is the shortest path that still works in an offline network: fill in a short form on the
board, get **one command**, paste it into a terminal on the new PC, and the PC appears online in
the Fleet tab. The PC reaches only this server, plus the public pip and npm registries.

## Goals

1. Registering a PC is a form plus one pasted command, with variants for Windows PowerShell,
   Windows cmd and Linux/macOS (bash). Each variant says plainly whether to run it elevated.
2. The form is filled and validated first. Only then does the server create the one-time install
   link and show the commands. Nothing is generated from an invalid or empty form.
3. The PC needs no inbound port, firewall rule, router change or administrator/sudo. It connects
   outward to the server and keeps that connection open (long polling). Everything the
   coordinator does to a PC today (submit, poll, cancel, health, capacity, capabilities) keeps
   working unchanged on top of this.
4. The server provides everything the PC needs from it: the install script, the worker source,
   a token, the PC's settings (name, slots, limits, engines). The PC gets Python packages from
   PyPI and the agent adapters from npm.
5. Design size: a few hundred PCs (about 300) on one coordinator.
6. Direct URL mode (the coordinator calls a reachable PC) keeps working for existing PCs and as
   an advanced option.

## Non-goals

- Installing Python or Node. The installer checks for them and stops with the exact install
  command for the OS if they are missing (my call, see below).
- UPnP, NAT-PMP, hole punching or any firewall change on the PC.
- Native engine placement on remote PCs (unchanged: every engine reaches a PC through its
  execution service).
- More than a few hundred PCs; a thousand would need the coordinator's catalog and placement
  scans reworked beyond health pushes.
- Uninstall tooling beyond printing the exact commands to remove the service and folder.

## Anti-goals (a failure even if it technically works)

- A token or install code typed into, or shown by, the board after creation; a token visible in a
  shared terminal, process list or the repository.
- A link that works twice, never expires, or registers a PC under a name the form did not allow.
- A PC that is "registered" but never actually connected, with no visible reason.
- Anything that needs the user to open a port or edit a firewall.

## Constraints from the existing code

- Every coordinator path (`distributed.py`, `workers.py`) treats `worker['url']` as an HTTP base
  and sends `Authorization: Bearer <token>` from `workers.local.env`. A dropped connection or
  timeout is read as "worker offline" and an HTTP 4xx/5xx as a failure, so the relay must signal
  an unreachable PC by closing the connection, never with an error status.
- Job bodies can reach 190 MB (`worker_service` client_max_size, workspace transfers), so relayed
  bodies must stream, never be buffered or put inside JSON.
- The board is the only entry the PCs can reach. Its login middleware (`board/auth.py`,
  `PUBLIC = {"/login", "/logout"}`) must let `/install/*` and `/relay/*` through; these routes
  authenticate with the install code or the PC's token instead.
- The flow host (`agents/flow_host.py`) is a thread-per-connection server on 127.0.0.1:8095; held
  long polls would need one thread each, so the relay must be an asyncio service.
- `fleet_registry.save_worker` already owns validation, revisions and the file lock; the install
  completion must go through it. `workers.local.env` holds tokens and is ignored by git.
- The Fleet tab is `board/static/fleet.js` with its `instructions()` setup text; the new form
  replaces that text for new PCs.
- Offline terminology: say "offline", never "airgapped".

## Design

### Parts

1. **Relay service** (`agents/fleet_relay.py`, asyncio/aiohttp), started and supervised by the
   flow host like the execution worker. Listens on 127.0.0.1:8097. Two faces:
   - Coordinator side: `ANY /w/<worker-id>/<path>` behaves like the PC's execution service. The
     registry URL of a relayed PC is `http://127.0.0.1:8097/w/<worker-id>`.
   - PC side: `/relay/...` (long poll, response, body transfer, status push) and `/install/...`.
2. **Connector** (`agents/relay_connector.py`) runs on the PC next to the execution service. It
   forwards relayed calls to the service on 127.0.0.1 and pushes the PC's status.
3. **Installer scripts** generated per install (`install.ps1`, `install.sh`), plus the worker
   source bundle built from `agents/` (no `.venv`, `node_modules`, `runtime`, tests or secrets).
4. **Install records** in `.flow-state/installs/`: pending, completed, expired.
5. **Board and flow host routes** and the new form in `fleet.js`.

### The form (all fields required unless marked) and validation

Name (the worker ID: letters, digits, dash, underscore, 1-64 chars, unique), operating system
(Windows, Linux, macOS), engines (at least one), pool (existing or new label), tags (optional),
slots (1-100, default 1), CPU and RAM limit (1-100, default 90), and "address this PC uses to
reach this server" (pre-filled from the page's origin; refused if the host is
localhost/127.0.0.1/::1 or not http/https).

The server revalidates every field with the same rules as `fleet_worker()` and refuses a name
that is registered or has an unexpired pending install. "Create install link" is disabled until
the form is valid; errors show next to their field. The result view shows the three commands,
each with a Copy button, the expiry time, an "Elevation" line, a live status (Waiting for the
PC, Downloading, Connected, Failed with the reason), and a Cancel link action. The status
reads the install record.

### The commands

- PowerShell: `irm 'http://<server>/install/<code>/install.ps1' | iex`
- cmd: `powershell -NoProfile -ExecutionPolicy Bypass -Command "irm 'http://<server>/install/<code>/install.ps1' | iex"`
- Linux/macOS: `curl -fsSL 'http://<server>/install/<code>/install.sh' | bash`

Elevation line, same for all three: "Run as a normal user. Do not use Administrator or sudo." (A
service for this user is installed; nothing needs to be opened.)

The code is 256 random bits, single use, valid 30 minutes (my call). The script and the bundle
may be downloaded repeatedly while the install is pending (retries); completion burns the code.

### What the installer does on the PC (no elevation)

1. Check Python 3.10+ and Node 18+ (and npm); if missing, stop with the exact install command
   for the OS (winget / brew / the distribution's package manager).
2. Install into the user's profile: `%LOCALAPPDATA%\AgentWorker` on Windows, `~/.agent-worker`
   elsewhere. Download the bundle from the server, create a venv, `pip install -r
   requirements.txt` from PyPI, `npm ci --prefix agents` from the npm registry.
3. Generate the PC's token locally (`secrets.token_urlsafe(32)`), write it to a file readable only
   by this user (NTFS ACL / mode 600), and write the service settings (worker ID, slots, server
   address) to a config file.
4. Install a per-user startup entry and start it: Windows Scheduled Task at logon for this user;
   Linux `systemd --user` unit (with a printed note to run `loginctl enable-linger` if it should
   start before login); macOS LaunchAgent. If the platform mechanism is unavailable, fall back to
   a background process and say so. The entry runs a launcher that starts the execution service
   (bound to 127.0.0.1, token required) and the connector.
5. Complete the registration by calling `POST /install/<code>/complete` with the token, hostname,
   OS and versions. Print success or the failure reason and exit non-zero on failure.

The scripts accept `AGENT_INSTALL_PREFIX` and `AGENT_INSTALL_NO_SERVICE=1` so they can be tested
without touching the real profile or startup entries.

### Completion on the server

`complete` verifies the code (unexpired, unused), writes the token to `workers.local.env` under
`FLOW_WORKER_TOKEN_<NAME>`, saves the worker through `save_worker(create=True)` with the form's
settings and URL `http://127.0.0.1:8097/w/<name>`, marks the install complete, and burns the code.
The PC then connects; the first status push shows it online. If the connector never connects
within two minutes of completion the install record shows "registered, not connected" with the
connector log hint, and the Fleet card shows the usual offline reason.

### The relay protocol

- PC to server, every call authenticated by `Authorization: Bearer <PC token>` (compared to the
  registry token with `hmac.compare_digest`), worker ID in the path.
- `GET /relay/<id>/next?wait=25` is the long poll. It returns 200 with a JSON list of pending
  calls (each: id, method, path, headers, body length) or 204 after 25 s. The connector reopens it
  at once.
- For each call the connector fetches the request body with `GET /relay/<id>/calls/<cid>/body`
  (streamed), makes the call to its local service, and returns the response with
  `PUT /relay/<id>/calls/<cid>/response` (status and headers in headers; body streamed).
- `POST /relay/<id>/status` every 5 s with the PC's `/health` payload (capacity, active jobs,
  inventory timestamps). The relay serves `GET /w/<id>/health` from this cache when it is under
  15 s old; this keeps catalog polling at O(1) per PC without relay round trips.
- A PC is online if it polled within 40 s. When a coordinator call arrives for an offline PC, or
  the PC does not pick it up within 30 s, or the connector's response never arrives, the relay
  aborts the coordinator's connection (no status code): the coordinator already treats that as
  "offline".
- The connector reconnects with backoff (1 s up to 30 s). A relay restart loses in-flight calls
  (the coordinator sees them as offline) and PCs reconnect by themselves.
- The coordinator's 0.2 s job poll is slowed to 1 s for relayed PCs, and status events for a job
  can be returned in one response (they already are batched by cursor).

### Security

- The install code is the only credential on the unauthenticated `/install/<code>/...` routes;
  it is 256-bit, single-use, expires, and is compared in constant time. Wrong codes get one slow
  generic 404 and are rate limited per source address.
- `/relay/*` and `/install/*` skip the board login; every other board route is unchanged. The
  board proxies them to the relay, streaming bodies, with a size cap of 200 MB.
- The PC's execution service binds to 127.0.0.1 and still requires its token. Nothing listens on
  the network on the PC.
- The script is delivered over whatever the server address is (plain HTTP on a LAN); the form
  shows a warning when the address is `http://` and not a private or tailnet address, and
  recommends HTTPS. The token never leaves the PC after being sent once at completion.
- Install records and the server's token file are readable only by the coordinator's account and
  git-ignored. `workers.local.yaml*` and the install state directory are added to `.gitignore`.

### Failure behaviour

- Expired or used link: the script prints "this link expired, create a new one in the Fleet tab".
- Missing Python/Node, no PyPI/npm access, port 8096 taken: stop with the specific reason and the
  command to fix it; the install record shows the failing step (the script reports each step to
  `/install/<code>/progress`).
- Re-running the command with the same link after a partial install resumes it (idempotent steps).
- Two installs of the same name: the second is refused at form validation.

## What is left to the builder

- Exact JSON shapes, file layout, and the PowerShell/bash script text.
- How to supervise the relay inside the flow host and its port setting (`FLOW_RELAY_PORT`).
- The Fleet tab layout and wording, within the behaviour above.
- Tests: unit tests for the relay, install records and form validation; script lint (`bash -n`,
  PowerShell parser); an end-to-end test on one machine with a real relay, connector and
  execution service in a temporary prefix with `AGENT_INSTALL_NO_SERVICE=1`; the browser test for
  the form (also at phone width). The existing fleet, distributed and placement suites must
  still pass unchanged.
- Documentation: README of agents, `docs/workers-and-context.md`, root README; the old manual
  steps move to an "Advanced: direct URL" section.

## Calls I made (change any of these)

1. Python and Node must already be on the PC; the installer detects them and tells the user how to
   install them, instead of installing them itself.
2. Link validity 30 minutes, single use at completion; poll window 25 s, offline after 40 s; PC
   status every 5 s; relay port 8097.
3. The PC generates its own token; the server never shows it.
4. A per-user install on every OS, and no elevation anywhere.
5. Existing PCs are not migrated; they keep direct URL mode until edited.
6. Large bodies stream through three short exchanges instead of being embedded in the long poll.
7. The relay runs as its own process under the flow host, not inside the board's process.
