# Graphical workflow builder implementation plan

**Goal:** Create, edit, validate, save and run the existing portable workflows visually in the
Agent Board, with Codex, OpenCode 2 and the other supported agents, program steps, dependencies,
worker placement and human review.

**Architecture:** The builder edits the existing portable workflow definition; Hatchet, Prefect
and Trigger.dev remain execution adapters. The board supplies an accessible graph editor and
step forms. The flow host owns parsing, semantic validation, versioned drafts and explicit
activation, so every client obeys the same rules. The existing YAML remains the portable source.

**Tech stack:** Existing Python `flowkit` and flow host, aiohttp board, browser JavaScript and
SVG/CSS. Bundle any dependencies locally; the builder must work without a CDN or internet access.

**Existing design:** `docs/superpowers/specs/2026-11-03-portable-flows-design.md`,
`flows/README.md`, `docs/workers-and-context.md`.

## Decisions and implementation choices

- **Engine scope (user decision):** Choose one orchestration engine per run. Per-step worker
  selection chooses a PC, not an engine. Mixing engines within one run is outside this plan;
  the addendum records what that separate project would require.
- **Devices (user decision):** Provide a touch-friendly graph and step forms on phones, along
  with mouse and keyboard support. On a narrow phone, editing through a list and step form must
  remain possible even if drag precision is poor.
- **Model:** YAML remains editable and portable; switching between graph and YAML must not silently
  discard any supported field. Exact comment and formatting preservation needs a deliberate
  implementation choice: use a locally bundled round-trip YAML parser, or retain the original
  source and block visual changes that cannot be merged without loss. Do not claim PyYAML alone
  preserves comments, anchors or formatting.
- **Human review:** Agent permissions and questions continue to use each node's existing chat.
  `review: true` requires the user to mark that agent step done. No automatic permission approval.
- **Activation:** Saving/editing is not permission to disrupt running work. The user explicitly
  wants confirmation before an interrupting Trigger.dev reload. Include Hatchet and Prefect
  workers in the impact report too: the current supervisor restarts both on a flow file change.

## Experience

1. Open **Flows** and choose an existing definition or a starter template. Start with the existing
   `multi-agent-design` as the example: a brief, Codex and OpenCode 2 in parallel, and a join.
2. Add a program or agent step; choose agent, prompt/command, timeout, retries, review behavior,
   worker/pool/tags and optional environment setup. Edit flow name, description and scalar inputs.
3. Connect a step to its prerequisites by dragging a connector or using an accessible **Add
   dependency** control. Select a node to edit its properties. Make parallel branches and fan-in
   visible. An auto-layout button and zoom/pan make large graphs manageable; manual positions are
   optional view metadata, not execution dependencies.
4. Insert a reference to a flow input or a direct prerequisite's output from a picker; show the
   actual template syntax. Diagnose missing/deleted steps, invalid references, duplicate IDs and
   cycles with messages on the affected node and in an error list.
5. Switch between **Graph** and **YAML**. Invalid YAML stays in the editor and never destroys the
   last valid draft. A graph edit preserves environment configuration, worker selectors, review,
   timeout, retries and all other fields supported by the existing parser.
6. Save a draft; inspect what must be activated on each backend, what is active and what may be
   interrupted. Choose to defer activation or explicitly confirm the described interruption. After
   activation, start a run by choosing an engine and entering inputs/placement. Open the existing
   board card, native run graph, step chat, approval controls and restart controls.

## Data and safety contracts

- The semantic document is the existing `name`, `description`, `input`, `environment`, `worker`
  and `steps` mapping. Do not write generated `setup_environment` back as a second user step; it
  is derived from `environment`. Graph edges correspond exactly to `needs`.
- Positions and viewport use a separate optional, versioned layout sidecar, keyed by stable step
  IDs. Missing or stale layouts get a deterministic auto-layout. Layout-only edits never trigger
  engine registration or change the semantic definition fingerprint.
- Editing is a transaction against a revision/hash: atomic write, reject stale saves with a
  conflict containing both revisions, and let the user reload or explicitly merge. No silent
  overwrites between browser tabs or file edits.
- Separate draft storage from immutable activated snapshots. Launches and replay use the exact
  activated definition and associated assets/fingerprint. A save, external file change or layout
  update cannot mutate an in-flight run's definition or silently activate an engine. A new active
  version applies to new runs; existing runs finish against their pinned version.
- The current `flow_host.py` watches `flows/*.yaml`, regenerates Trigger code, and restarts flow
  workers immediately. Replace that side effect before exposing visual Save: watch/import external
  file edits as drafts, and make worker restart/code generation an explicit activation operation.
  Workers and Trigger generation must load the activated snapshot rather than the mutable draft.
  Preserve existing workflows through a documented, one-time migration, without restarting active
  runs during migration. Define rollback if activation fails partway through.
- Before activation, the server checks the affected workers and active runs across all boards,
  not just the current tab. Present known run IDs/statuses and which workers will reload. Recheck
  immediately before a disruptive apply; if state cannot be determined, say so and offer to defer.
  Enforce the confirmation server-side, including API and external-file paths; a browser-only
  dialog is insufficient. Distinguish saving a draft from activating it. Report per-engine success,
  pending, unavailable and failure; do not call an engine active merely because a file was written.
- Never include tokens, credential values or environment secrets in flow definitions, UI state,
  graph metadata, logs or previews. Existing token-env references remain references. Retain the
  existing board authentication and explicit agent permission prompts.
- Native-engine behavior remains visible: Hatchet's declared DAG, Prefect task dependencies, and
  Trigger.dev local development task hierarchy. Do not promise identical graph presentation or
  Trigger production execution: the current flow executor needs local CLIs on the dev worker.
- Offline operation includes editor assets, parser libraries and transfer instructions. Update
  the offline bundle only as a separately verified deliverable; test from locally loaded assets.

## Delivery tasks

### 1. Canonical editable document and validation

**Files:** `agents/flowkit/definition.py`, `agents/flowkit/template.py`, new graph-document module
under `agents/flowkit/`, `agents/tests/test_flowkit.py` and focused tests.

- [ ] Add a parse/validate/serialize interface that preserves all currently supported fields and
  avoids exposing the generated environment node as duplicated source. Represent validation
  errors with paths/step IDs while retaining readable YAML-editor errors.
- [ ] Validate both `needs` and template references: unknown input, absent or non-prerequisite
  output, deleted step, duplicate IDs and cycle. Keep shell quoting behavior in the runtime.
- [ ] Define stable step IDs and safe rename/delete handling: update dependencies and references
  atomically or require explicit user resolution; never silently change a prompt or command.
- [ ] Verify YAML -> document -> YAML -> document semantic equality on all `flows/*.yaml`, plus
  environment, all worker selectors, review, retry, timeout, Unicode, empty values and external
  edits. Test comment/anchor preservation or the explicit loss-prevention fallback.

### 2. Drafts, revisions and activation

**Files:** `agents/flow_host.py`, `agents/flowkit/definition.py`, `agents/flowkit/trigger_codegen.py`,
`agents/flow_worker_hatchet.py`, `agents/flow_worker_prefect.py`, `board/flows_client.py`,
`board/server.py`, `agents/tests/`, `board/test_*.py`.

- [ ] Introduce saved drafts, layout sidecars and immutable active snapshots with content revisions.
  Add authenticated read, validate, save-with-expected-revision, activation-preview and activate
  operations; protect old direct-save and external-file watcher paths too.
- [ ] Inventory active affected runs/workers per engine and recheck at commit time. Return an
  explicit unknown status when an engine is unavailable. Defer by default; require an explicit
  confirmation scoped to the exact revision and impact before any disruptive reload. Do not
  implement confirmation by silently cancelling runs.
- [ ] Generate and register only approved active snapshots. Preserve the old active version if
  an engine's activation fails; expose per-engine state and retry. Make idempotent retries and
  partial success unambiguous. Ensure a layout-only save has zero engine side effects.
- [ ] Cover two simultaneous editors, external file edits, malformed YAML, unavailable engine,
  active run appearing between preview and apply, user cancellation, partial activation, restart,
  replay and recovery after host restart. Prove no worker is restarted by draft/validation calls.

### 3. Graph editor and step inspector

**Files:** `board/static/index.html`, new focused `board/static/flow-builder.js` and CSS,
`board/static/chat.browser.test.cjs` or a separate browser test.

- [ ] Reuse drawing conventions from `layoutDag` / `renderDag` without coupling edit state to
  the run-view state. Implement add/select/move/connect/delete, auto-layout, zoom/pan and a
  keyboard/touch-accessible dependency editor. Preserve scrolling and pagination in run views.
- [ ] Add structured properties for every supported field, including agent model choices where
  supported by the existing runtime, review toggle, inputs, workers, environment/context and
  templates. Unsupported fields remain visible/editable in YAML and are never dropped.
- [ ] Implement Graph/YAML synchronization with explicit invalid/dirty states, validation errors,
  draft-save conflict resolution and activation preview. Use text-safe DOM rendering, not raw HTML.
- [ ] Browser-test create/edit/delete, parallel branches and fan-in, reference picker, cycle
  errors, unsaved navigation, failed save, stale revision, long labels, phone viewport, keyboard
  editing and dark/light themes. Assert no restart or activation on draft saves.

### 4. Run and cross-engine verification

**Files:** `board/server.py`, `board/static/`, `board/smoke_test.py` or focused integration tests,
`agents/tests/`, `flows/README.md`, `board/README.md`, offline docs/bundle when in scope.

- [ ] From the builder, choose Hatchet, Prefect or Trigger.dev local, input values and execution
  workers. Make unavailable engines and unsupported Trigger production explicit before launch.
- [ ] Create one graphical flow with the existing `demo` agent and a program step; assert the
  same dependency semantics, outputs and independent branches on all three real local engines.
  Check native-engine views and board graph; test error, cancellation, human review and replay.
  Run real Codex/OpenCode 2 separately when logins and approvals are available; report those as
  real-agent results rather than treating the simulated demo as proof of model integration.
- [ ] Verify a multi-agent flow with `review: true` waits for the user. Confirm existing runs keep
  their snapshot when a new draft/version is saved; test the explicit interruption consent path.
- [ ] Update instructions with first-run setup, mobile/keyboard controls, validation errors,
  engine limitations, active-version/rollback behavior and offline asset packaging. Document the
  YAML escape hatch and what the visual editor does or does not round-trip.

## Mixed-engine addendum (only if requested)

If one run must dispatch individual nodes to different orchestration engines, the current native
workflow model does not provide that. Design a separate durable coordinator with per-node engine
selection, cross-engine identity, event delivery, input/output transfer, timeouts, failure and
cancellation propagation, retries/idempotency, human input, recovery after coordinator/engine
outages and a security model. Define which engine, if any, owns the overall graph, how user-visible
state is reconciled, and what happens when an engine is offline. Each engine adapter must expose
consistent capabilities and explicit unsupported operations. Do not advertise native per-engine
graphs as one native mixed-engine workflow. Prototype failure/recovery and cross-engine data
transfer before committing to this architecture; keep single-engine portable flows working.

## Acceptance

A user can graphically build the `multi-agent-design` pattern, safely save it without interrupting
active work, activate the chosen version with informed consent where needed, select a supported
engine, launch it, inspect intermediate results and interact with agent nodes. A graph/YAML
round-trip preserves supported behavior. Existing YAML flows, single-engine runs and offline use
continue to work. Performance targets and any mixed-engine semantics require explicit agreement.
