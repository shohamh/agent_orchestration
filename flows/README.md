# Portable flows

A flow is a YAML file here. It runs on **Hatchet, Prefect and Trigger.dev** as a native workflow,
so it also shows up in each backend's own UI:

| Backend | What the flow becomes |
|---|---|
| Hatchet | A registered DAG workflow named after the flow; each step is a task with `parents` = `needs` |
| Prefect | Flow `<name>`, served as deployment `<name>/flows`; each step is a task run, with its dependencies recorded |
| Trigger.dev | Parent task `flow-<name>` plus one task per step, `flow-<name>-<step>` (generated into `src/trigger/flows.generated.ts`) |

Create or edit flows from the board (**Flows** button) or edit the files directly. A file here is
a **draft**: saving it changes nothing that runs. **Activating** a draft (from the board) makes the
flow host (`agents/flow_host.py`) register that exact version on every backend; see
[Drafts and activation](#drafts-and-activation).

`multi-agent-design` is a reusable example: a brief fans out to Codex for architecture and
OpenCode 2 for an independent risk review, then Codex combines both into an implementation
plan. The `feature` input can be changed when starting a run. Select **Flows** or **New card**
on the board to inspect or launch it, and click an agent node to follow its conversation and
answer any permission requests. The real agents require working local logins.

## Graphical builder

**Flows** on the board opens the builder, which edits the same YAML:

- **Graph** (desktop): drag a step to move it, and drag from the round handle under a step onto
  another step to make that step wait for it. Drag the background to pan; pinch or Ctrl+scroll to
  zoom. **Auto-layout** and **Fit** tidy large graphs. Tab, Enter, the arrow keys and Delete work
  on the selected step.
- **Steps** (the default on phones): steps grouped into stages that run in parallel, with an
  **Add dependency** menu on each step. Selecting a step opens its form.
- **YAML**: the text itself. Invalid YAML stays in the editor and never replaces the last valid
  draft. Fields the forms don't cover, such as `environment`, are kept as written. If a file has
  comments or custom formatting, the builder asks before its first graph edit, because graph
  edits rewrite the file in canonical form.
- **Insert reference** on a prompt or command offers the flow's inputs and the outputs of the
  step's direct prerequisites, with the `{{ ... }}` syntax shown.
- **Save draft** never touches running work. Saving over a version someone else saved since you
  opened it shows both and lets you reload or overwrite. **Activate…** lists what each engine
  will reload and which runs it would interrupt, and asks for explicit confirmation before
  interrupting anything. **Run…** opens **New card** for the active version.

## Format

```yaml
name: research-flow              # lowercase letters, digits, '-'; must match the file name
description: What it does
input:                           # parameters and their defaults (text, numbers, true/false)
  topic: edge caching
steps:
  fetch_sources:                 # step id: lowercase letters, digits, '_'
    kind: program                # program: a command; its output is a log
    run: python {flow_dir}/steps/fetch_sources.py {{ input.topic }}
  summarize:
    kind: agent                  # agent: a coding/LLM agent you can watch and talk to
    needs: [fetch_sources]       # runs after these steps, and can use their output
    agent: demo                  # demo | codex | claude | opencode | opencode2 | omp | pi
    review: false                # true: the flow waits until you click "Mark done"
    prompt: |
      Summarize: {{ steps.fetch_sources.output.docs }}
    retries: 0                   # optional, any step
    timeout_minutes: 60          # optional, any step
```

- `{{ input.<name> }}` and `{{ steps.<needed step>.output[.field] }}` are replaced in `run` and
  `prompt`. Values that aren't text become JSON; in `run` they are quoted for the shell, so a
  value is always one argument and never runs as a command. `{flow_dir}` is this folder on the worker.
  On Windows, a `run` that uses values starts its program directly, without cmd.exe, unless it
  needs cmd.exe (pipes, redirection, `%VARIABLES%`, built-in commands such as `echo`, or `.cmd`
  tools). Values then reach the program exactly, line breaks included. A command that does need
  cmd.exe can't take a value with a double quote, `%` or a line break, because cmd.exe can't pass
  those on safely; the step fails with a message saying so. Read such values from `FLOW_CONTEXT`
  instead.
- **Program steps** run in the flow run's workspace folder (shared by all steps of the run on that
  worker). They get their context (`{"input": ..., "steps": {...}}`) as `FLOW_CONTEXT` and on
  stdin. Their result is the JSON written to the file named by `FLOW_OUTPUT`; without one it is
  `{"stdout": "<last 40 lines>"}`. A non-zero exit fails the step. `python` means the worker's
  Python. See `steps/flowstep.py` for helpers.
- **Agent steps** run the same session engine as the board's coding-agent sessions. The step's
  output is the agent's final message. The `demo` agent is simulated (no model) and, without
  `review`, answers from its prompt without asking anything.

## Watching and restarting steps

Click an agent block in the board's DAG to use its live chat, permissions, questions, Interrupt,
Continue and Mark done. Set `review: true` to hold the flow until you mark that agent step done.
Program blocks show a scrolling log window. Conversations open on the latest 100 messages and
logs on the latest 1000 lines; scrolling up loads earlier pages.

**Restart step** reruns that step and its dependent descendants. Prerequisites and independent
branches keep their results. Finish or stop the source flow first.

- **Hatchet:** the board sends the selected task ID to Hatchet's native replay API. Hatchet walks
  its persisted dependency edges and reruns the subtree in the same workflow run, even when
  the selected step's output is unchanged.
- **Prefect and local Trigger.dev:** these engines have no equivalent native subtree invalidation.
  The flow host's orchestration extension creates a new native flow run from the selected step.
  The runtime owns dependency selection and reuse; the board forwards only backend/run/step IDs.
  Engine tasks use their normal dependencies and return saved results for unaffected steps.
  The original workspace is copied before execution; the new run records its source.

The extension saves exact outputs in `<FLOW_WORKSPACES>/<run>/.flow-results/`, separately from
clipped UI log previews. Restart requires those files on the flow-host PC, an unchanged definition,
and no active source child tasks. Historical runs made before result persistence must be replayed
in full first. Internal `flowRestart` cannot collide with lowercase user input names.

## Drafts and activation

- `<name>.yaml` here is the editable draft. A hand edit just shows as a changed draft in the board.
  Its revision is `sha256:` of the file's bytes; a save through the board names the revision it
  edited and is refused if the file changed meanwhile (another tab, an editor), instead of
  overwriting it. Invalid YAML is never saved over a valid draft.
- `<name>.layout.json` (optional) holds graph positions for the builder. It is not part of the
  revision and saving it never touches an engine.
- Activation copies the draft into an immutable snapshot in the flow state folder (`FLOW_STATE_DIR`,
  default `<repo>/.flow-state/`, not committed) and applies it: the Hatchet and Prefect flow workers
  restart with the new version and the Trigger.dev tasks are regenerated when they change (the
  `trigger dev` worker reloads them itself, so that engine stays *pending* rather than confirmed).
  Workers, Trigger.dev code generation, card labels and launches only ever use active snapshots.
- Before applying, the board lists the queued and running runs on every engine and board that
  the reload would interrupt, rechecks just before applying and asks for confirmation; if an
  engine cannot be asked, it says so. It never cancels runs itself.
- If an engine fails (its worker does not stay up), it keeps serving the previous snapshot and the
  board shows the failure; activating again retries only what is not active yet.
- Deleting a draft does not stop the flow: its active version stays until the removal is activated.
- First start without a state folder: every valid draft is activated once (the workers start then
  anyway). Delete `.flow-state/` to repeat that import.
- `{flow_dir}` in a step still means this folder on the worker, so helper scripts under `steps/`
  are used as they are (they are not part of the snapshot).

## Limits

- Steps run on the PC running the flow host and the native workers (they need the agent CLIs and
  this folder). On Trigger.dev that means the **local development** worker, not production.
- Activating a flow restarts the Hatchet and Prefect flow workers, and changed Trigger.dev tasks
  reload the local dev worker; steps running on them at that moment are interrupted (the board
  asks first). Saving a draft or a layout interrupts nothing.
- No loops, conditions or fan-out over lists yet.
