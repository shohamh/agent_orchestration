# Portable flows

A flow is a YAML file here. It runs on **Hatchet, Prefect and Trigger.dev** as a native workflow,
so it also shows up in each backend's own UI:

| Backend | What the flow becomes |
|---|---|
| Hatchet | A registered DAG workflow named after the flow; each step is a task with `parents` = `needs` |
| Prefect | Flow `<name>`, served as deployment `<name>/flows`; each step is a task run, with its dependencies recorded |
| Trigger.dev | Parent task `flow-<name>` plus one task per step, `flow-<name>-<step>` (generated into `src/trigger/flows.generated.ts`) |

Create or edit flows from the board (**Flows** button) or edit the files directly. The flow host
(`agents/flow_host.py`) re-registers them on every backend within a few seconds.

`multi-agent-design` is a reusable example: a brief fans out to Codex for architecture and
OpenCode 2 for an independent risk review, then Codex combines both into an implementation
plan. The `feature` input can be changed when starting a run. Select **Flows** or **New card**
on the board to inspect or launch it, and click an agent node to follow its conversation and
answer any permission requests. The real agents require working local logins.

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
  `prompt`. Values that aren't text become JSON; in `run` they are quoted for the shell.
  `{flow_dir}` is this folder on the worker.
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

## Limits

- Steps run on the PC running the flow host and the native workers (they need the agent CLIs and
  this folder). On Trigger.dev that means the **local development** worker, not production.
- Saving a flow restarts the Hatchet and Prefect flow workers; steps running on them at that
  moment are interrupted.
- No loops, conditions or fan-out over lists yet.
