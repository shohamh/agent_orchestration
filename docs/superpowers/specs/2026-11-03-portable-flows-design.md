# Portable flows: one DAG definition, native workflows in Hatchet, Trigger.dev and Prefect

Project 1 of 4 (then: step view with chat/log/restart/open-in links, worker selection, environment
setup with context). Requested 2026-11-03.

## Intent

Today a DAG flow is written three times, once per backend, and the board can only start the flows
compiled into it. The user wants to define a flow once (from the board or as a file), have it run on
each backend, and see it as a **real workflow object in each backend's own GUI**: a Hatchet workflow
with its DAG, a Prefect deployment with task runs, Trigger.dev tasks in its dashboard.

Steps are either **programs** (a command; its output is a log) or **agents** (a coding/LLM agent the
user can watch and talk to). Projects 2 to 4 build on the step kinds, the per-step input channel and
the shared workspace defined here.

## Goals

- A flow is a YAML file in `flows/`: input parameters, steps, `needs` between steps, step kind.
- Each flow becomes, per backend:
  - **Hatchet**: a registered DAG workflow named after the flow; each step a task with `parents`.
  - **Prefect**: a flow named after the flow, served as deployment `<flow>/flows`; each step a task
    run, dependencies passed as task inputs so Prefect records the data flow.
  - **Trigger.dev**: a parent task `flow-<flow>` plus one task per step `flow-<flow>-<step>`, generated
    as TypeScript (`src/trigger/flows.generated.ts`) that `trigger dev` hot-reloads.
- Saving a flow (file edit or board) updates all three without manual steps: the native flow workers
  re-register, and the Trigger.dev code is regenerated.
- The board lists flows, can create/edit one (YAML editor with validation), and starts runs of it on
  the current backend. Runs show as cards with their DAG, nodes marked agent/program.
- One step executor shared by all backends, so a step behaves the same everywhere.

## Non-goals (this project)

- Chat UI per agent node, log window, restart button, open-in links: project 2. (This project
  already records agent events and program logs per step so project 2 only renders them.)
- Choosing worker pools/tags: project 3. Context setup (repos, Confluence, files): project 4.
- Trigger.dev **production** flows. Steps run the Python executor and agent CLIs installed on the
  worker PC, which deployed containers don't have; like `agent-session`, flows need the local
  development worker. Production needs `trigger deploy` plus a Python image layer (future).
- Conditionals, loops, fan-out over lists, retries policy per step beyond a `retries` number.

## Anti-goals

- A flow that runs only on one backend, or that looks different in each backend's GUI from what
  the YAML says.
- Hiding the backends: their native UIs must stay usable for these flows.

## Flow file

```yaml
name: research-flow            # [a-z][a-z0-9-]*, unique; the workflow / deployment / task id
description: Research a topic and write a report
input:                         # parameters with defaults (strings, numbers, booleans)
  topic: edge caching
steps:
  fetch_sources:               # step id: [a-z][a-z0-9_]*
    kind: program
    run: python {flow_dir}/steps/fetch_sources.py
  summarize:
    kind: agent
    needs: [fetch_sources]
    agent: demo                # demo | codex | claude | opencode | opencode2 | omp | pi
    review: false              # true: wait for "Mark done" before the flow continues
    prompt: |
      Summarize these sources about {{ input.topic }}:
      {{ steps.fetch_sources.output }}
```

- `{{ input.x }}` and `{{ steps.<id>.output[.field] }}` are substituted (JSON for non-strings) in
  `run` and `prompt`. `{flow_dir}` is the `flows/` folder on the worker.
- **Program steps** run with the shell in the run's workspace. They get `FLOW_CONTEXT` (JSON:
  input + needed steps' outputs) in the environment and as stdin. stdout/stderr lines are the log.
  The output is the JSON written to the file named by `FLOW_OUTPUT`, else `{"stdout": <last 40
  lines>}`. A non-zero exit fails the step.
- **Agent steps** run the existing session engine (`agentdriver`). With `review: false` (default)
  the step finishes when the agent's turn ends; the output is the agent's final message. The `demo`
  agent in flows answers from its prompt without permission/question prompts unless `review: true`.
- All steps of one flow run share a workspace folder on the worker: `<FLOW_WORKSPACES>/<run id>`.

## Runtime

- `agents/flowkit/`: definition loading/validation, templating, the step executor, and a stdio
  bridge (`python -m flowkit.stdio`) used by Trigger.dev.
- Step events use the session event schema (`agentdriver/events.py`) plus `log {stream, text}`
  for program output and `step {state, output?}`. They are persisted the same way agent sessions
  persist theirs (Hatchet/Prefect logs as `EVT <json>`, Trigger.dev metadata `events`), per step.
- Input to an agent step goes to that **step's** run: Hatchet event `agent:input` scoped to the
  task run id; Prefect flow run input with key prefix `agentinput-<task run id>-auto`; Trigger.dev
  input stream `agent-input` on the step's run.
- **Flow host** (`agents/flow_host.py`, native, like the existing local workers): owns `flows/`,
  runs the Hatchet and Prefect flow workers as child processes and restarts them when a flow
  changes, regenerates the Trigger.dev file, and serves a small HTTP API on 127.0.0.1 for the board
  (list, get, validate, save, delete). The board runs in Docker and can't bind-mount on this PC;
  Docker Desktop forwards `host.docker.internal` to host loopback. On Linux, set
  `FLOW_HOST_BIND`/`FLOW_HOST_URL` and `FLOW_HOST_TOKEN`.
- Trigger.dev parent tasks start steps with `trigger` (no wait) as soon as their needs are done and
  poll the children, so the DAG runs with real parallelism (a run can only have one `*AndWait` in
  flight).

## Board

- New card dialog: "Flow" types listed from the flow host, with their input fields.
- "Flows" editor: list, new/edit YAML with live validation, save, delete.
- DAG nodes carry `kind` (agent/program) and `step` from the definition.

## Decided by Claude (user said "go")

YAML over JSON/a visual editor; a native flow host instead of bind mounts; one Python executor
for all backends; Trigger.dev flows in development only; `review: false` default for agent steps;
a quiet demo agent for examples; parameters are flat scalars.
