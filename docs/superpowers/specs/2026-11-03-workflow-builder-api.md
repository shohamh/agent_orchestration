# Workflow builder API contract

Shared contract between the flow host / board backend and the graphical builder
(`board/static/flow-builder.js`). Implements tasks 1–3 of
`docs/superpowers/plans/2026-11-03-graphical-workflow-builder.md`.

## Model

- **Draft**: `flows/<name>.yaml`, the portable, user-editable source. Saving a draft (API or a
  hand edit) never restarts a worker, regenerates Trigger code or changes what runs.
- **Active snapshot**: an immutable copy of an approved draft revision in the flow state directory
  (`FLOW_STATE_DIR`, default `<repo>/.flow-state/`, gitignored). Flow workers, Trigger code
  generation, card labelling and launches use active snapshots only. A run keeps the snapshot it
  started with.
- **Revision**: `sha256:` + hex digest of the UTF-8 YAML text. Layout never affects it.
- **Layout sidecar**: `flows/<name>.layout.json`, `{"version": 1, "positions": {"<step id>": {"x": n, "y": n}}}`.
  Optional view metadata only; unknown/stale step IDs are ignored; missing steps get auto-layout.
- **Migration**: on first start with no active snapshots, every valid draft is activated (workers
  start at that moment anyway, so nothing is interrupted).

## Document JSON

The semantic document is the authored mapping, in authored order, without the generated
`setup_environment` step:

```json
{
  "name": "multi-agent-design",
  "description": "...",
  "input": {"feature": "..."},
  "worker": {"pool": "gpu"},
  "environment": {"...": "as authored"},
  "steps": {
    "brief": {"kind": "program", "run": "python ...", "needs": []},
    "architecture": {"kind": "agent", "needs": ["brief"], "agent": "codex", "review": false,
                     "prompt": "...", "retries": 0, "timeout_minutes": 60,
                     "description": "...", "worker": {"tags": ["x"]}}
  }
}
```

Optional keys are present only when authored, except `needs`: every step has it as a list (`[]`
when not authored, a single string becomes a one-item list), and an empty `needs` is not written
back. Graph edges are exactly `needs`. Serialisation is
deterministic: top-level `name, description, input, worker, environment, steps`; step keys
`kind, description, needs, agent, review, worker, retries, timeout_minutes, setup, run, prompt`;
multi-line strings as `|` blocks; unicode kept.

## Errors

`errors: [{"message": str, "step": str | null, "field": str | null}]`. `step`/`field` locate the
problem for highlighting (`field` is a step key such as `needs` or `prompt`, or a top-level key
when `step` is null). Validation covers everything `flowkit.definition.parse` checks, plus:
duplicate keys in YAML, template references (`{{ input.x }}` must be a declared input;
`{{ steps.y.output... }}` must name a step listed in that step's `needs`), unknown steps in
`needs`, self-dependency, cycles (one error per step in the cycle).

## Board endpoints (proxied to the flow host)

All under the existing board authentication. Existing `/api/flows*` endpoints keep working;
`GET /api/flows` now returns **active** definitions (what can be launched) and `PUT /api/flows/<name>`
saves a draft only.

| Method and path | Body | Response |
|---|---|---|
| `GET /api/flow-documents` | | `{"documents": [Doc]}` |
| `GET /api/flow-documents/{name}` | | `Doc` or 404 |
| `POST /api/flow-documents/parse` | `{"yaml"}` | `{"document": obj \| null, "errors": [...], "lossless": bool}` |
| `POST /api/flow-documents/serialize` | `{"document"}` | `{"yaml": str, "errors": [...]}` |
| `PUT /api/flow-documents/{name}` | `{"yaml", "expectedRevision": str \| null}` | `Doc`; 409 `{"error": "conflict", "current": Doc}`; 400 `{"errors": [...]}` |
| `PUT /api/flow-documents/{name}/layout` | `{"layout"}` | `{"ok": true}` (no revision change, no side effects) |
| `DELETE /api/flow-documents/{name}?revision=` | | `{"ok": true}`; 409 on stale revision. Deletes the draft; the active version remains until a removal is activated. |
| `GET /api/flow-documents/{name}/activation` | | `Preview` |
| `POST /api/flow-documents/{name}/activate` | `{"revision", "impact", "confirm": bool}` | `Result`; 409 `{"error": "impact changed" \| "confirmation required" \| "conflict", "preview": Preview}` |

`expectedRevision: null` means "create": 409 if the draft exists. A YAML whose `name` differs from
`{name}` is a 400 error.

```text
Doc = {name, revision, yaml, document | null, errors, lossless, layout | null,
       state: "active" | "changed" | "inactive" | "broken" | "removed",
       activeRevision | null}
```

`lossless` is true when serialising the parsed document reproduces the YAML exactly. When false,
the UI must warn once before a graph edit replaces the text with canonical YAML (comments and
formatting are not kept); YAML-tab edits are always kept verbatim. `state`: `active` = draft equals
the active snapshot; `changed` = both exist and differ; `inactive` = draft never activated;
`broken` = draft fails validation (an older active snapshot may still exist); `removed` = active
snapshot with no draft.

```text
Preview = {name, revision (draft revision, or null for a removal), activeRevision,
           change: "add" | "update" | "remove" | "none",
           engines: [{engine: "hatchet" | "prefect" | "trigger", action: "reload worker" | "regenerate tasks" | "none",
                      available: bool, activeRuns: [{system, board, id, title, status}] | null}],
           disruptive: bool, impact: str}
```

`activeRuns: null` means the state could not be determined. `disruptive` is true when any
affected engine has active runs or unknown state. `impact` is an opaque hash of the revision plus
the impact; activation recomputes it and returns 409 `impact changed` if it differs. A disruptive
activation needs `confirm: true` (409 `confirmation required` otherwise). Activation never cancels
runs on its own.

```text
Result = {name, activeRevision, engines: [{engine, state: "active" | "pending" | "failed" | "unavailable", error | null}]}
```

If an engine fails, the previous active snapshot stays in force for it and the result says so;
retrying the same activation is idempotent.

## Implementation notes (backend)

Small additions to the tables above; everything listed there behaves as described.

- 400 responses carry `{"error": <first message>, "errors": [...]}`; `errors` is the contract.
- `Preview` also has `errors` (the draft's validation errors). Activating a broken draft is a 400
  `{"errors"}` and changes nothing.
- `PUT .../layout` needs an existing draft (404 otherwise) and also accepts an optional
  `"viewport": {"x", "y", "zoom"}`. A removed flow's `Doc` has `revision: null`, `layout: null` and
  the active snapshot's `yaml` (so it can be restored by saving it with `expectedRevision: null`).
- `activeRevision` is the most recently activated revision; `Result.engines` tells whether each
  engine serves it. Engine `state`: `active` = the worker restarted and stayed up for
  `FLOW_ACTIVATION_READY_SECONDS` (default 5, so activation can take several seconds); `pending` =
  Trigger.dev tasks were regenerated, which the `trigger dev` worker reloads on its own, unconfirmed;
  `failed` = the engine keeps its previous snapshot; `unavailable` = the engine is not enabled on the
  flow host (`FLOW_BACKENDS`). A retry restarts only engines that are not active on that revision.
- `Preview.engines[].available` is false when the board has no adapter for that engine, the flow
  host does not enable it, or its active runs could not be listed (`activeRuns: null`). Hatchet
  lists queued/running runs of all active flows, Prefect all unfinished `portable-flow` runs, and
  Trigger.dev development every queued/executing/waiting run (a dev worker reload interrupts agent
  sessions too). `impact` hashes the revisions, change, each engine's action and its run IDs (not
  statuses), so a new run between preview and apply returns 409 `impact changed`.
- Drafts must be UTF-8 text without YAML aliases (`*name`) or surrogate escapes such as
  `"\ud83d"`; otherwise the `Doc` is `broken` with an error saying so, and it cannot be activated.
- The flow host's `Preview` also has `served`: `{flow: definition}` for every flow some engine still
  serves (a removal that failed on one engine included); the board uses it for the run inventory
  and does not pass it on. Activation applies Trigger.dev first and then restarts all affected
  workers at once, so the interruption follows the board's final check immediately. While it
  runs, saving drafts is not blocked; a board request that times out reports "flow host is busy".
- Hatchet's inventory covers 6 days and 500 runs; it reports `activeRuns: null` when a served flow
  could run longer than that or the limit is reached.
- The legacy `PUT /api/flows/{name}` validates like the builder and, when the body has
  `expectedRevision`, returns 409 `{"error": "conflict", "current": Doc}` for a stale save.
  Flow host writes (POST/PUT/DELETE) require `Content-Type: application/json` (415 otherwise).
- The flow host serves the same operations without the `/api` prefix (`/documents...`). Its
  activation does not check `impact`/`confirm`: the board, which can see every engine, enforces it.
