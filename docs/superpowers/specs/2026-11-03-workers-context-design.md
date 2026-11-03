# Workers and environment context

The remaining requested work is choosing worker pools/tags for flows and individual steps,
showing the worker that actually executes each step, and preparing repositories, Confluence
page trees and files through an interactive setup agent. The user explicitly requires different
steps in one DAG to be able to execute on different PCs.

## Execution and ownership

Keep native Hatchet tasks, Prefect tasks and Trigger child runs as the scheduling and dependency
boundary. The board forwards selections and renders runtime state; it does not schedule tasks,
compute descendants or copy workspaces. Native coordinator processes dispatch step execution to
a trusted execution service. This extension supplies a consistent execution target across the
current Prefect serve and Trigger development backends, which cannot route arbitrary Python/CLI
steps to a particular PC. Execution worker identity is distinct from engine coordinator identity.

A worker service runs on each PC with an id, pool, tags, slot limit and its own installed agent
CLIs/credentials. A trusted YAML registry on the flow host maps ids to endpoints and token
environment variable names. Network services require authentication; loopback development may
omit a token. Never put credentials or arbitrary execution URLs in native run parameters or the
browser. Offline/unmatched selections fail explicitly; never silently execute on another PC.

Flow YAML may declare a default worker selector and per-step overrides, each with worker id,
pool and/or required tags. The start dialog can override the default and individual steps.
Selection is resolved by the runtime at execution, and the actual worker id, hostname, pool and
tags are emitted only when execution begins. Native engine retries remain responsible for retry
policy. Cancellation propagates to the execution process and descendants; interactive input and
events travel through the existing engine channels. A configured service with no free slot queues
the step, and publishes the identity after acquiring a slot.

## Workspace contract (builder decision)

Each step receives an isolated workspace containing changes from all its completed ancestors.
The runtime transfers content, not absolute local paths. Files include their relative names and
bytes; symbolic links and traversal names are rejected. Dependency outputs remain native task
outputs. Publish immutable per-step file deltas and the exact output before successful completion.
Sequential changes to a path supersede earlier changes. Incomparable branches making different
changes to the same path cause a clear merge error before the downstream command starts; identical
changes are allowed. Deletions participate in this rule. Retry replaces only that step's published
delta, and consumers never read partial publications. Bound transferred bytes and file count and
report limits as errors rather than dropping files. Initial default transfer limit: 64 MiB and
10,000 files. Repository .git directories travel too, so setup repair agents can use git normally.

The host retains canonical merged workspaces and exact result manifests for existing restart
validation. Prefect/Trigger runtime restart preserves routing selections, validates all source
tasks are terminal, reuses unaffected results and deltas, and reruns the selected subtree through
engine dependencies. Hatchet native replay still owns descendant invalidation; replayed steps
replace their own artifact delta so descendants consume current data. Older local-only runs keep
their existing restart behavior. Users may change YAML selectors for future runs; changing a flow
definition still invalidates selective restart under the existing fingerprint contract.

## Environment setup

An optional `environment` section in a flow contains named git sources (URL/ref/relative target),
files (source path on the selected setup worker, relative target), and Confluence sources (base
URL, root page id, relative target, token_env and optional username_env). Confluence exports the
root and descendants as HTML with a JSON index, using the supported content API and pagination.
Credentials are read only on the execution PC. The YAML editor is the configuration interface;
the launch dialog shows the configured environment and setup worker selection. File sources are
explicit paths on the setup PC; browser upload is outside this iteration and must be labeled.

Parsing an environment-bearing flow expands it into an ordinary `setup_environment` agent task
and dependencies from that task to all prior roots. The name is reserved only for such flows.
Setup first attempts deterministic materialization of each source, emits success/error events,
then opens the chosen agent (default codex) in the prepared workspace with the source report and
repair instructions. It always waits for human review, including when automatic preparation
succeeds. Errors remain visible and can be repaired interactively; Mark done validates required
targets before allowing engine dependencies to continue. A failed validation reopens the repair
agent with the missing targets. Test/demo setup may use the existing demo agent, but never claims
to repair missing sources. The setup workspace is transferred like any other ancestor output.
Git clone uses argument arrays, credential helpers or SSH; reject HTTP URLs containing passwords.
Safe relative destinations cannot overlap one another or internal runtime directories.

## Builder check decisions

The existing native coordinators run on the flow-host PC and share its artifact store on disk;
execution PCs need only the authenticated service. Moving coordinators themselves to other PCs
requires a shared artifact store and is outside this iteration. Transfer an immutable snapshot of
`flows/` executable assets with each job and render `{flow_dir}` against that worker-local copy.
Preserve file permission bits and reject case-folded collisions and file/directory collisions.
For each path merge only causally maximal writers, allowing identical parallel writes followed by
a sequential edit. Reconstruct restarted steps from ancestors, never from the final source files.
The copied restart workspace retains immutable publications, not an execution starting directory.
Fence publications with a per-step execution UUID stored atomically before dispatch; only its
current holder may publish. Job submission is idempotent on that UUID. A coordinator that loses
contact fails the native attempt and cancels its remote job; it cannot claim successful execution.
Git validation checks a usable worktree and requested revision; file validation checks actual
content; Confluence validation re-fetches the paginated tree and checks exported page content.
Partial target directories never count as successful setup.

## Verification and delivery

Tests must demonstrate actual execution on two independently configured services, a file crossing
a worker boundary, branch conflict/deletion behavior, queued cancellation and interactive inputs.
Exercise setup with a disposable git repository, a real file and a local paginated Confluence API
fixture, including a failed source and subsequent repair. Run native smoke flows on all three
engines and show actual worker identity in the board. Use disposable test runs only. Preserve
authentication and existing chat/restart behavior; publish the finished changes to the already
authorized feature branch. Update deployment and worker setup documentation, and state limits
(including that two local test services do not constitute a physical two-PC network test).
