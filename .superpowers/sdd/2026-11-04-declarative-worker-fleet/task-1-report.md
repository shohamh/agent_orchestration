# Task 1 report

Status: feasibility established only in part; adopt the shared-service fallback for the current
Hatchet, Prefect serve and Trigger local development modes. Existing service still needs the
proposed fleet admission, waiting and fencing guarantees. Native placement is not approved.

An isolated Docker Compose project `fleet-feasibility-disposable` used separate ports 17077,
18080 and 15435, fresh project-scoped volumes and a newly generated tenant token. The server
image was `ghcr.io/hatchet-dev/hatchet/hatchet-engine:v0.114.2`, SDK 1.44.0. Startup command:
`docker compose -p fleet-feasibility-disposable -f hatchet/docker-compose.yml --env-file
hatchet/.env.example up -d`, with HATCHET_GRPC_PORT=17077, HATCHET_DASHBOARD_PORT=18080 and
POSTGRES_HOST_PORT=15435 in the shell. The token was generated inside that project with
`docker compose ... run --rm --no-deps -T setup-config /hatchet/hatchet-admin token create
--config /hatchet/config --tenant-id 707d0855-80ab-4e1f-a156-f1c4546cbf52` and passed only
via the process environment. No existing service or credential was used.

Live suite: three SDK checks passed, queued cancellation passed, and two checks failed (539.598s).
A task required kind=eligible, but a run-level required station=station-b selected a station-b
worker whose kind was ineligible; this failure reproduced in a separate 44.174s run. Therefore
runtime selector and task-level requirements cannot be assumed to combine. A separate 60.775s
live test without a runtime override passed: a task waited with its matching worker disconnected,
then executed when the other worker was re-registered with an eligible label. This demonstrates
re-registration, not in-place mutation of a live worker label. Cancellation was checked by
observing no execution after adding a matching worker, not by asserting board state.

The long-wait test set an eight-minute scheduling timeout, waited 310 seconds with no eligible
worker, then started one and observed no execution marker during a further 90 seconds. Engine
logs included a slot-replenishment context deadline exceeded; the cause of the missing execution
is not established and >5-minute waiting is not proven. Worker restart, full cancellation state,
in-place label updates, run ID selector and physical multi-PC execution remain unverified.

Live tests require explicit FLEET_HATCHET_ISOLATED=1 and explicit isolated client endpoint and
token environment variables; otherwise four live tests skip. Default invocation after changes:
`agents/.venv/Scripts/python.exe -m unittest agents.tests.test_fleet_engine_feasibility -v`
(with the workspace venv) passed three surface checks, skipped four live tests. Live failure is
an intentional guard against assuming guarantees that the observed server did not provide.

Teardown: stopped matching probe worker process trees, then ran `docker compose -p
fleet-feasibility-disposable -f hatchet/docker-compose.yml --env-file hatchet/.env.example down
--volumes --remove-orphans`; command exited 0 and removed the project's containers, network and
three volumes. The live suite was started before the combined selector/reconnection test was
split into separate tests; the separate reconnection test was run independently. Worker cleanup
was subsequently strengthened for Windows process trees.
