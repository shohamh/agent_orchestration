# Live updates: implementation and verified engine feeds (2026-11-03)

Requested by the user:
- Push instead of polling (1 s or better).
- One shared watcher per board.
- Lightweight cards; the transcript loads only for the open card.
- Incremental engine reads, with finished runs cached.
- Send only changes, and pause hidden tabs.
- Fast first load.
- **Use the engines' live feeds.**
- Transcripts: load the latest 100 messages; scrolling up loads older ones, with a loading indicator.
- Ordinary logs: load the latest 1000 lines; scrolling up loads more.

## Measured before the change (board API, one refresh)

| Board | Size | Time | Notes |
|---|---|---|---|
| prefect/research | 93 KB | | One 704-event session is 78 KB of it |
| trigger-dev/live-demo | 34 KB | up to 1.0 s | |
| others | 3–22 KB | | |

Every poll sends every session's full transcript, and the server re-reads each run's logs from the start.

## Engine live feeds (verified against the running servers)

**Trigger.dev Realtime.**
- `GET /realtime/v1/runs?tags=board:<b>` (whole board) or `/realtime/v1/runs/<id>` (one run).
- Send `Authorization: Bearer <env secret>` **and `x-trigger-electric-version: 1.0.0-beta.1`**. Without that header the webapp uses an older protocol (`shape_id`) that keeps answering `must-refetch`.
- Electric shape protocol:
  - The first request uses `offset=-1` and returns a snapshot. Response headers: `electric-handle`, `electric-offset`.
  - Then poll with `offset=<o>&handle=<h>&live=true[&cursor=<c>]`. This long-polls about 20 s and returns change rows, or `[{"headers":{"control":"up-to-date"}}]` with new `electric-offset` / `electric-cursor`.
  - On 409 `must-refetch`, restart from `offset=-1`.
- Row values include `status`, `metadata` (a JSON string, holding `events` for sessions), `runTags`, `updatedAt`, `completedAt`.

**Prefect websockets.**
- `ws://<host>:4200/api/events/out` and `/api/logs/out`. They need **subprotocol `prefect`**; without it the server closes with 1002.
- Send `{"type":"auth","token":null}`, which returns `{"type":"auth_success"}`. Then send `{"type":"filter","filter":{...}}`.
  - Events filter: `{"occurred":{since,until},"event":{"prefix":["prefect.flow-run."]}}`.
  - Logs filter: `{"flow_run_id":{"any_":[id]},"timestamp":{"after_","before_"}}`.
- Verified state events and streamed log messages against the running server. Log messages use
  `{type: "log", log: {flow_run_id, ...}}`; routing maps those run IDs back to board watchers.

**Hatchet.**
- Verified gRPC run-event streaming filtered by additional metadata `board`.
- Session and portable-flow workers call `aio_put_stream` per transcript event to wake watchers.
- Logs have no live feed. Use them for history and paging.

## Design

- **Server** (`board/live.py`): a hub keyed by (system, board).
  - Each key has one watcher task. It is woken by the engine feed, with a fallback tick of 5 s.
  - The watcher fetches lightweight cards (title, state, last line, queue), diffs them by hash, and pushes `cards` patches over SSE (`GET /api/live?system=&board=&card=`).
  - A per-card subscriber pushes new transcript rows or log lines for the open card.
  - Idle watchers stop 30 s after the last viewer leaves.
  - Finished run logs are cached in bounded caches and frozen after a grace period for late logs.
- **Paging**:
  - `GET /api/cards/{s}/{id}/transcript?limit=100&before=<cursor>` returns **coalesced rows**: agent chunks merged by `message_id`, tools by `id`, requests with their resolution. Always cut on a row boundary.
  - `GET /api/cards/{s}/{id}/logs?limit=1000&before=<cursor>`, and the same for DAG node logs.
- Browser reads return only the requested newest page. Hatchet/Prefect cold caches currently read
  historical logs once, then fetch incrementally with overlapping timestamp windows. Trigger.dev
  metadata holds a capped rolling window. Native backwards engine pagination remains a future
  optimization for very large cold transcripts; lightweight card first paint avoids waiting on it.
- **UI**:
  - Replace `setInterval` polling with an `EventSource`, and close it when the tab is hidden.
  - Cards no longer carry `events`.
  - The chat and log lists load older pages when scrolled to the top, with a spinner and the scroll position kept.
