# Confluence Markdown exporter implementation plan

> **For agentic workers:** Use superpowers:executing-plans to implement inline.

**Goal:** Use the requested Confluence Markdown Exporter for environment imports and prove it on public Confluence pages.
**Architecture:** Replace the existing HTML importer with an isolated subprocess adapter around Spenhouet's pinned exporter. Keep worker-side credential variables, source validation, interactive setup review and workspace transfer unchanged. Publish complete exports atomically and validate exporter completion as well as output hashes.
**Tech Stack:** Existing Python/aiohttp runtime; confluence-markdown-exporter==5.6.0, verified from live PyPI metadata.
**Spec:** User request in chat: explain current file transfer; use Confluence Markdown Exporter; set it up and test public pages. Existing worker/context design remains applicable.

## Global constraints

- Credentials stay on the setup worker and outside transferred files/logs.
- Export root plus descendants by default; allow opting out for an individual page.
- Incomplete exports and missing/changed Markdown or referenced attachments block setup completion.
- Existing workspace limits remain 64 MiB and 10,000 files.
- Preserve unrelated concurrent documentation edits.

## Review focus

- Upstream exporter can report page failures without a failing CLI exit: inspect statistics and the complete page set.
- A failed refresh must preserve the previous complete target.
- Anonymous public sites must work without requiring stored credentials.
- Separate imports must not share exporter configuration or leak secrets into artifacts.
- Missing files/partial trees must fail validation before engine dependencies continue.

### Task 1: Replace and verify the importer

Files: new `agents/flowkit/confluence_export.py` subprocess and adapter; modify `environment.py`, requirements and tests. Add a realistic paginated Confluence fixture shared by tests/native smoke.

- [x] Add failing real-exporter tests for Markdown, page trees, referenced attachments, incomplete output and anonymous auth.
- [x] Implement isolated settings, exporter page discovery/conversion, completion checks, hashes and atomic target publication. Add `include_descendants` option while retaining existing source fields.
- [x] Run focused and existing suites; test fresh import and validation on at least two public pages, retaining local outputs and concise provenance evidence.
- [x] Update native smoke checks for Markdown, review the adapter, run native integration and document usage and file-transfer behavior.
- [x] Commit/push the feature and verify the running services.

## Execution ledger

The existing agents virtualenv gained 13 new packages through uv; no existing dependency versions were changed. PyPI source and installed 5.6.0 API were inspected rather than relying on cached web version numbers.

Verification: see ../2026-11-03-confluence-markdown-verification.md. Review found independent attachment-version drift and code-span preservation problems; both were fixed with regression tests and spot-checked.
Delivery verification: The main board authenticated worker catalog and flow-host catalog returned 200 locally and through Funnel; the local worker was online and idle after native smoke cleanup.
