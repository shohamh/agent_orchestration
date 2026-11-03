# Confluence Markdown exporter verification

The environment setup importer uses [Spenhouet's Confluence Markdown Exporter](https://github.com/Spenhouet/confluence-markdown-exporter),
version **5.6.0**. It is pinned in `agents/requirements.txt` and installed in the local worker
virtualenv. `uv pip check --python agents/.venv/Scripts/python.exe` reported all 143 packages
compatible; installing the exporter added 13 packages and did not change existing versions.

## Public sites

The final `agents/confluence_smoke_test.py` run completed successfully without credentials.
It used the same preparation and validation functions used by setup workers. Outputs and a
machine-readable `verification.json` are retained under ignored
`agents/runtime/confluence-public-final`.

| Public source | Markdown pages | Markdown bytes | Notes |
|---|---:|---:|---|
| [Apache Tika: TikaServer](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=148639291) | 6 | 59,007 | Root plus five descendants, four attachments |
| [Apache Tika: UsingGit](https://cwiki.apache.org/confluence/spaces/TIKA/pages/109454103/UsingGit) | 1 | 7,382 | Public Data Center page |
| [Atlassian: View Page Information](https://confluence.atlassian.com/conf712/view-page-information-1056671473.html) | 1 | 2,420 | Two referenced attachments |
| [Shibboleth: SystemRequirements](https://shibboleth.atlassian.net/wiki/spaces/SP3/pages/2067399657/SystemRequirements) | 1 | 7,059 | Public Confluence Cloud page |

The six-page tree's Markdown includes local links to exported children `191334866.md` and
`240883999.md`; a link to page `109454069`, outside the tree, stays an absolute Confluence URL.
The importer checks the page set, page versions, attachment IDs/versions and hashes of every
output file. Private Confluence accounts were not available for an end-to-end login test; the
fixture verifies bearer and username/token authentication headers, credential isolation and missing
credentials.

## Engine and regression checks

- Three native engines imported a paginated local Confluence fixture with the real exporter,
  waited for setup review, transferred Markdown and a referenced PNG to a different worker
  service, and restarted the selected dependent step successfully:
  Hatchet `2594e1a7-20b7-4da4-9733-7a09fde88fab`, Prefect
  `f7b02f8f-9f27-4c6e-bc27-9edd8d10c244` and Trigger.dev
  `run_cmw10t5a400gz4go2gwwg3jfr`.
- A separate native Hatchet run `f837aad0-d13b-474c-9c0f-71e0482a6822` imported the actual
  public Shibboleth Cloud page on one local service, waited for review, read its Markdown on a
  second local service, and passed native step replay. These services ran on the same physical PC.
- Agents suite: 51 tests run, 50 passed and the POSIX-only process-group test was skipped on Windows.
  Board suite: 36 tests passed. Focused exporter tests additionally cover code-fence and inline-code
  preservation, child links, missing files, source page and attachment version changes, partial
  export, credential isolation and preservation of the previous complete import.
- `git diff --check` passed for the feature. The worker runtime was restarted with the pinned
  exporter installed; unrelated concurrent documentation edits were preserved.

The previous offline bundle was not rebuilt; setup workers on an offline PC need the exporter and
its dependencies supplied through their own offline package source.
