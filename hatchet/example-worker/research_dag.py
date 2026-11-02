"""Research-report DAG: a realistic agent pipeline with an uneven dependency graph.

    fetch_sources ─┬─> summarize ────────────────┐
                   └─> extract_entities ─┬───────┼─> synthesize_report
                                         └─> assess_risk ─┘

summarize runs in parallel with the extract_entities -> assess_risk chain; synthesize_report
waits for all three. The engine schedules each task once its parents finish. The task bodies are
deterministic stubs: replace them with calls to your (on-prem) LLM and tools.
"""

import re

from pydantic import BaseModel

from hatchet_sdk import Context, Hatchet

hatchet = Hatchet()

RISK_TERMS = ("outage", "breach", "lawsuit", "recall")


class ResearchInput(BaseModel):
    topic: str
    sources: list[str] = ["internal-wiki", "incident-db", "news-feed"]


research_report = hatchet.workflow(name="research-report", input_validator=ResearchInput)


@research_report.task(retries=3)
def fetch_sources(input: ResearchInput, ctx: Context) -> dict:
    # Real version: pull documents from search / a vector store / internal APIs.
    docs = [
        {
            "source": s,
            "text": f"{s} on {input.topic}: Acme Corp and Globex reviewed the {input.topic} rollout. "
            f"One region saw a short outage; Initech is evaluating the same approach.",
        }
        for s in input.sources
    ]
    ctx.log(f"fetched {len(docs)} documents")
    return {"docs": docs}


@research_report.task(parents=[fetch_sources], retries=2)
def summarize(input: ResearchInput, ctx: Context) -> dict:
    docs = ctx.task_output(fetch_sources)["docs"]
    # Real version: one LLM call per document (or one map-reduce call).
    return {"summaries": [{"source": d["source"], "summary": d["text"].split(". ")[0]} for d in docs]}


@research_report.task(parents=[fetch_sources], retries=2)
def extract_entities(input: ResearchInput, ctx: Context) -> dict:
    docs = ctx.task_output(fetch_sources)["docs"]
    # Real version: an LLM call with a structured-output schema.
    found = {m for d in docs for m in re.findall(r"\b[A-Z][a-z]+(?: Corp)?\b", d["text"])}
    return {"entities": sorted(found - {"One"})}


@research_report.task(parents=[fetch_sources, extract_entities])
def assess_risk(input: ResearchInput, ctx: Context) -> dict:
    docs = ctx.task_output(fetch_sources)["docs"]
    entities = ctx.task_output(extract_entities)["entities"]
    hits = sorted({t for d in docs for t in RISK_TERMS if t in d["text"].lower()})
    return {"level": "high" if hits else "low", "signals": hits, "entities_at_risk": entities if hits else []}


@research_report.task(parents=[summarize, extract_entities, assess_risk])
def synthesize_report(input: ResearchInput, ctx: Context) -> dict:
    summaries = ctx.task_output(summarize)["summaries"]
    entities = ctx.task_output(extract_entities)["entities"]
    risk = ctx.task_output(assess_risk)
    # Real version: a final LLM call that writes the report from these structured parts.
    return {
        "title": f"Research report: {input.topic}",
        "key_points": [s["summary"] for s in summaries],
        "entities": entities,
        "risk": risk,
    }
