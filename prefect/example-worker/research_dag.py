"""The same five-task research DAG as the Hatchet example.

fetch_sources -> summarize -----------------------> synthesize_report
             -> extract_entities -> assess_risk --> synthesize_report

Task futures carry both results and dependencies into Prefect's run graph.
Bodies use deterministic local fixtures; replace them with your LLM/tools.
"""

import re

from prefect import flow, get_run_logger, task

RISK_TERMS = ("outage", "breach", "lawsuit", "recall")


@task(retries=3, retry_delay_seconds=1)
def fetch_sources(topic: str, sources: list[str]) -> dict:
    docs = [
        {"source": source,
         "text": f"{source} on {topic}: Acme Corp and Globex reviewed the {topic} rollout. "
                 "One region saw a short outage; Initech is evaluating the same approach."}
        for source in sources
    ]
    get_run_logger().info("Fetched %s documents", len(docs))
    return {"docs": docs}


@task(retries=2, retry_delay_seconds=1)
def summarize(sources: dict) -> dict:
    return {"summaries": [{"source": d["source"], "summary": d["text"].split(". ")[0]}
                          for d in sources["docs"]]}


@task(retries=2, retry_delay_seconds=1)
def extract_entities(sources: dict) -> dict:
    found = {match for d in sources["docs"] for match in re.findall(r"\b[A-Z][a-z]+(?: Corp)?\b", d["text"])}
    return {"entities": sorted(found - {"One"})}


@task
def assess_risk(sources: dict, extracted: dict) -> dict:
    hits = sorted({term for d in sources["docs"] for term in RISK_TERMS if term in d["text"].lower()})
    return {"level": "high" if hits else "low", "signals": hits,
            "entities_at_risk": extracted["entities"] if hits else []}


@task
def synthesize_report(topic: str, summarized: dict, extracted: dict, risk: dict) -> dict:
    return {"title": f"Research report: {topic}",
            "key_points": [s["summary"] for s in summarized["summaries"]],
            "entities": extracted["entities"], "risk": risk}


@flow(name="research-report")
def research_report(topic: str, sources: list[str] | None = None) -> dict:
    documents = fetch_sources.submit(topic, sources if sources is not None else ["internal-wiki", "incident-db", "news-feed"])
    summaries = summarize.submit(documents)
    entities = extract_entities.submit(documents)
    risk = assess_risk.submit(documents, entities)
    return synthesize_report.submit(topic, summaries, entities, risk).result()
