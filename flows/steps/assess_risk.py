"""assess_risk: flag risk terms in the documents and the entities involved."""

from flowstep import context, log, output

RISK_TERMS = ("outage", "breach", "lawsuit", "recall")

ctx = context()["steps"]
docs = ctx["fetch_sources"]["output"]["docs"]
entities = ctx["extract_entities"]["output"]["entities"]
hits = sorted({t for d in docs for t in RISK_TERMS if t in d["text"].lower()})
for term in RISK_TERMS:
    log(f"{term:>8}: {'FOUND' if term in hits else 'no'}")
level = "high" if hits else "low"
log(f"risk level: {level}")
output({"level": level, "signals": hits, "entities_at_risk": entities if hits else []})
