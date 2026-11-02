import { batch, logger, task } from "@trigger.dev/sdk";

// Research-report DAG, the same graph as hatchet/example-worker/research_dag.py:
//
//   fetch-sources ─┬─> summarize ────────────────┐
//                  └─> extract-entities ─┬───────┼─> synthesize-report
//                                        └─> assess-risk ─┘
//
// Trigger.dev has no declarative DAG: a parent task awaits its children in code. To run summarize
// in parallel with the extract-entities -> assess-risk chain, that chain becomes its own child task
// (entity-risk-branch), because a run can only have one wait in flight at a time.
// The task bodies are deterministic stubs: replace them with calls to your (on-prem) LLM and tools.

type Doc = { source: string; text: string };
const RISK_TERMS = ["outage", "breach", "lawsuit", "recall"];

export const fetchSources = task({
  id: "fetch-sources",
  retry: { maxAttempts: 3 },
  run: async (payload: { topic: string; sources: string[] }) => {
    // Real version: pull documents from search / a vector store / internal APIs.
    const docs: Doc[] = payload.sources.map((s) => ({
      source: s,
      text:
        `${s} on ${payload.topic}: Acme Corp and Globex reviewed the ${payload.topic} rollout. ` +
        `One region saw a short outage; Initech is evaluating the same approach.`,
    }));
    logger.info(`fetched ${docs.length} documents`);
    return { docs };
  },
});

export const summarize = task({
  id: "summarize",
  retry: { maxAttempts: 2 },
  // Real version: one LLM call per document (or one map-reduce call).
  run: async (payload: { docs: Doc[] }) => ({
    summaries: payload.docs.map((d) => ({ source: d.source, summary: d.text.split(". ")[0] })),
  }),
});

export const extractEntities = task({
  id: "extract-entities",
  retry: { maxAttempts: 2 },
  run: async (payload: { docs: Doc[] }) => {
    // Real version: an LLM call with a structured-output schema.
    const found = new Set(payload.docs.flatMap((d) => d.text.match(/\b[A-Z][a-z]+(?: Corp)?\b/g) ?? []));
    found.delete("One");
    return { entities: [...found].sort() };
  },
});

export const assessRisk = task({
  id: "assess-risk",
  run: async (payload: { docs: Doc[]; entities: string[] }) => {
    const hits = RISK_TERMS.filter((t) => payload.docs.some((d) => d.text.toLowerCase().includes(t)));
    return {
      level: hits.length ? "high" : "low",
      signals: hits,
      entitiesAtRisk: hits.length ? payload.entities : [],
    };
  },
});

// The sequential branch extract-entities -> assess-risk, run as one child of the pipeline.
export const entityRiskBranch = task({
  id: "entity-risk-branch",
  run: async (payload: { docs: Doc[] }) => {
    const { entities } = await extractEntities.triggerAndWait(payload).unwrap();
    const risk = await assessRisk.triggerAndWait({ docs: payload.docs, entities }).unwrap();
    return { entities, risk };
  },
});

export const researchReport = task({
  id: "research-report",
  run: async (payload: { topic: string; sources?: string[] }) => {
    const sources = payload.sources ?? ["internal-wiki", "incident-db", "news-feed"];
    const { docs } = await fetchSources.triggerAndWait({ topic: payload.topic, sources }).unwrap();

    const { runs } = await batch.triggerByTaskAndWait([
      { task: summarize, payload: { docs } },
      { task: entityRiskBranch, payload: { docs } },
    ]);
    const [s, b] = runs;
    if (!s.ok || !b.ok) throw new Error("a child run failed");

    // Real version: a final LLM call that writes the report from these structured parts.
    return {
      title: `Research report: ${payload.topic}`,
      keyPoints: s.output.summaries.map((x) => x.summary),
      entities: b.output.entities,
      risk: b.output.risk,
    };
  },
});
