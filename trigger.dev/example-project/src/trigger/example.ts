import { batch, logger, task, wait } from "@trigger.dev/sdk";

// 1. Plain task with retries
export const uppercase = task({
  id: "uppercase",
  retry: { maxAttempts: 3 },
  run: async (payload: { message: string }) => {
    logger.info("uppercasing", { message: payload.message });
    return { upper: payload.message.toUpperCase() };
  },
});

// 2. Fan-out/fan-in "pipeline": plan -> (research, draft in parallel) -> review
export const research = task({
  id: "research",
  run: async (payload: { topic: string }) => ({ notes: `notes about ${payload.topic}` }),
});

export const draft = task({
  id: "draft",
  run: async (payload: { topic: string }) => ({ draft: `first draft about ${payload.topic}` }),
});

export const agentPipeline = task({
  id: "agent-pipeline",
  run: async (payload: { message: string }) => {
    const topic = payload.message;
    // Parallel children must go through batch.*AndWait - Promise.all over triggerAndWait
    // fails with TASK_DID_CONCURRENT_WAIT.
    const { runs } = await batch.triggerByTaskAndWait([
      { task: research, payload: { topic } },
      { task: draft, payload: { topic } },
    ]);
    const [r, d] = runs;
    if (!r.ok || !d.ok) throw new Error("a child run failed");
    return { final: `${d.output.draft} (reviewed with ${r.output.notes})` };
  },
});

// 3. Long-running "agent loop" with waits (self-hosted has no checkpoints: the
//    container stays alive during wait.for instead of being suspended)
export const agentLoop = task({
  id: "agent-loop",
  run: async (payload: { message: string }) => {
    const history: string[] = [];
    for (let turn = 0; turn < 3; turn++) {
      // In a real agent this is where you'd call your (on-prem) LLM and tools.
      history.push(`turn ${turn}: thinking about ${payload.message}`);
      await wait.for({ seconds: 1 });
    }
    return { history };
  },
});
