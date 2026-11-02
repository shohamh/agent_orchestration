// Exercise steering and cancellation against the running stack.
//   TRIGGER_SECRET_KEY=tr_prod_... TRIGGER_API_URL=http://localhost:8030 npx tsx scripts/demo-steer.ts
import { runs, tasks } from "@trigger.dev/sdk";
import { steer, type steerableAgent } from "../src/trigger/steering.js";

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function waitForStatus(runId: string, done: (status: string) => boolean) {
  for (;;) {
    const run = await runs.retrieve(runId);
    if (done(run.status)) return run;
    await sleep(1000);
  }
}

// 1. Steer: redirect the goal mid-run, then stop it early.
const handle = await tasks.trigger<typeof steerableAgent>("steerable-agent", {
  goal: "summarize logs",
  turns: 30,
});
await waitForStatus(handle.id, (s) => s === "EXECUTING");
await sleep(3000);
await steer.send(handle.id, { action: "redirect", instruction: "focus on errors only" });
await sleep(4000);
await steer.send(handle.id, { action: "stop" });
const steered = await waitForStatus(handle.id, (s) => ["COMPLETED", "FAILED", "CRASHED"].includes(s));
console.log("steered   ->", steered.status, JSON.stringify(steered.output));

// 2. Hard interrupt: cancel a running agent.
const victim = await tasks.trigger<typeof steerableAgent>("steerable-agent", { goal: "long job", turns: 100 });
await waitForStatus(victim.id, (s) => s === "EXECUTING");
await sleep(3000);
await runs.cancel(victim.id);
const cancelled = await waitForStatus(victim.id, (s) => s === "CANCELED");
console.log("cancelled ->", cancelled.status);
