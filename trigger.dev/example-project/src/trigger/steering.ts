import { logger, metadata, streams, task } from "@trigger.dev/sdk";

// Human-steerable, interruptible agent.
//  - Steer: send on the "steer" input stream from your backend/UI:
//      await steer.send(runId, { action: "redirect", instruction: "..." })   // change the goal
//      await steer.send(runId, { action: "stop" })                           // finish early
//  - Hard interrupt: cancel the run (dashboard "Cancel" or runs.cancel(runId)).
//  - Live progress: run metadata ("transcript") is visible in the dashboard and via Realtime.
// Every step is a span (logger.trace) so it shows up in the run's trace view.
export type SteerMessage = { action: "redirect" | "stop"; instruction?: string };
export const steer = streams.input<SteerMessage>({ id: "steer" });

export const steerableAgent = task({
  id: "steerable-agent",
  maxDuration: 1800,
  run: async (payload: { goal: string; turns?: number; stepSeconds?: number }) => {
    let goal = payload.goal;
    const transcript: string[] = [];
    const record = (line: string) => {
      transcript.push(line);
      metadata.set("transcript", transcript);
    };

    for (let turn = 0; turn < (payload.turns ?? 6); turn++) {
      await logger.trace("agent.step", async (span) => {
        span.setAttributes({ "agent.turn": turn, "agent.goal": goal });
        // In a real agent: call the (on-prem) LLM / tools here.
        record(`turn ${turn}: working towards "${goal}"`);
      });

      // Give a human stepSeconds to steer before the next step. `.once()` keeps the container
      // alive; for long human waits use `steer.wait({ timeout: "1h" })`, which suspends instead.
      const message = await steer.once({ timeoutMs: (payload.stepSeconds ?? 2) * 1000 });
      if (!message.ok) continue;

      logger.info("human steer", { ...message.output });
      if (message.output.action === "stop") {
        record(`turn ${turn}: stopped by human`);
        return { goal, transcript, stoppedEarly: true };
      }
      if (message.output.action === "redirect" && message.output.instruction) {
        goal = message.output.instruction;
        record(`turn ${turn}: human redirected goal to "${goal}"`);
      }
    }

    return { goal, transcript, stoppedEarly: false };
  },
});
