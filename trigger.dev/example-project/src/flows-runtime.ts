// Runtime for portable flows (flows/*.yaml). src/trigger/flows.generated.ts calls defineFlow once
// per flow; regenerate it with agents/flowkit/trigger_codegen.py (flow_host.py does this on save).
//
// A flow becomes a parent task `flow-<name>` plus one task per step `flow-<name>-<step>`. Trigger.dev
// has no declared DAG, so the parent starts each step as soon as the steps it needs have completed
// (plain trigger + polling: a run can only have one *AndWait in flight, which would serialize
// independent branches) and records step -> run id in its metadata for the board.
// Steps run the shared Python executor (agents/flowkit) through the native bridge, so like
// agent-session they need the local development worker.
import { metadata, runs, task } from "@trigger.dev/sdk";
import { agentInput, runAgentSession } from "./trigger/agent-session.js";

type Json = string | number | boolean | null | Json[] | { [key: string]: Json };
type StepDef = { kind: "program" | "agent"; needs: string[]; retries: number; timeout_minutes: number } & Record<string, Json>;
export type FlowDef = { name: string; description: string; input: Record<string, Json>; steps: Record<string, StepDef> };
type StepPayload = { flowRunId: string; context: { input: Record<string, Json>; steps: Record<string, { output: Json }>; _flow_restart?: Json } };

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

export function defineFlow(def: FlowDef) {
  const steps = Object.fromEntries(Object.entries(def.steps).map(([id, step]) => [id, task({
    id: `flow-${def.name}-${id}`,
    description: `${def.name}: ${step.kind} step ${id}`,
    maxDuration: step.timeout_minutes * 60,
    retry: { maxAttempts: step.retries + 1 },
    run: async (payload: StepPayload, { ctx, signal }) => {
      if (ctx.environment.type !== "DEVELOPMENT") throw new Error("Flow steps run on the local development worker");
      metadata.set("title", `${def.name} · ${id}`);
      metadata.set("flowStep", { flow: def.name, step: id, kind: step.kind, flowRunId: payload.flowRunId });
      const result = await runAgentSession({ flow: def as unknown as Json, step: id, context: payload.context, runId: payload.flowRunId }, {
        module: "flowkit.stdio",
        environment: ctx.environment.type,
        signal,
        subscribe: (handler) => { const subscription = agentInput.on(handler); return () => subscription.off(); },
        publish: async (events, dropped) => {
          metadata.set("events", events);
          metadata.set("eventsDropped", dropped);
          await metadata.flush();
        },
      });
      return { output: result.output ?? null };
    },
  })]));

  const parent = task({
    id: `flow-${def.name}`,
    description: def.description || `Flow ${def.name}`,
    maxDuration: 86400,
    retry: { maxAttempts: 1 },
    run: async (params: Record<string, Json>, { ctx }) => {
      const { flowRestart: _flow_restart, flowExecution: _flow_execution, ...userInput } = params ?? {};
      const input = { ...def.input, ...userInput };
      metadata.set("title", String(input.topic ?? input.goal ?? def.name).slice(0, 512));
      metadata.set("flow", def.name);
      const runIds: Record<string, string> = {};
      const outputs: Record<string, Json> = {};
      const ids = Object.keys(def.steps);
      while (Object.keys(outputs).length < ids.length) {
        for (const id of ids) {
          const step = def.steps[id];
          if (runIds[id] || !step.needs.every((need) => need in outputs)) continue;
          const context = { input, steps: Object.fromEntries(step.needs.map((need) => [need, { output: outputs[need] }])), _flow_restart, _flow_execution };
          const handle = await steps[id].trigger({ flowRunId: ctx.run.id, context }, { tags: [`flowrun:${ctx.run.id}`] });
          runIds[id] = handle.id;
          metadata.set("steps", runIds);
          await metadata.flush();
        }
        for (const [id, runId] of Object.entries(runIds)) {
          if (id in outputs) continue;
          const run = await runs.retrieve(runId);
          if (run.isSuccess) outputs[id] = ((run.output as { output?: Json } | undefined)?.output) ?? null;
          else if (run.isCompleted) throw new Error(`step ${id} ended ${run.status}`);
        }
        await sleep(1000);
      }
      const leaves = ids.filter((id) => !ids.some((other) => def.steps[other].needs.includes(id)));
      return Object.fromEntries(leaves.map((id) => [id, outputs[id]]));
    },
  });
  return { parent, steps };
}
