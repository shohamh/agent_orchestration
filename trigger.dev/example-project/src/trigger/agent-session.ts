import { spawn, type ChildProcess } from "node:child_process";
import { fileURLToPath } from "node:url";
import path from "node:path";
import { createInterface } from "node:readline";
import { existsSync } from "node:fs";
import { metadata, streams, task } from "@trigger.dev/sdk";

type SessionConfig = { goal: string; agent: string; workspace?: string; simulateFailures?: number };
type Json = string | number | boolean | null | Json[] | { [key: string]: Json };
type SessionEvent = Record<string, Json>;
type Runtime = {
  environment: string;
  signal: AbortSignal;
  root?: string;
  python?: string;
  subscribe: (handler: (data: object) => void) => () => void;
  publish: (events: SessionEvent[], dropped: number, commands?: Json[]) => Promise<void>;
  /** Python module to run (default agentdriver.stdio; flow steps use flowkit.stdio). */
  module?: string;
};

export const agentInput = streams.input<object>({ id: "agent-input" });

function defaultAgentRoot(): string {
  // trigger dev bundles task files under .trigger/, so their relative depth varies.
  let directory = path.dirname(fileURLToPath(import.meta.url));
  while (true) {
    const candidate = path.join(directory, "agents");
    if (existsSync(path.join(candidate, "agentdriver", "stdio.py"))) return candidate;
    const parent = path.dirname(directory);
    if (parent === directory) throw new Error("Cannot find agents/; set AGENT_ROOT to its absolute path");
    directory = parent;
  }
}

async function killTree(child: ChildProcess): Promise<void> {
  if (child.exitCode !== null || !child.pid) return;
  if (process.platform === "win32") {
    await new Promise<void>((resolve) => {
      const killer = spawn("taskkill", ["/pid", String(child.pid), "/T", "/F"], { windowsHide: true });
      killer.once("error", () => { child.kill(); resolve(); });
      killer.once("close", () => resolve());
    });
  } else {
    try { process.kill(-child.pid, "SIGKILL"); } catch { child.kill("SIGKILL"); }
  }
}

/** Native bridge. Consecutive message chunks compact into <=6000-char segments;
 * tool updates merge at their original position. Metadata retains at most 200
 * compacted events / 24 KiB of JSON (UTF-8).
 * Older events are discarded and eventsDropped records the count. A single event larger
 * than the budget fails explicitly, so an approval/question is never silently corrupted.
 * Title is capped to 512 chars, leaving headroom under a 32 KiB metadata budget.
 */
export async function runAgentSession(payload: SessionConfig | Record<string, Json>, runtime: Runtime): Promise<{ agent?: Json; eventsDropped: number; eventCount: number; output?: Json }> {
  if (runtime.environment !== "DEVELOPMENT") throw new Error("Agent sessions require a development environment and native trigger dev");
  runtime.signal.throwIfAborted();
  const root = runtime.root ?? process.env.AGENT_ROOT ?? defaultAgentRoot();
  const python = runtime.python ?? process.env.AGENT_PYTHON ?? path.join(root, ".venv", process.platform === "win32" ? "Scripts/python.exe" : "bin/python");
  const child = spawn(python, ["-u", "-m", runtime.module ?? "agentdriver.stdio"], {
    cwd: root, windowsHide: true, detached: process.platform !== "win32",
    env: { ...process.env, PYTHONUTF8: "1", PYTHONUNBUFFERED: "1" },
    stdio: ["pipe", "pipe", "pipe"],
  });
  const events: SessionEvent[] = [];
  let previousEvent: SessionEvent | undefined;
  let commands: Json[] | undefined;
  let result: SessionEvent | undefined; // flowkit.stdio's final {"type": "result", "output"}
  let dropped = 0, stderr = "", inputError: Error | undefined;
  const trimEvents = () => {
    // Reserve metadata space for the persistent command snapshot and the title.
    const budget = Math.min(24 * 1024, 29 * 1024 - Buffer.byteLength(JSON.stringify(commands ?? []), "utf8"));
    while (events.length && (events.length > 200 || Buffer.byteLength(JSON.stringify(events), "utf8") > budget)) {
      events.shift(); dropped++;
    }
  };
  let aborted = false;
  let killPromise: Promise<void> | undefined;
  const abort = () => { aborted = true; killPromise ??= killTree(child); };
  runtime.signal.addEventListener("abort", abort, { once: true });
  // Cancellation may race between the preflight check and listener registration.
  if (runtime.signal.aborted) abort();
  child.stderr.on("data", (chunk: Buffer) => { stderr = (stderr + chunk.toString("utf8")).slice(-8000); });
  child.stdin.on("error", (error) => { inputError = error; });
  const exited = new Promise<number | null>((resolve, reject) => {
    child.once("error", reject);
    child.once("close", (code) => resolve(code));
  });
  // Attach the rejection handler immediately; output parsing can still be in progress.
  void exited.catch(() => {});
  let unsubscribe = () => {};
  try {
    child.stdin.write(JSON.stringify({ type: "start", ...payload }) + "\n");
    unsubscribe = runtime.subscribe((message) => {
      if (!child.stdin.destroyed && !aborted) child.stdin.write(JSON.stringify(message) + "\n");
    });
    for await (const line of createInterface({ input: child.stdout, crlfDelay: Infinity })) {
      if (!line.trim()) continue;
      if (Buffer.byteLength(line, "utf8") + 2 > 24 * 1024) throw new Error("Agent event exceeds the 24 KiB transcript limit");
      const event: SessionEvent = JSON.parse(line);
      if (!event || typeof event !== "object" || Array.isArray(event) || typeof event.type !== "string") throw new Error("Invalid agent event JSON");
      if (event.type === "result") { result = event; continue; }
      if (event.type === "commands" && Array.isArray(event.commands)) {
        commands = event.commands;
        previousEvent = event;
        trimEvents();
        await runtime.publish(events, dropped, commands);
        continue;
      }
      if (Buffer.byteLength(line, "utf8") + 2 > 29 * 1024 - Buffer.byteLength(JSON.stringify(commands ?? []), "utf8"))
        throw new Error("Agent event exceeds the transcript space available alongside commands");
      const last = events.at(-1);
      if (event.type === "agent" && typeof event.message_id === "string" && typeof event.text === "string" &&
          previousEvent?.type === "agent" && previousEvent.message_id === event.message_id &&
          last?.type === "agent" && last.message_id === event.message_id && typeof last.text === "string") {
        const combined = last.text + event.text;
        last.text = combined.slice(0, 6000);
        for (let offset = 6000; offset < combined.length; offset += 6000) {
          events.push({ ...event, text: combined.slice(offset, offset + 6000) });
        }
      } else if (event.type === "tool" && typeof event.id === "string") {
        const existing = [...events].reverse().find((entry) => entry.type === "tool" && entry.id === event.id);
        if (existing) {
          const seq = existing.seq, ts = existing.ts;
          Object.assign(existing, event);
          if (seq !== undefined) existing.seq = seq;
          if (ts !== undefined) existing.ts = ts;
        } else events.push(event);
      } else events.push(event);
      previousEvent = event;
      trimEvents();
      await runtime.publish(events, dropped, commands);
    }
    const code = await exited;
    if (aborted) throw new Error("Agent session aborted");
    if (code !== 0) throw new Error(`Native agent exited (${code}): ${stderr || inputError?.message || "no stderr"}`);
    await runtime.publish(events, dropped, commands);
    return { agent: (payload as Record<string, Json>).agent, eventsDropped: dropped, eventCount: events.length, output: result?.output };
  } finally {
    unsubscribe();
    runtime.signal.removeEventListener("abort", abort);
    child.stdin.destroy();
    await (killPromise ?? killTree(child));
    await exited.catch(() => {});
  }
}

export const agentSession = task({
  id: "agent-session",
  maxDuration: 86400,
  retry: { maxAttempts: 1 },
  run: async (payload: SessionConfig, { ctx, signal }) => {
    if (ctx.environment.type !== "DEVELOPMENT") throw new Error("Use the development backend for local coding sessions");
    metadata.set("title", payload.goal.slice(0, 512));
    metadata.set("agent", payload.agent);
    return runAgentSession(payload, {
      environment: ctx.environment.type,
      signal,
      subscribe: (handler) => { const subscription = agentInput.on(handler); return () => subscription.off(); },
      publish: async (events, dropped, commands) => {
        metadata.set("events", events);
        metadata.set("eventsDropped", dropped);
        if (commands !== undefined) metadata.set("commands", commands);
        await metadata.flush();
      },
    });
  },
});
