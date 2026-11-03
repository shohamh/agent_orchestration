import assert from "node:assert/strict";
import { mkdtemp, mkdir, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import test from "node:test";
import { runAgentSession } from "./trigger/agent-session.js";

async function fixture(source: string) {
  const root = await mkdtemp(path.join(tmpdir(), "agent-bridge-"));
  await mkdir(path.join(root, "agentdriver"));
  await writeFile(path.join(root, "agentdriver", "stdio.py"), source);
  return root;
}

test("bridge routes inputs, persists events and flushes terminal output", async () => {
  const root = await fixture(`import sys,json\nstart=json.loads(input())\nprint(json.dumps({'type':'agent','text':start['goal']}),flush=True)\nmessage=json.loads(input())\nprint(json.dumps({'type':'status','state':message['type']}),flush=True)\n`);
  const snapshots: unknown[][] = [];
  let send: (data: object) => void = () => {};
  try {
    await runAgentSession({ goal: "hello", agent: "demo" }, {
      environment: "DEVELOPMENT", signal: new AbortController().signal, root,
      python: process.env.AGENT_PYTHON!,
      subscribe: (handler) => { send = handler; return () => {}; },
      publish: async (events) => {
        snapshots.push(structuredClone(events));
        if (events.some((event) => event.type === "agent")) send({ type: "done" });
      },
    });
    assert.deepEqual(snapshots.at(-1), [{ type: "agent", text: "hello" }, { type: "status", state: "done" }]);
  } finally { await rm(root, { recursive: true, force: true }); }
});

test("bridge abort kills the native Python process", async () => {
  const root = await fixture(`import os,json,time\ninput()\nprint(json.dumps({'type':'agent','pid':os.getpid()}),flush=True)\ntime.sleep(60)\n`);
  const controller = new AbortController();
  let pid = 0;
  try {
    await assert.rejects(runAgentSession({ goal: "hello", agent: "demo" }, {
      environment: "DEVELOPMENT", signal: controller.signal, root,
      python: process.env.AGENT_PYTHON!, subscribe: () => () => {},
      publish: async (events) => { pid = Number(events[0]?.pid); controller.abort(); },
    }), /abort|cancel/i);
    assert.ok(pid > 0);
    assert.throws(() => process.kill(pid, 0));
  } finally { await rm(root, { recursive: true, force: true }); }
});

test("bridge rejects production before subscribing or spawning", async () => {
  await assert.rejects(runAgentSession({ goal: "hello", agent: "demo" }, {
    environment: "PRODUCTION", signal: new AbortController().signal,
    subscribe: () => { throw new Error("must not subscribe"); }, publish: async () => {},
  }), /development/i);
});

test("bridge bounds UTF-8 transcript metadata and reports trimmed history", async () => {
  const root = await fixture(`import json\ninput()\nfor i in range(240): print(json.dumps({'type':'agent','seq':i,'text':'\\u05d0'*150}),flush=True)\n`);
  let last: unknown[] = [], dropped = 0;
  try {
    await runAgentSession({ goal: "history", agent: "demo" }, {
      environment: "DEVELOPMENT", signal: new AbortController().signal, root,
      python: process.env.AGENT_PYTHON!, subscribe: () => () => {},
      publish: async (events, count) => {
        assert.ok(Buffer.byteLength(JSON.stringify(events), "utf8") <= 24 * 1024);
        assert.ok(events.length <= 200);
        last = structuredClone(events); dropped = count;
      },
    });
    assert.ok(dropped > 0);
    assert.equal(last.length + dropped, 240);
    assert.equal((last.at(-1) as {seq: number}).seq, 239);
  } finally { await rm(root, { recursive: true, force: true }); }
});

test("command snapshot survives the rolling transcript", async () => {
  const root = await fixture(`import json\ninput()\nprint(json.dumps({'type':'commands','commands':[{'name':'review','description':'Review'}]}),flush=True)\nprint(json.dumps({'type':'acp_state','usage':{'used':3,'size':10}}),flush=True)\nfor i in range(240): print(json.dumps({'type':'user','text':str(i)}),flush=True)\n`);
  let commands: unknown, state: unknown;
  try {
    await runAgentSession({ goal: "history", agent: "demo" }, {
      environment: "DEVELOPMENT", signal: new AbortController().signal, root,
      python: process.env.AGENT_PYTHON!, subscribe: () => () => {},
      publish: async (_events, _dropped, latestCommands, latestState) => { commands = latestCommands; state = latestState; },
    });
    assert.deepEqual(commands, [{ name: 'review', description: 'Review' }]);
    assert.deepEqual(state, { type: 'acp_state', usage: { used: 3, size: 10 } });
  } finally { await rm(root, { recursive: true, force: true }); }
});

test("300 chunks retain opening user, complete message and pending approval", async () => {
  const root = await fixture(`import json\ninput()\ndef emit(event): print(json.dumps(event),flush=True)\nemit({'type':'user','text':'opening goal'})\nfor i in range(300): emit({'type':'agent','message_id':'m1','text':str(i)+','})\nemit({'type':'tool','id':'t1','name':'shell','summary':'inspect','status':'running'})\nemit({'type':'agent','message_id':'m2','text':'before steering'})\nemit({'type':'user','text':'steer'})\nemit({'type':'tool','id':'t1','status':'ok','output':'result'})\nemit({'type':'agent','message_id':'m2','text':'after steering'})\nemit({'type':'approval','id':'p1','options':[{'id':'allow'}]})\n`);
  let last: Array<Record<string, unknown>> = [], dropped = -1;
  try {
    await runAgentSession({ goal: "chunks", agent: "demo" }, {
      environment: "DEVELOPMENT", signal: new AbortController().signal, root,
      python: process.env.AGENT_PYTHON!, subscribe: () => () => {},
      publish: async (events, count) => { last = structuredClone(events); dropped = count; },
    });
    assert.equal(dropped, 0);
    assert.equal(last[0].text, 'opening goal');
    assert.equal(last[1].text, Array.from({length: 300}, (_, i) => `${i},`).join(''));
    assert.deepEqual(last.map(e => e.type), ['user','agent','tool','agent','user','agent','approval']);
    assert.deepEqual(last[2], {type:'tool',id:'t1',name:'shell',summary:'inspect',status:'ok',output:'result'});
    assert.equal(last[3].text, 'before steering');
    assert.equal(last[5].text, 'after steering');
    assert.equal(last[6].id, 'p1');
  } finally { await rm(root, { recursive: true, force: true }); }
});
