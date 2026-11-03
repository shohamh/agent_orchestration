// GENERATED from flows/*.yaml by agents/flowkit/trigger_codegen.py. Do not edit; edit the YAML.
import { defineFlow } from "../flows-runtime.js";

const hello_flow = defineFlow({"name": "hello-flow", "description": "Two-step smoke test", "input": {"who": "world"}, "steps": {"greet": {"kind": "program", "needs": [], "retries": 0, "timeout_minutes": 60, "run": "python -c \"print('hello {{ input.who }}')\""}, "reply": {"kind": "agent", "needs": ["greet"], "retries": 0, "timeout_minutes": 60, "agent": "demo", "prompt": "Reply to this greeting: {{ steps.greet.output.stdout }}\n", "review": false}}});
export const flow_hello_flow = hello_flow.parent;
export const flow_hello_flow__greet = hello_flow.steps["greet"];
export const flow_hello_flow__reply = hello_flow.steps["reply"];

const research_flow = defineFlow({"name": "research-flow", "description": "Research a topic from several sources and write a short report.", "input": {"topic": "edge caching", "sources": "internal-wiki,incident-db,news-feed"}, "steps": {"fetch_sources": {"kind": "program", "needs": [], "retries": 0, "timeout_minutes": 60, "run": "python {flow_dir}/steps/fetch_sources.py {{ input.topic }} {{ input.sources }}"}, "extract_entities": {"kind": "program", "needs": ["fetch_sources"], "retries": 0, "timeout_minutes": 60, "run": "python {flow_dir}/steps/extract_entities.py"}, "summarize": {"kind": "agent", "needs": ["fetch_sources"], "retries": 0, "timeout_minutes": 60, "agent": "demo", "prompt": "Summarize what these sources say about {{ input.topic }}.\n{{ steps.fetch_sources.output.docs }}\n", "review": false}, "assess_risk": {"kind": "program", "needs": ["fetch_sources", "extract_entities"], "retries": 0, "timeout_minutes": 60, "run": "python {flow_dir}/steps/assess_risk.py"}, "write_report": {"kind": "agent", "needs": ["summarize", "extract_entities", "assess_risk"], "retries": 0, "timeout_minutes": 60, "agent": "demo", "prompt": "Write a short research report on {{ input.topic }}.\nSummary: {{ steps.summarize.output }}\nEntities: {{ steps.extract_entities.output.entities }}\nRisk: {{ steps.assess_risk.output }}\n", "review": false}}});
export const flow_research_flow = research_flow.parent;
export const flow_research_flow__fetch_sources = research_flow.steps["fetch_sources"];
export const flow_research_flow__extract_entities = research_flow.steps["extract_entities"];
export const flow_research_flow__summarize = research_flow.steps["summarize"];
export const flow_research_flow__assess_risk = research_flow.steps["assess_risk"];
export const flow_research_flow__write_report = research_flow.steps["write_report"];

const ui_made_flow = defineFlow({"name": "ui-made-flow", "description": "What this flow does", "input": {"topic": "something"}, "steps": {"gather": {"kind": "program", "needs": [], "retries": 0, "timeout_minutes": 60, "run": "python -c \"print('gathering {{ input.topic }}')\""}, "think": {"kind": "agent", "needs": ["gather"], "retries": 0, "timeout_minutes": 60, "agent": "demo", "prompt": "Work on {{ input.topic }} using:\n{{ steps.gather.output.stdout }}\n", "review": false}}});
export const flow_ui_made_flow = ui_made_flow.parent;
export const flow_ui_made_flow__gather = ui_made_flow.steps["gather"];
export const flow_ui_made_flow__think = ui_made_flow.steps["think"];
