"""Trigger the example workflows and print their results (run while worker.py is running)."""

from research_dag import ResearchInput, research_report
from worker import MessageInput, agent_loop, agent_pipeline, uppercase

if __name__ == "__main__":
    print("uppercase  ->", uppercase.run(MessageInput(message="hello hatchet")))
    print("pipeline   ->", agent_pipeline.run(MessageInput(message="offline agents")))
    print("agent-loop ->", agent_loop.run(MessageInput(message="durability")))
    print("research   ->", research_report.run(ResearchInput(topic="edge caching")))
