"""Step 3 - Create the AgentCore Memory resource used by the orchestrator.

Short-term memory : every conversation turn is stored as an event (automatic).
Long-term memory  : three strategies extract durable knowledge asynchronously:
    /preferences/{actorId}/            user preferences (tone, audience, length ...)
    /facts/{actorId}/                  facts the user told us
    /summaries/{actorId}/{sessionId}/  rolling session summaries
Takes ~2-3 minutes to become ACTIVE.
"""
from bedrock_agentcore.memory import MemoryClient

from common import PROJECT, REGION, load_state, save_state

NAME = f"{PROJECT.replace('-', '_')}_memory"

STRATEGIES = [
    {"userPreferenceMemoryStrategy": {"name": "UserPreferences", "namespaceTemplates": ["/preferences/{actorId}/"]}},
    {"semanticMemoryStrategy": {"name": "UserFacts", "namespaceTemplates": ["/facts/{actorId}/"]}},
    {"summaryMemoryStrategy": {"name": "SessionSummaries", "namespaceTemplates": ["/summaries/{actorId}/{sessionId}/"]}},
]

if __name__ == "__main__":
    client = MemoryClient(region_name=REGION)
    existing = load_state().get("memory_id")
    if existing:
        print(f"Memory already in state.json: {existing} (status {client.get_memory_status(existing)})")
    else:
        for m in client.list_memories():
            if m.get("id", "").startswith(NAME):
                existing = m["id"]
                print(f"Found existing memory {existing}")
        if not existing:
            print(f"==> Creating memory {NAME} (2-3 min)...")
            mem = client.create_memory_and_wait(
                name=NAME,
                description="Multi-agent POC memory: preferences, facts, session summaries",
                strategies=STRATEGIES,
                event_expiry_days=30,
            )
            existing = mem["id"]
        save_state(memory_id=existing)
    print(f"MEMORY_ID={existing}")
