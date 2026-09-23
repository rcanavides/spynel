# Roadmap

Short-lived orientation for contributors. It records where the work stands and the invariants that govern what comes next; it is not a design spec.

## Completed

- C7.6a
- C7.6b
- C8.2 — 7b6a711
- C8.3 — 0ae4175
- C8.4a — current until committed

## Next

- C9-F
- C9+C10
- C11
- C12
- C13

## Forward rules

1. Agents write content; Spynel changes durable workflow state.
2. Every Developer receives an isolated workspace.
3. Spynel owns Git integration.
4. Every execution has a launch_id.
5. Every approval targets an exact digest.
6. Every review targets an exact SHA.
7. DONE requires evidence, not agent claim.

## Boundaries

- Agent Zero stays a chat-role harness via ACP or an external client via MCP; it is never a second durable workflow authority.
- Jev is out of productive scope; a later shadow-only experiment remains possible.
