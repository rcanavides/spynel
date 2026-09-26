# Tasks and goals

Spynel stores orchestration state as human-readable Markdown with machine-readable YAML front matter. External coding harnesses do the reasoning and implementation; Spynel owns deterministic claiming, lifecycle, review policy, recovery, and dispatch.

## Tasks

A task is one finite, independently verifiable objective. The normal reviewed lifecycle is:

```text
todo → working → review → reviewing → done
```

`harness.reviews` controls review policy. Its default, `skip-trivial`, requires each task to carry a boolean `review_required`: broad, risky, hard-to-reverse, security-sensitive, or materially uncertain work normally receives independent review, while a minor localized reversible change may complete directly with proportionate evidence and residual uncertainty. `always` forces review and `never` forces the direct-evidence path. Missing or malformed task policy fails safe under the default mode.

Task documents retain their objective, constraints, evidence, current status, and a timestamped `## Progress` handoff log. A claimed phase uses a persisted lease, so restart and stale-session recovery can continue from durable evidence rather than assuming that a provider response completed the work.

With the default `orchestrator.workspace_isolation: shared` the lifecycle above is exactly what runs: agents share one workspace checkout and Spynel records additive launch evidence (a durable launch identity per execution, provider admission, and provider terminal) without changing any session or transition behavior.

With `orchestrator.workspace_isolation: git-worktree`, new task implementation and review claims execute in isolated detached Git worktrees while the same Markdown lifecycle and transitions remain the status authority. The agent works inside its worktree and still requests its transition through the durable document path; Spynel accepts or rejects the request using mechanical evidence only: the canonical result captured from the worktree's working content, the frozen `orchestrator.checks` set run by Spynel against that exact result, and an independent reviewer pinned to the exact reviewed commit whose read-only source checkout must stay untouched. Every route into review binds its request to the current finalized implementation launch, canonical result SHA, and original target SHA; a historical request cannot select an older result. An accepted review integrates the result into the target branch fast-forward-only before the task's `done` transition has any effect. When review is not required, direct completion follows the same integration-before-`done` rule whenever the canonical result SHA differs from the original target SHA, so even a no-change continuation of earlier work integrates before the task completes. A target that advanced meanwhile returns the task to `todo` with its result retained for porting. Agents must not commit or move the worktree HEAD — Spynel captures content itself, and an agent's Git state is evidence, never authority. Failed or interrupted isolated launches resume their capture, missing checks, or reviewer without duplicating completed work; a launch that cannot complete records its failure and the task returns to `todo`.

## Goals

A goal is a longer-lived outcome with measurable success criteria. Planning creates a numbered round of finite linked tasks and records the exact cohort. After that cohort settles—or at an explicitly configured checkpoint—a fresh goal review compares cumulative evidence with every success criterion.

Goal review can complete the goal, wait for a concrete external condition, abandon it, or return it to planning for another round. Task completion is evidence for this decision; it never completes a goal automatically.

## Waiting, recovery, and review

Waiting is reserved for a precise external condition. An optional RFC 3339 `wake_at` lets Spynel return due work to its queue; otherwise human input or another external change must resolve the condition.

Every implementation, planning, and review claim is journaled before its queue file moves. Persisted leases, owner fencing, bounded recovery, and status-folder checks prevent duplicate ownership across restarts. Independent reviewers start from the durable artifact rather than the implementation session. Reviewers may repair only trivial localized findings themselves; broader findings return to implementation.

The elected primary also runs a bounded semantic heartbeat worker. The framework only starts one non-overlapping stable-session turn and ignores its provider output. The agent uses ordinary Spynel CLI inspection plus the task/goal Markdown contracts to make and journal evidence-backed safe repairs; no heartbeat result, finding, incident, health, escalation, fallback, or retry state exists. Ordinary terminal and actionable unscheduled waiting task transitions invoke the dedicated notification agent directly.

Heartbeat and notification workers are ordinary asynchronous jobs. Provider admission is not completion: their job histories remain live through delayed output until a real terminal final/error event, cancellation, or bounded timeout. The first terminal boundary settles the job once; late duplicates are ignored, and heartbeat scheduling remains non-overlapping until provider release.

## Creating and inspecting work

In a conversation, `/task` and `/goal` ask the communication assistant to create or refine framework-compliant work. Dedicated `/tasks`, `/goals`, `/status`, and `/jobs` commands inspect bounded durable state without invoking a harness. Scripts can use the matching `spynel task`, `spynel goal`, `spynel tasks`, and `spynel goals` commands.

Selected task outcomes may carry an authorized notification origin. Every terminal or actionable unscheduled waiting transition directly starts one ordinary notification-agent job with the task and `spynel notify` guidance. The agent decides, calls the ordinary CLI when useful, and edits task progress with its send, skip, or CLI-failure result. Separately, the heartbeat agent may decide from bounded progress that an inactive user wait merits a reminder, use `--recent-authorized`, and journal the result. Spynel provides recent authorized channel resolution and ordinary delivery but no reminder scheduler or state. A user reply remains ordinary conversation context rather than a hidden workflow acknowledgement.

For the state machine, lease, recovery, notification, and primary-owner invariants, see [architecture](architecture.md). For command syntax and output contracts, see [plain CLI and automation](cli.md). For route and review settings, see [configuration](configuration.md).
