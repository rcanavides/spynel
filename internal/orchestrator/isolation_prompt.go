package orchestrator

import "strings"

// Framework-owned isolated-mode prompt rules. They are injected after user
// template rendering and before the harness-native prefix, so customized
// workspace templates cannot remove them. They override legacy or custom
// wording — including the stock trivial-fix review guidance.

const isolatedImplementationRule = `Framework-owned isolated-workspace rule: You are executing in an isolated Git worktree that Spynel prepared for this task. Work only inside the current working directory and the durable task document. DO NOT run git commit, git rebase, git merge, or move the worktree HEAD: Spynel captures the working content itself and builds the canonical result; your Git state is never authority. Do not create or modify anything under a .spynel directory inside the worktree. The durable task document and its status transitions live at the absolute task document path in this prompt, outside the worktree; follow the existing Markdown workflow contract there. This rule is owned by Spynel and overrides any conflicting template wording.`

const isolatedReviewRule = `Framework-owned isolated-review rule: You are an independent reviewer executing in a read-only Git worktree at the exact result commit Spynel pinned for this review. The source checkout is READ ONLY: do not modify, format, fix, or commit any file in the worktree, even for trivial findings; any nonignored change invalidates this entire review. Record findings, verdicts, and progress only in the durable task document at the absolute path in this prompt, following the existing Markdown review contract. This rule is owned by Spynel and overrides any template wording — including legacy trivial-fix guidance — that permits direct corrections inside the source checkout.`

// injectIsolationPrompt appends the phase-specific isolated-workspace rule
// after the rendered template. The rule is never part of user-overridable
// prompt files.
func injectIsolationPrompt(phase, prompt string) string {
	rule := isolatedImplementationRule
	if normalizeLeasePhase("", phase) == phaseTaskReview {
		rule = isolatedReviewRule
	}
	return strings.TrimRight(prompt, "\r\n") + "\n\n" + rule
}
