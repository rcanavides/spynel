// Package execws owns the execution-plane boundary between orchestrator
// workflow state and isolated Git workspaces: worktree lifecycle, canonical
// result capture, system-owned checks, and ff-only target integration.
//
// Control-plane durable workflow state (Markdown, leases, facts) never depends
// on an execution-plane worktree surviving. Provider process lifecycle remains
// in internal/harness.
package execws

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base32"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Backend kinds. C9-F ships the git-worktree backend; shared mode uses no
// backend at all.
const (
	BackendGitWorktree = "git-worktree"
)

// Workspace identity kinds mirror the backend kinds.
const (
	WorkspaceKindGitWorktree = BackendGitWorktree
	WorkspaceKindShared      = "shared"
)

// Check outcomes.
const (
	CheckOutcomePassed  = "passed"
	CheckOutcomeFailed  = "failed"
	CheckOutcomeMutated = "mutated"
)

// Integration mechanisms recorded in durable evidence.
const (
	IntegrationFFWorkingTree = "ff-working-tree"
	IntegrationCASUpdateRef  = "cas-update-ref"
	IntegrationObserved      = "observed"
)

// Ref identifies one workspace managed by a backend.
type Ref struct {
	ID   string `json:"id"`
	Kind string `json:"kind"`
}

// Spec is the creation input for one workspace: the durable workspace identity
// and the exact start commit the workspace must check out.
type Spec struct {
	ID       string `json:"id"`
	StartSHA string `json:"start_sha"`
}

// Target names the repository ref an approved result integrates into, plus the
// exact SHA the integration evidence captured when the launch started.
type Target struct {
	Ref    string `json:"ref"`
	OldSHA string `json:"old_sha"`
}

// Backend owns workspace lifecycle for one repository root.
type Backend interface {
	Kind() string
	Preflight(context.Context) (PreflightReport, error)
	Prepare(context.Context, Spec) error
	Open(context.Context, Ref) (Workspace, error)
	Remove(context.Context, Ref) error
	List(context.Context) ([]Ref, error)
	Integrator() Integrator
}

// Workspace is one prepared execution workspace. It captures canonical
// results, verifies exact SHAs, and runs system-owned checks inside itself.
type Workspace interface {
	Ref() Ref
	// ProviderView is the directory the provider process runs in: the
	// workspace path plus the workspace root's relative position inside the
	// repository.
	ProviderView() string
	Capture(context.Context, CaptureRequest) (CaptureResult, error)
	VerifyAt(context.Context, string) error
	RunCheck(context.Context, CheckRequest) (CheckResult, error)
}

// Integrator moves one repository target ref to an approved result SHA under
// the repository-global integration lock, ff-only and CAS-guarded.
type Integrator interface {
	Integrate(context.Context, Target, string) (IntegrationResult, error)
}

// PreflightReport captures the repository facts an isolated claim depends on.
type PreflightReport struct {
	RepoTop   string `json:"repo_top"`
	RootRel   string `json:"root_rel"` // slash path of cfg.Root relative to RepoTop, "." at the top
	TargetRef string `json:"target_ref"`
	TargetOld string `json:"target_old"`
}

// CaptureRequest describes one canonical result capture at provider terminal.
type CaptureRequest struct {
	LaunchID string
	BaseSHA  string
	TaskDoc  string    // full durable document identity for the result trailers
	Title    string    // bounded human title for the canonical commit message
	At       time.Time // provider terminal time; retry must reproduce the same SHA
}

// CaptureResult reports the canonical result established by a capture.
type CaptureResult struct {
	Changed        bool     `json:"changed"`
	ResultSHA      string   `json:"result_sha"` // == BaseSHA when unchanged
	TreeSHA        string   `json:"tree_sha"`
	AgentHeadMoved bool     `json:"agent_head_moved"`
	ChangedPaths   []string `json:"changed_paths"`
	Adopted        bool     `json:"adopted"` // the launch's result ref already existed and matched
}

// CheckDef is one configured system check. Configuration owns validation; the
// runner trusts the resolved values.
type CheckDef struct {
	ID      string
	Command string
	Args    []string
	Timeout time.Duration
}

// CheckRequest runs one check inside a workspace at an exact result SHA.
type CheckRequest struct {
	LaunchID  string
	Def       CheckDef
	ResultSHA string
}

// CheckResult is the durable evidence of one completed check.
type CheckResult struct {
	ID           string `json:"id"`
	DefDigest    string `json:"def_digest"`
	ResultSHA    string `json:"result_sha"`
	Outcome      string `json:"outcome"`
	ExitCode     int    `json:"exit_code"`
	DurationMS   int64  `json:"duration_ms"`
	OutputPath   string `json:"output_path"`
	OutputBytes  int64  `json:"output_bytes"`
	OutputKept   bool   `json:"output_kept"`
	OutputSHA256 string `json:"output_sha256"`
	Truncated    bool   `json:"truncated"`
}

// IntegrationResult reports what integration actually did.
type IntegrationResult struct {
	NewSHA    string `json:"new_sha"`
	Mechanism string `json:"mechanism"`
}

var (
	ErrPreflight           = errors.New("execws: git-worktree preflight failed")
	ErrWorkspaceConflict   = errors.New("execws: workspace conflict")
	ErrWorkspaceGone       = errors.New("execws: workspace is gone")
	ErrCaptureRejected     = errors.New("execws: capture rejected")
	ErrVerifyFailed        = errors.New("execws: workspace verification failed")
	ErrIntegrationBlocked  = errors.New("execws: integration blocked")
	ErrIntegrationAdvanced = errors.New("execws: target advanced")
)

// PreflightError names why one isolated claim cannot start. The claim stays
// eligible and unclaimed; the reason is surfaced to the operator.
type PreflightError struct {
	Reason string
	Detail string
}

func (e *PreflightError) Error() string {
	if e.Detail == "" {
		return fmt.Sprintf("%v: %s", ErrPreflight, e.Reason)
	}
	return fmt.Sprintf("%v: %s: %s", ErrPreflight, e.Reason, e.Detail)
}

func (e *PreflightError) Unwrap() error { return ErrPreflight }

// ConflictError details one workspace identity conflict.
type ConflictError struct{ Reason string }

func (e *ConflictError) Error() string { return fmt.Sprintf("%v: %s", ErrWorkspaceConflict, e.Reason) }
func (e *ConflictError) Unwrap() error { return ErrWorkspaceConflict }

// CaptureRejectedError names why working content cannot become the canonical
// result.
type CaptureRejectedError struct{ Reason string }

func (e *CaptureRejectedError) Error() string {
	return fmt.Sprintf("%v: %s", ErrCaptureRejected, e.Reason)
}
func (e *CaptureRejectedError) Unwrap() error { return ErrCaptureRejected }

// VerifyError reports a workspace that no longer matches its expected SHA or
// whose nonignored content changed.
type VerifyError struct{ Reason string }

func (e *VerifyError) Error() string { return fmt.Sprintf("%v: %s", ErrVerifyFailed, e.Reason) }
func (e *VerifyError) Unwrap() error { return ErrVerifyFailed }

// IntegrationBlockedError names a retryable integration obstacle. The lease
// and evidence stay; a later attempt may succeed.
type IntegrationBlockedError struct{ Reason string }

func (e *IntegrationBlockedError) Error() string {
	return fmt.Sprintf("%v: %s", ErrIntegrationBlocked, e.Reason)
}
func (e *IntegrationBlockedError) Unwrap() error { return ErrIntegrationBlocked }

// IntegrationAdvancedError reports a target that moved or a result that is
// not a fast-forward of the captured target: never merge, rebase, or force.
type IntegrationAdvancedError struct{ Reason string }

func (e *IntegrationAdvancedError) Error() string {
	return fmt.Sprintf("%v: %s", ErrIntegrationAdvanced, e.Reason)
}
func (e *IntegrationAdvancedError) Unwrap() error { return ErrIntegrationAdvanced }

// NewWorkspaceID generates one durable workspace identity: ws- plus 26
// lowercase base32 characters from 16 random bytes. It is independent of task,
// launch, and path.
func NewWorkspaceID() string { return randomID("ws-") }

func randomID(prefix string) string {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		panic("execws: crypto/rand unavailable: " + err.Error())
	}
	encoded := strings.ToLower(base32.StdEncoding.EncodeToString(raw[:]))
	return prefix + strings.TrimRight(encoded, "=")
}

// CheckDefDigest binds check evidence to the exact definition it ran. The
// digest covers id, command, arguments, and timeout.
func CheckDefDigest(def CheckDef) string {
	digest := sha256.New()
	digest.Write([]byte(def.ID))
	digest.Write([]byte{0})
	digest.Write([]byte(def.Command))
	for _, arg := range def.Args {
		digest.Write([]byte{0})
		digest.Write([]byte(arg))
	}
	digest.Write([]byte{0})
	digest.Write([]byte(strconv.FormatInt(int64(def.Timeout), 10)))
	return hex.EncodeToString(digest.Sum(nil))
}

// CheckSetDigest digests an exact ordered check set for launch evidence. The
// digest is deterministic across processes: definitions sort by ID.
func CheckSetDigest(defs []CheckDef) string {
	ordered := append([]CheckDef(nil), defs...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].ID < ordered[j].ID })
	digest := sha256.New()
	for _, def := range ordered {
		digest.Write([]byte(CheckDefDigest(def)))
		digest.Write([]byte{0})
	}
	return hex.EncodeToString(digest.Sum(nil))
}
