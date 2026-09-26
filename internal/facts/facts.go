package facts

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

// SchemaVersion is the only supported fact journal schema version. A complete
// journal line carrying a different version fails closed as corruption.
const SchemaVersion = 1

// Document kinds name the two fixed workflow routes that own fact journals.
const (
	DocKindTask = "task"
	DocKindGoal = "goal"
)

// Fact kinds. The taxonomy is fixed: no lifecycle symmetry events exist.
const (
	KindLaunchCreated        = "launch_created"
	KindWorkspaceReady       = "workspace_ready"
	KindProviderAdmitted     = "provider_admitted"
	KindProviderTerminal     = "provider_terminal"
	KindResultCaptured       = "result_captured"
	KindCheckCompleted       = "check_completed"
	KindReviewRequested      = "review_requested"
	KindReviewCompleted      = "review_completed"
	KindIntegrationCompleted = "integration_completed"
	KindIntegrationRejected  = "integration_rejected"
	KindLaunchFailed         = "launch_failed"
	KindWorkspaceRemoved     = "workspace_removed"
)

// MaxErrorDetailBytes bounds error_detail so one failed launch can never
// smuggle unbounded text into durable evidence.
const MaxErrorDetailBytes = 512

// SanitizeErrorDetail converts arbitrary process/provider diagnostics into
// one durable, bounded line. Invalid UTF-8 uses Go's standard replacement
// rune, line separators become spaces, and truncation preserves a complete
// rune boundary while retaining the diagnostic prefix.
func SanitizeErrorDetail(value string) string {
	value = strings.ToValidUTF8(value, string(utf8.RuneError))
	value = strings.Map(func(r rune) rune {
		switch r {
		case '\r', '\n', '\u0085', '\u2028', '\u2029':
			return ' '
		default:
			return r
		}
	}, value)
	if len(value) <= MaxErrorDetailBytes {
		return value
	}
	end := MaxErrorDetailBytes
	for end > 0 && !utf8.RuneStart(value[end]) {
		end--
	}
	return value[:end]
}

// ErrJournalCorrupt marks complete journal corruption: a complete line that is
// invalid JSON or carries an unsupported schema version. Torn final lines are
// crash residue, not corruption, and are repaired under the journal lock.
var ErrJournalCorrupt = errors.New("fact journal corrupt")

// CorruptError attributes one corrupt journal line with its path and line
// number so an operator can repair exactly the damaged document evidence.
type CorruptError struct {
	Path   string
	Line   int
	Reason string
}

func (e *CorruptError) Error() string {
	return fmt.Sprintf("%v: %s line %d: %s", ErrJournalCorrupt, e.Path, e.Line, e.Reason)
}

// Unwrap binds corruption attribution to the sentinel so errors.Is matches.
func (e *CorruptError) Unwrap() error { return ErrJournalCorrupt }

func corruptAt(path string, line int, reason string) error {
	return &CorruptError{Path: path, Line: line, Reason: reason}
}

// Doc is the full durable document identity stored inside every fact. The
// journal file layout derives from it, but the stored identity — never the
// filesystem path — is authoritative.
type Doc struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
}

// CheckEvidence binds one completed check to the exact launch, result SHA, and
// check definition digest it ran against.
type CheckEvidence struct {
	Check       string `json:"check"`
	DefDigest   string `json:"def_digest,omitempty"`
	ResultSHA   string `json:"result_sha,omitempty"`
	Outcome     string `json:"outcome,omitempty"`
	ExitCode    int    `json:"exit_code,omitempty"`
	DurationMS  int64  `json:"duration_ms,omitempty"`
	OutputPath  string `json:"output_path,omitempty"`
	OutputBytes int64  `json:"output_bytes,omitempty"`
	OutputKept  bool   `json:"output_kept,omitempty"`
	OutputSHA   string `json:"output_sha256,omitempty"`
	Truncated   bool   `json:"truncated,omitempty"`
}

// Fact is one append-only machine evidence record. Markdown remains the
// workflow-status authority; facts only prove what Spynel itself did.
type Fact struct {
	V    int       `json:"v"`
	Seq  uint64    `json:"seq"`
	Kind string    `json:"kind"`
	Key  string    `json:"key"`
	At   time.Time `json:"at"`
	Doc  Doc       `json:"doc"`

	Launch           string          `json:"launch,omitempty"`
	Phase            string          `json:"phase,omitempty"`
	Supersedes       string          `json:"supersedes,omitempty"`
	Provider         string          `json:"provider,omitempty"`
	WorkspaceID      string          `json:"workspace_id,omitempty"`
	WorkspaceKind    string          `json:"workspace_kind,omitempty"`
	BaseSHA          string          `json:"base_sha,omitempty"`
	TargetRef        string          `json:"target_ref,omitempty"`
	TargetOld        string          `json:"target_old,omitempty"`
	ResultSHA        string          `json:"result_sha,omitempty"`
	TreeSHA          string          `json:"tree_sha,omitempty"`
	ReviewSHA        string          `json:"review_sha,omitempty"`
	ImplLaunch       string          `json:"impl_launch,omitempty"`
	Changed          bool            `json:"changed,omitempty"`
	AgentHeadMoved   bool            `json:"agent_head_moved,omitempty"`
	ChangedPaths     []string        `json:"changed_paths,omitempty"`
	CheckSet         string          `json:"check_set,omitempty"`
	Checks           []CheckEvidence `json:"checks,omitempty"`
	Check            string          `json:"check,omitempty"`
	DefDigest        string          `json:"def_digest,omitempty"`
	Outcome          string          `json:"outcome,omitempty"`
	ExitCode         int             `json:"exit_code,omitempty"`
	DurationMS       int64           `json:"duration_ms,omitempty"`
	OutputPath       string          `json:"output_path,omitempty"`
	OutputBytes      int64           `json:"output_bytes,omitempty"`
	OutputKept       bool            `json:"output_kept,omitempty"`
	OutputSHA        string          `json:"output_sha256,omitempty"`
	Truncated        bool            `json:"truncated,omitempty"`
	Verdict          string          `json:"verdict,omitempty"`
	WorkspaceMutated bool            `json:"workspace_mutated,omitempty"`
	DocSHA           string          `json:"doc_sha256,omitempty"`
	Integration      string          `json:"integration,omitempty"`
	Stage            string          `json:"stage,omitempty"`
	ErrorClass       string          `json:"error_class,omitempty"`
	ErrorDetail      string          `json:"error_detail,omitempty"`
}

var validKinds = map[string]bool{
	KindLaunchCreated: true, KindWorkspaceReady: true, KindProviderAdmitted: true,
	KindProviderTerminal: true, KindResultCaptured: true, KindCheckCompleted: true,
	KindReviewRequested: true, KindReviewCompleted: true, KindIntegrationCompleted: true,
	KindIntegrationRejected: true, KindLaunchFailed: true, KindWorkspaceRemoved: true,
}

// ValidKind reports whether kind belongs to the fixed fact taxonomy.
func ValidKind(kind string) bool { return validKinds[kind] }

func validDocKind(kind string) bool {
	return kind == DocKindTask || kind == DocKindGoal
}

var docKeyPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,100}$`)

// DocKey derives the filesystem-safe journal key for one full document ID.
// Filesystem identity is never authority: the complete ID always remains
// inside the fact.
func DocKey(fullID string) string {
	if docKeyPattern.MatchString(fullID) {
		return fullID
	}
	digest := sha256.Sum256([]byte(fullID))
	return "h-" + hex.EncodeToString(digest[:16])
}

func boundedLine(name, value string, limit int) error {
	if len(value) > limit {
		return fmt.Errorf("%s must be at most %d bytes", name, limit)
	}
	if !utf8.ValidString(value) {
		return fmt.Errorf("%s must be valid UTF-8", name)
	}
	for _, r := range value {
		if r == '\n' || r == '\r' {
			return fmt.Errorf("%s must be one line", name)
		}
	}
	return nil
}

func (f Fact) validateForAppend() error {
	if !ValidKind(f.Kind) {
		return fmt.Errorf("fact kind %q is not in the taxonomy", f.Kind)
	}
	if f.Key == "" {
		return errors.New("fact key is required")
	}
	if err := boundedLine("fact key", f.Key, 256); err != nil {
		return err
	}
	if !validDocKind(f.Doc.Kind) {
		return fmt.Errorf("fact doc kind %q must be task or goal", f.Doc.Kind)
	}
	if f.Doc.ID == "" {
		return errors.New("fact doc id is required")
	}
	if err := boundedLine("fact doc id", f.Doc.ID, 512); err != nil {
		return err
	}
	if err := boundedLine("error_detail", f.ErrorDetail, MaxErrorDetailBytes); err != nil {
		return err
	}
	if err := boundedLine("error_class", f.ErrorClass, 128); err != nil {
		return err
	}
	for _, path := range f.ChangedPaths {
		if !utf8.ValidString(path) || len(path) > 4096 {
			return errors.New("changed_paths entries must be valid UTF-8 of at most 4096 bytes")
		}
	}
	return nil
}
