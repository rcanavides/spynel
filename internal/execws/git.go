package execws

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

// gitOutputBound caps each captured Git stream so a pathological repository
// can never grow unbounded diagnostic memory.
const gitOutputBound = 1 << 20

// scrubbedGitEnv removes dangerous inherited Git environment. GIT_DIR,
// GIT_WORK_TREE, and friends would redirect Spynel-owned operations into
// attacker- or accident-chosen repositories.
var scrubbedGitEnv = []string{
	"GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE", "GIT_OBJECT_DIRECTORY",
	"GIT_ALTERNATE_OBJECT_DIRECTORIES", "GIT_COMMON_DIR", "GIT_NAMESPACE",
	"GIT_CEILING_DIRECTORIES",
}

const scrubbedGitEnvPrefix = "GIT_CONFIG_"

type gitRunner struct {
	// hooksPath points at an intentionally empty directory so Spynel-owned Git
	// operations never execute repository hooks.
	hooksPath string
}

type gitOutput struct {
	stdout   []byte
	stderr   []byte
	stdoutOK bool
	stderrOK bool
}

// run executes one explicit git invocation. No shell is involved anywhere in
// the execution plane.
func (g gitRunner) run(ctx context.Context, dir string, extraEnv []string, args ...string) (gitOutput, error) {
	full := append([]string{"-c", "commit.gpgsign=false"}, args...)
	if g.hooksPath != "" {
		full = append([]string{"-c", "core.hooksPath=" + g.hooksPath}, full...)
	}
	cmd := exec.CommandContext(ctx, "git", full...)
	cmd.Dir = dir
	cmd.Env = g.env(extraEnv...)
	stdout := &boundedBuffer{limit: gitOutputBound}
	stderr := &boundedBuffer{limit: gitOutputBound}
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	err := cmd.Run()
	out := gitOutput{stdout: stdout.bytes(), stderr: stderr.bytes()}
	if err != nil {
		return out, fmt.Errorf("git %s: %w%s", strings.Join(args, " "), err, g.detail(out, err))
	}
	return out, nil
}

// runStdin executes one explicit git invocation with stdin content, used by
// commit-tree where the message must not touch a shell or a message file.
func (g gitRunner) runStdin(ctx context.Context, dir string, stdin []byte, extraEnv []string, args ...string) ([]byte, error) {
	full := append([]string{"-c", "commit.gpgsign=false"}, args...)
	if g.hooksPath != "" {
		full = append([]string{"-c", "core.hooksPath=" + g.hooksPath}, full...)
	}
	cmd := exec.CommandContext(ctx, "git", full...)
	cmd.Dir = dir
	cmd.Env = g.env(extraEnv...)
	cmd.Stdin = bytes.NewReader(stdin)
	stdout := &boundedBuffer{limit: gitOutputBound}
	stderr := &boundedBuffer{limit: gitOutputBound}
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	err := cmd.Run()
	if err != nil {
		return stdout.bytes(), fmt.Errorf("git %s: %w%s", strings.Join(args, " "), err, g.detail(gitOutput{stdout: stdout.bytes(), stderr: stderr.bytes()}, err))
	}
	return stdout.bytes(), nil
}

// detail appends bounded stderr diagnostics to a git failure.
func (g gitRunner) detail(out gitOutput, err error) string {
	if len(out.stderr) == 0 {
		return ""
	}
	message := strings.TrimSpace(string(out.stderr))
	if message == "" {
		return ""
	}
	const bound = 512
	if len(message) > bound {
		message = message[len(message)-bound:]
	}
	return " (" + message + ")"
}

func (g gitRunner) env(extra ...string) []string {
	scrubbed := make([]string, 0, 16+len(extra))
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		if scrubbedEnvName(name) {
			continue
		}
		scrubbed = append(scrubbed, entry)
	}
	scrubbed = append(scrubbed, "GIT_TERMINAL_PROMPT=0", "GIT_OPTIONAL_LOCKS=0")
	return append(scrubbed, extra...)
}

func scrubbedEnvName(name string) bool {
	for _, scrubbedName := range scrubbedGitEnv {
		if name == scrubbedName {
			return true
		}
	}
	return strings.HasPrefix(name, scrubbedGitEnvPrefix)
}

// boundedBuffer keeps at most the first limit bytes of one stream.
type boundedBuffer struct {
	limit int
	data  []byte
	full  bool
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	if room := b.limit - len(b.data); room > 0 {
		if len(p) <= room {
			b.data = append(b.data, p...)
		} else {
			b.data = append(b.data, p[:room]...)
			b.full = true
		}
	} else {
		b.full = true
	}
	return len(p), nil
}

func (b *boundedBuffer) bytes() []byte { return b.data }
