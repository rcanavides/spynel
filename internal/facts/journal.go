package facts

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/agent0ai/spynel/internal/fsx"
)

// Journal owns the append-only fact store below one workspace's
// .spynel/runtime/facts directory.
//
// Layout:
//
//	<dir>/.lock           cross-process append lock
//	<dir>/seq             LAST allocated sequence number
//	<dir>/activation.json create-once activation marker
//	<dir>/task/<key>.jsonl
//	<dir>/goal/<key>.jsonl
//
// The fact journal is machine evidence only. Markdown stays the workflow
// status authority; no event-sourced reconstruction of workflow state exists.
type Journal struct {
	dir string

	mu sync.Mutex

	// now is swapped only by tests for deterministic timestamps.
	now func() time.Time
	// failAppend injects a line-append failure after the sequence counter is
	// durably persisted, simulating the counter/line crash window.
	failAppend func() error

	activationOnce sync.Once
}

// Open binds one journal to its directory. Directories and the lock file are
// created lazily by the first append; read-only consumers work against a
// missing journal.
func Open(dir string) *Journal {
	return &Journal{dir: dir, now: time.Now}
}

func (j *Journal) lockPath() string       { return filepath.Join(j.dir, ".lock") }
func (j *Journal) seqPath() string        { return filepath.Join(j.dir, "seq") }
func (j *Journal) activationPath() string { return filepath.Join(j.dir, "activation.json") }
func (j *Journal) docPath(doc Doc) string {
	return filepath.Join(j.dir, doc.Kind, DocKey(doc.ID)+".jsonl")
}

func (j *Journal) ensureDirectories() error {
	for _, path := range []string{j.dir, filepath.Join(j.dir, DocKindTask), filepath.Join(j.dir, DocKindGoal)} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			return err
		}
	}
	return nil
}

// Append durably appends one fact. When a fact with the same key already
// exists for the document, the existing fact is returned and nothing is
// written: duplicate keys never allocate a second sequence number.
//
// Crash safety: the in-process mutex and the cross-process lock serialize
// allocation; the LAST allocated sequence is persisted before the journal
// line is appended, so a crash between the two leaves an allowed gap.
func (j *Journal) Append(fact Fact) (Fact, error) {
	if err := fact.validateForAppend(); err != nil {
		return Fact{}, err
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	unlock, err := lockFile(j.lockPath())
	if err != nil {
		return Fact{}, err
	}
	defer unlock()
	if err := j.ensureDirectories(); err != nil {
		return Fact{}, err
	}
	path := j.docPath(fact.Doc)
	existing, err := j.loadRepairing(path)
	if err != nil {
		return Fact{}, err
	}
	for _, prior := range existing {
		if prior.Key == fact.Key {
			return prior, nil
		}
	}
	sequence, err := j.allocateSeq()
	if err != nil {
		return Fact{}, err
	}
	fact.V = SchemaVersion
	fact.Seq = sequence
	if fact.At.IsZero() {
		fact.At = j.now().UTC()
	}
	line, err := json.Marshal(fact)
	if err != nil {
		return Fact{}, err
	}
	if j.failAppend != nil {
		if err := j.failAppend(); err != nil {
			return Fact{}, err
		}
	}
	if err := appendLine(path, append(line, '\n')); err != nil {
		return Fact{}, err
	}
	j.ensureActivation(sequence)
	return fact, nil
}

// Facts returns the complete evidence for one document in sequence order. A
// torn final line is tolerated as crash residue; complete invalid lines and
// unsupported schema versions fail closed for this document only.
func (j *Journal) Facts(doc Doc) ([]Fact, error) {
	data, err := os.ReadFile(j.docPath(doc))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return parseJournal(j.docPath(doc), data)
}

// AllFacts enumerates every recorded fact across all document journals in
// sequence order. A journal file that fails closed for corruption is skipped
// and reported: enumeration never mutates, and callers that need ownership
// proof must treat unprovable identities as unknown.
func (j *Journal) AllFacts() ([]Fact, []error) {
	var all []Fact
	var errs []error
	for _, kind := range []string{DocKindTask, DocKindGoal} {
		directory := filepath.Join(j.dir, kind)
		entries, err := os.ReadDir(directory)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			errs = append(errs, err)
			continue
		}
		for _, entry := range entries {
			if entry.IsDir() || filepath.Ext(entry.Name()) != ".jsonl" {
				continue
			}
			path := filepath.Join(directory, entry.Name())
			data, err := os.ReadFile(path)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			document, err := parseJournal(path, data)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			all = append(all, document...)
		}
	}
	sort.Slice(all, func(i, k int) bool { return all[i].Seq < all[k].Seq })
	return all, errs
}

// Activation returns the create-once activation marker. The second result is
// false while no fact was ever recorded for the workspace.
func (j *Journal) Activation() (Activation, bool, error) {
	data, err := os.ReadFile(j.activationPath())
	if os.IsNotExist(err) {
		return Activation{}, false, nil
	}
	if err != nil {
		return Activation{}, false, err
	}
	var activation Activation
	if err := json.Unmarshal(data, &activation); err != nil {
		return Activation{}, false, fmt.Errorf("read fact activation: %w", err)
	}
	return activation, true, nil
}

// Activation records the moment this workspace began recording facts. It is
// created exactly once and never rewritten; pre-activation history never gets
// synthetic facts.
type Activation struct {
	V           int       `json:"v"`
	ActivatedAt time.Time `json:"activated_at"`
	FirstSeq    uint64    `json:"first_seq"`
}

func (j *Journal) ensureActivation(firstSeq uint64) {
	j.activationOnce.Do(func() {
		data, err := json.Marshal(Activation{V: SchemaVersion, ActivatedAt: j.now().UTC(), FirstSeq: firstSeq})
		if err != nil {
			return
		}
		// AtomicCreateFile cannot replace an existing marker; an already
		// existing activation is the documented create-once success.
		if err := fsx.AtomicCreateFile(j.activationPath(), data, 0o600); err != nil && !os.IsExist(err) {
			// Activation is evidence metadata, never a workflow gate: a
			// creation failure must not fail the already-durable fact append.
			_ = err
		}
	})
}

// loadRepairing reads a journal file under the append lock and truncates a
// torn final line back to the last complete line boundary.
func (j *Journal) loadRepairing(path string) ([]Fact, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if torn := len(data) > 0 && data[len(data)-1] != '\n'; torn {
		complete := bytes.LastIndexByte(data, '\n')
		if err := os.Truncate(path, int64(complete+1)); err != nil {
			return nil, fmt.Errorf("repair torn journal tail %s: %w", path, err)
		}
		data = data[:complete+1]
	}
	return parseJournal(path, data)
}

// parseJournal decodes complete journal lines. It rejects complete invalid
// JSON and unsupported schema versions with ErrJournalCorrupt, attributing
// the exact path and line number.
func parseJournal(path string, data []byte) ([]Fact, error) {
	if len(data) == 0 {
		return nil, nil
	}
	var facts []Fact
	line := 1
	for offset := 0; offset < len(data); {
		end := bytes.IndexByte(data[offset:], '\n')
		if end < 0 {
			// An unterminated final line is torn crash residue, never
			// authoritative evidence and never corruption.
			break
		}
		content := data[offset : offset+end]
		offset += end + 1
		if len(bytes.TrimSpace(content)) == 0 {
			line++
			continue
		}
		var fact Fact
		if err := json.Unmarshal(content, &fact); err != nil {
			return nil, corruptAt(path, line, err.Error())
		}
		if fact.V != SchemaVersion {
			return nil, corruptAt(path, line, fmt.Sprintf("unsupported schema version %d (want %d)", fact.V, SchemaVersion))
		}
		facts = append(facts, fact)
		line++
	}
	return facts, nil
}

// allocateSeq persists and returns the next sequence number. The seq file
// stores the LAST allocated value: a missing file with no facts means zero,
// and a missing file with existing facts rebuilds the last value from the
// maximum sequence present in the journals (never max+1), so the next append
// becomes max+1. Duplicates are impossible; gaps after a crash are allowed.
func (j *Journal) allocateSeq() (uint64, error) {
	last, found, err := j.readSeqFile()
	if err != nil {
		return 0, err
	}
	if !found {
		last, err = j.rebuildLastSeq()
		if err != nil {
			return 0, err
		}
	}
	next := last + 1
	if err := fsx.AtomicWriteFile(j.seqPath(), []byte(strconv.FormatUint(next, 10)+"\n"), 0o600); err != nil {
		return 0, fmt.Errorf("persist fact sequence: %w", err)
	}
	return next, nil
}

func (j *Journal) readSeqFile() (uint64, bool, error) {
	data, err := os.ReadFile(j.seqPath())
	if os.IsNotExist(err) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	value, parseErr := strconv.ParseUint(string(bytes.TrimSpace(data)), 10, 64)
	if parseErr != nil {
		return 0, false, nil
	}
	return value, true, nil
}

// rebuildLastSeq recovers the last allocated sequence from recorded facts
// after the seq file disappeared. The rebuilt value is the maximum sequence
// present (never max+1), so the next append becomes max+1. Any complete
// corrupt line fails closed because the true maximum cannot be known.
func (j *Journal) rebuildLastSeq() (uint64, error) {
	var last uint64
	for _, kind := range []string{DocKindTask, DocKindGoal} {
		directory := filepath.Join(j.dir, kind)
		entries, err := os.ReadDir(directory)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return 0, err
		}
		for _, entry := range entries {
			if entry.IsDir() || filepath.Ext(entry.Name()) != ".jsonl" {
				continue
			}
			path := filepath.Join(directory, entry.Name())
			data, err := os.ReadFile(path)
			if err != nil {
				return 0, err
			}
			docFacts, err := parseJournal(path, data)
			if err != nil {
				return 0, err
			}
			for _, fact := range docFacts {
				if fact.Seq > last {
					last = fact.Seq
				}
			}
		}
	}
	return last, nil
}

func appendLine(path string, line []byte) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	if _, err := file.Write(line); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	return file.Close()
}
