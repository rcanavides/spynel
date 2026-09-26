package facts

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"
)

func testDoc(kind, id string) Doc { return Doc{Kind: kind, ID: id} }

func launchFact(key string, seqHint string) Fact {
	return Fact{
		Kind: KindLaunchCreated, Key: key,
		Doc:    testDoc(DocKindTask, "doc-"+seqHint),
		Launch: "ln-" + seqHint, Phase: "task_implementation",
		WorkspaceKind: "shared",
	}
}

func requireFact(t *testing.T, journal *Journal, doc Doc) []Fact {
	t.Helper()
	facts, err := journal.Facts(doc)
	if err != nil {
		t.Fatalf("Facts: %v", err)
	}
	return facts
}

func TestAppendAssignsSeqAndPersistsCounter(t *testing.T) {
	journal := Open(t.TempDir())
	first, err := journal.Append(launchFact("l1:launch_created", "one"))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if first.Seq != 1 || first.V != SchemaVersion {
		t.Fatalf("first fact seq/version = %d/%d, want 1/%d", first.Seq, first.V, SchemaVersion)
	}
	if first.At.IsZero() {
		t.Fatal("journal must stamp the fact time")
	}
	data, err := os.ReadFile(filepath.Join(journal.dir, "seq"))
	if err != nil {
		t.Fatalf("read seq: %v", err)
	}
	if got := strings.TrimSpace(string(data)); got != "1" {
		t.Fatalf("seq file stores %q, want the last allocated value 1", got)
	}
	second, err := journal.Append(Fact{
		Kind: KindProviderAdmitted, Key: "l1:provider_admitted",
		Doc: testDoc(DocKindTask, "doc-one"), Launch: "l1",
	})
	if err != nil {
		t.Fatalf("Append second: %v", err)
	}
	if second.Seq != 2 {
		t.Fatalf("second fact seq = %d, want 2", second.Seq)
	}
}

func TestFactsStoredUnderDocumentKindDirectory(t *testing.T) {
	journal := Open(t.TempDir())
	if _, err := journal.Append(Fact{Kind: KindLaunchCreated, Key: "k", Doc: testDoc(DocKindGoal, "g1"), Launch: "ln-g"}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if _, err := os.Stat(filepath.Join(journal.dir, DocKindGoal, "g1.jsonl")); err != nil {
		t.Fatalf("goal journal missing: %v", err)
	}
	// A task document with the same file name journals separately.
	if _, err := journal.Append(Fact{Kind: KindLaunchCreated, Key: "k2", Doc: testDoc(DocKindTask, "g1"), Launch: "ln-t"}); err != nil {
		t.Fatalf("Append task: %v", err)
	}
	if _, err := os.Stat(filepath.Join(journal.dir, DocKindTask, "g1.jsonl")); err != nil {
		t.Fatalf("task journal missing: %v", err)
	}
	// Document IDs that are not filesystem-safe derive hashed keys.
	if _, err := journal.Append(Fact{Kind: KindLaunchCreated, Key: "k3", Doc: testDoc(DocKindTask, "id with space"), Launch: "ln-t2"}); err != nil {
		t.Fatalf("Append hashed doc: %v", err)
	}
	if _, err := os.Stat(filepath.Join(journal.dir, DocKindTask, DocKey("id with space")+".jsonl")); err != nil {
		t.Fatalf("hashed journal missing: %v", err)
	}
}

func TestDocKeyDirectAndHashed(t *testing.T) {
	if got := DocKey("simple-task.1_v2"); got != "simple-task.1_v2" {
		t.Fatalf("DocKey = %q, want direct identity", got)
	}
	if got := DocKey("has space"); !strings.HasPrefix(got, "h-") || len(got) != 34 {
		t.Fatalf("DocKey = %q, want h- plus 32 hex characters", got)
	}
	if DocKey("id one") == DocKey("id two") {
		t.Fatal("distinct IDs must derive distinct hashed keys")
	}
}

// FJ1: concurrent appends from many goroutines produce globally unique,
// strictly increasing sequences in file order.
func TestFJ1ConcurrentSeqUniquenessAndOrder(t *testing.T) {
	journal := Open(t.TempDir())
	const writers = 8
	const perWriter = 12
	var wg sync.WaitGroup
	errs := make(chan error, writers*perWriter)
	for worker := 0; worker < writers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for round := 0; round < perWriter; round++ {
				doc := testDoc(DocKindTask, fmt.Sprintf("doc-%d", worker%3))
				key := fmt.Sprintf("ln-%d-%d:launch_created", worker, round)
				if _, err := journal.Append(Fact{Kind: KindLaunchCreated, Key: key, Doc: doc, Launch: fmt.Sprintf("ln-%d-%d", worker, round)}); err != nil {
					errs <- err
				}
			}
		}(worker)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent Append: %v", err)
	}
	seen := map[uint64]string{}
	for worker := 0; worker < 3; worker++ {
		facts := requireFact(t, journal, testDoc(DocKindTask, fmt.Sprintf("doc-%d", worker)))
		var previous uint64
		for _, fact := range facts {
			if fact.Seq <= previous {
				t.Fatalf("sequences must strictly increase in file order: %d after %d", fact.Seq, previous)
			}
			if other, dup := seen[fact.Seq]; dup {
				t.Fatalf("duplicate sequence %d for %s and %s", fact.Seq, other, fact.Key)
			}
			seen[fact.Seq] = fact.Key
			previous = fact.Seq
		}
	}
	data, err := os.ReadFile(filepath.Join(journal.dir, "seq"))
	if err != nil {
		t.Fatalf("read seq: %v", err)
	}
	last, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		t.Fatalf("parse seq: %v", err)
	}
	if int(last) != writers*perWriter {
		t.Fatalf("seq file = %d, want %d", last, writers*perWriter)
	}
}

// FJ3: a torn final line is crash residue: reads tolerate it and the next
// append truncates only back to the last newline.
func TestFJ3TornTailRecoveredOnAppend(t *testing.T) {
	journal := Open(t.TempDir())
	doc := testDoc(DocKindTask, "torn")
	if _, err := journal.Append(Fact{Kind: KindLaunchCreated, Key: "a", Doc: doc, Launch: "ln-a"}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	path := journal.docPath(doc)
	// Append a torn line directly, simulating a crash mid-write.
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("open journal: %v", err)
	}
	if _, err := file.WriteString(`{"v":1,"seq":99,"kind":"provider_a`); err != nil {
		t.Fatalf("write torn tail: %v", err)
	}
	file.Close()

	facts := requireFact(t, journal, doc)
	if len(facts) != 1 || facts[0].Key != "a" {
		t.Fatalf("reads must tolerate the torn tail; got %d facts", len(facts))
	}
	if _, err := journal.Append(Fact{Kind: KindProviderAdmitted, Key: "b", Doc: doc, Launch: "ln-a"}); err != nil {
		t.Fatalf("Append after torn tail: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read journal: %v", err)
	}
	if !bytesEndWithNewline(data) {
		t.Fatal("journal must end with a newline after repair")
	}
	if strings.Contains(string(data), `"seq":99`) {
		t.Fatal("torn tail content must be truncated before the new append")
	}
	facts = requireFact(t, journal, doc)
	if len(facts) != 2 {
		t.Fatalf("repaired journal has %d facts, want 2", len(facts))
	}
}

func bytesEndWithNewline(data []byte) bool { return len(data) > 0 && data[len(data)-1] == '\n' }

// FJ4: a complete corrupt line fails closed for that document only.
func TestFJ4CompleteCorruptionFailsClosed(t *testing.T) {
	journal := Open(t.TempDir())
	corrupt := testDoc(DocKindTask, "corrupt")
	healthy := testDoc(DocKindTask, "healthy")
	if _, err := journal.Append(Fact{Kind: KindLaunchCreated, Key: "h", Doc: healthy, Launch: "ln-h"}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := os.WriteFile(journal.docPath(corrupt), []byte("{not json at all\n"), 0o600); err != nil {
		t.Fatalf("write corrupt journal: %v", err)
	}
	if _, err := journal.Facts(corrupt); !errors.Is(err, ErrJournalCorrupt) {
		t.Fatalf("Facts on corrupt journal: %v, want ErrJournalCorrupt", err)
	}
	// The corrupt error must attribute path and line number.
	_, err := journal.Facts(corrupt)
	var attributed *CorruptError
	if !errors.As(err, &attributed) {
		t.Fatalf("error must be *CorruptError, got %T", err)
	}
	if attributed.Path != journal.docPath(corrupt) || attributed.Line != 1 {
		t.Fatalf("corruption attribution = %s:%d", attributed.Path, attributed.Line)
	}
	// Appends to the corrupt document fail closed...
	if _, err := journal.Append(Fact{Kind: KindLaunchCreated, Key: "c", Doc: corrupt, Launch: "ln-c"}); !errors.Is(err, ErrJournalCorrupt) {
		t.Fatalf("Append on corrupt journal: %v, want ErrJournalCorrupt", err)
	}
	// ...but unrelated documents keep working.
	if _, err := journal.Append(Fact{Kind: KindProviderAdmitted, Key: "h2", Doc: healthy, Launch: "ln-h"}); err != nil {
		t.Fatalf("Append on healthy document: %v", err)
	}
}

// FJ5: an existing key returns the existing fact without a new sequence.
func TestFJ5DuplicateKeyReturnsExistingWithoutSecondSeq(t *testing.T) {
	journal := Open(t.TempDir())
	doc := testDoc(DocKindTask, "dup")
	first, err := journal.Append(Fact{Kind: KindLaunchCreated, Key: "l:launch_created", Doc: doc, Launch: "l", Phase: "task_implementation"})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	duplicate, err := journal.Append(Fact{Kind: KindLaunchCreated, Key: "l:launch_created", Doc: doc, Launch: "l", Phase: "task_review"})
	if err != nil {
		t.Fatalf("duplicate Append: %v", err)
	}
	if duplicate.Seq != first.Seq || duplicate.Phase != first.Phase {
		t.Fatalf("duplicate append must return the existing fact: got seq %d phase %s, want seq %d phase %s", duplicate.Seq, duplicate.Phase, first.Seq, first.Phase)
	}
	facts := requireFact(t, journal, doc)
	if len(facts) != 1 {
		t.Fatalf("journal has %d facts, want 1", len(facts))
	}
	data, err := os.ReadFile(filepath.Join(journal.dir, "seq"))
	if err != nil {
		t.Fatalf("read seq: %v", err)
	}
	if got := strings.TrimSpace(string(data)); got != "1" {
		t.Fatalf("seq file = %q: duplicate must not allocate a sequence", got)
	}
}

// FJ6: the counter is authoritative even when it advanced past the last
// visible fact (crash between counter persistence and line append).
func TestFJ6GapAfterCounterPersistenceCrash(t *testing.T) {
	journal := Open(t.TempDir())
	doc := testDoc(DocKindTask, "gap")
	if _, err := journal.Append(Fact{Kind: KindLaunchCreated, Key: "a", Doc: doc, Launch: "ln-a"}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	// Simulate the crash window: the counter persisted 5 but no line landed.
	if err := os.WriteFile(filepath.Join(journal.dir, "seq"), []byte("5\n"), 0o600); err != nil {
		t.Fatalf("write seq: %v", err)
	}
	fact, err := journal.Append(Fact{Kind: KindProviderAdmitted, Key: "b", Doc: doc, Launch: "ln-a"})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if fact.Seq != 6 {
		t.Fatalf("fact seq = %d, want 6 allocated after the persisted counter", fact.Seq)
	}
	facts := requireFact(t, journal, doc)
	if len(facts) != 2 || facts[len(facts)-1].Seq != 6 {
		t.Fatalf("journal = %+v, want the gap tolerated", facts)
	}
}

// FJ7: a failed line append after counter persistence retries to exactly one
// visible fact under the same key.
func TestFJ7SyncFailureRetryIdempotence(t *testing.T) {
	journal := Open(t.TempDir())
	doc := testDoc(DocKindTask, "retry")
	journal.failAppend = func() error { return errors.New("simulated line append failure") }
	if _, err := journal.Append(Fact{Kind: KindLaunchCreated, Key: "l:launch_created", Doc: doc, Launch: "l"}); err == nil {
		t.Fatal("append must surface the injected failure")
	}
	// The failed attempt persisted its sequence; the key has no fact yet.
	facts := requireFact(t, journal, doc)
	if len(facts) != 0 {
		t.Fatalf("failed append must leave no fact, got %d", len(facts))
	}
	journal.failAppend = nil
	fact, err := journal.Append(Fact{Kind: KindLaunchCreated, Key: "l:launch_created", Doc: doc, Launch: "l"})
	if err != nil {
		t.Fatalf("retry Append: %v", err)
	}
	facts = requireFact(t, journal, doc)
	if len(facts) != 1 || facts[0].Key != "l:launch_created" || facts[0].Seq != fact.Seq {
		t.Fatalf("retry must produce exactly one fact under the key: %+v", facts)
	}
}

// FJ8: activation is created exactly once and never rewritten.
func TestFJ8ActivationCreateOnce(t *testing.T) {
	journal := Open(t.TempDir())
	doc := testDoc(DocKindTask, "act")
	if _, err := journal.Append(Fact{Kind: KindLaunchCreated, Key: "a", Doc: doc, Launch: "ln-a"}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	activation, ok, err := journal.Activation()
	if err != nil || !ok {
		t.Fatalf("Activation: ok=%v err=%v", ok, err)
	}
	if activation.V != SchemaVersion || activation.FirstSeq != 1 || activation.ActivatedAt.IsZero() {
		t.Fatalf("activation = %+v", activation)
	}
	if _, err := journal.Append(Fact{Kind: KindProviderAdmitted, Key: "b", Doc: doc, Launch: "ln-a"}); err != nil {
		t.Fatalf("second Append: %v", err)
	}
	again, ok, err := journal.Activation()
	if err != nil || !ok {
		t.Fatalf("second Activation: ok=%v err=%v", ok, err)
	}
	if !again.ActivatedAt.Equal(activation.ActivatedAt) || again.FirstSeq != activation.FirstSeq {
		t.Fatalf("activation was rewritten: %+v then %+v", activation, again)
	}
	// A fresh journal instance must also never rewrite the marker.
	reopened := Open(journal.dir)
	if _, err := reopened.Append(Fact{Kind: KindProviderAdmitted, Key: "c", Doc: doc, Launch: "ln-a"}); err != nil {
		t.Fatalf("reopened Append: %v", err)
	}
	third, _, err := reopened.Activation()
	if err != nil {
		t.Fatalf("reopened Activation: %v", err)
	}
	if !third.ActivatedAt.Equal(activation.ActivatedAt) {
		t.Fatalf("reopened journal rewrote activation: %+v", third)
	}
}

// FJ9: a complete line with an unknown schema version fails closed.
func TestFJ9UnknownSchemaVersionRejected(t *testing.T) {
	journal := Open(t.TempDir())
	doc := testDoc(DocKindTask, "versioned")
	if err := os.MkdirAll(filepath.Join(journal.dir, DocKindTask), 0o700); err != nil {
		t.Fatalf("prepare journal directory: %v", err)
	}
	if err := os.WriteFile(journal.docPath(doc), []byte(`{"v":2,"seq":1,"kind":"launch_created","key":"x","at":"2026-01-01T00:00:00Z","doc":{"kind":"task","id":"versioned"}}`+"\n"), 0o600); err != nil {
		t.Fatalf("write journal: %v", err)
	}
	if _, err := journal.Facts(doc); !errors.Is(err, ErrJournalCorrupt) {
		t.Fatalf("Facts: %v, want ErrJournalCorrupt for unsupported schema", err)
	}
}

// FJ10: error_detail is bounded to 512 bytes of valid UTF-8.
func TestFJ10ErrorDetailBoundedUTF8(t *testing.T) {
	journal := Open(t.TempDir())
	doc := testDoc(DocKindTask, "error-bound")
	base := Fact{Kind: KindLaunchFailed, Key: "l:launch_failed", Doc: doc, Launch: "l"}
	if _, err := journal.Append(withDetail(base, strings.Repeat("x", 512))); err != nil {
		t.Fatalf("512-byte error_detail must be accepted: %v", err)
	}
	if _, err := journal.Append(withDetail(base, strings.Repeat("x", 513))); err == nil {
		t.Fatal("513-byte error_detail must be rejected")
	}
	if _, err := journal.Append(withDetail(base, "bad utf8 \xff\xfe")); err == nil {
		t.Fatal("invalid UTF-8 error_detail must be rejected")
	}
	// A multi-byte value counts bytes, not runes.
	if _, err := journal.Append(withDetail(base, strings.Repeat("é", 256))); err != nil {
		t.Fatalf("512-byte UTF-8 detail must be accepted: %v", err)
	}
	if _, err := journal.Append(withDetail(base, strings.Repeat("é", 257))); err == nil {
		t.Fatal("514-byte UTF-8 detail must be rejected")
	}
}

func TestSanitizeErrorDetailPreservesValidSingleLinePrefix(t *testing.T) {
	for _, test := range []struct {
		name  string
		input string
		want  []string
	}{
		{name: "multibyte boundary", input: strings.Repeat("界", 171), want: []string{strings.Repeat("界", 170)}},
		{name: "multiline", input: "line\rnext\nmore\r\ntail\u0085after\u2028last\u2029end", want: []string{"line", "next", "end"}},
		{name: "invalid utf8", input: "prefix " + string([]byte{0xff, 0xfe}) + " suffix", want: []string{"prefix", "suffix"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			detail := SanitizeErrorDetail(test.input)
			if !utf8.ValidString(detail) {
				t.Fatal("sanitized detail is not valid UTF-8")
			}
			if len(detail) > MaxErrorDetailBytes {
				t.Fatalf("sanitized detail is %d bytes", len(detail))
			}
			if strings.ContainsAny(detail, "\r\n") || strings.ContainsAny(detail, "\u0085\u2028\u2029") {
				t.Fatalf("sanitized detail is not one line: %q", detail)
			}
			for _, want := range test.want {
				if !strings.Contains(detail, want) {
					t.Fatalf("sanitized detail %q lost diagnostic %q", detail, want)
				}
			}
		})
	}
}

func withDetail(fact Fact, detail string) Fact {
	fact.Key = fact.Key + ":" + strconv.Itoa(len(detail)) + "-" + detail[:min(8, len(detail))]
	fact.ErrorDetail = detail
	return fact
}

// FJ11 (amended): deleting the seq file with facts present rebuilds the last
// value from the maximum sequence present, and the next append becomes max+1.
func TestFJ11SeqRebuildFromMaxPresent(t *testing.T) {
	journal := Open(t.TempDir())
	docA := testDoc(DocKindTask, "a")
	docB := testDoc(DocKindGoal, "b")
	for i := 0; i < 3; i++ {
		if _, err := journal.Append(Fact{Kind: KindLaunchCreated, Key: fmt.Sprintf("a%d", i), Doc: docA, Launch: fmt.Sprintf("ln-a%d", i)}); err != nil {
			t.Fatalf("Append a%d: %v", i, err)
		}
	}
	if _, err := journal.Append(Fact{Kind: KindLaunchCreated, Key: "b0", Doc: docB, Launch: "ln-b0"}); err != nil {
		t.Fatalf("Append b0: %v", err)
	}
	if err := os.Remove(filepath.Join(journal.dir, "seq")); err != nil {
		t.Fatalf("remove seq: %v", err)
	}
	// The rebuild also must not rewrite a bogus value while the file exists.
	fact, err := journal.Append(Fact{Kind: KindLaunchCreated, Key: "a-next", Doc: docA, Launch: "ln-a-next"})
	if err != nil {
		t.Fatalf("Append after seq loss: %v", err)
	}
	if fact.Seq != 5 {
		t.Fatalf("rebuilt allocation = %d, want max(4)+1 = 5", fact.Seq)
	}
	// The rebuilt counter is persisted as the last allocated value.
	data, err := os.ReadFile(filepath.Join(journal.dir, "seq"))
	if err != nil {
		t.Fatalf("read seq: %v", err)
	}
	if got := strings.TrimSpace(string(data)); got != "5" {
		t.Fatalf("seq file = %q, want the last allocated value 5", got)
	}
}

func TestAppendRejectsInvalidFacts(t *testing.T) {
	journal := Open(t.TempDir())
	cases := []struct {
		name string
		fact Fact
	}{
		{"empty key", Fact{Kind: KindLaunchCreated, Doc: testDoc(DocKindTask, "x")}},
		{"unknown kind", Fact{Kind: "invented_event", Key: "k", Doc: testDoc(DocKindTask, "x")}},
		{"bad doc kind", Fact{Kind: KindLaunchCreated, Key: "k", Doc: testDoc("chats", "x")}},
		{"empty doc id", Fact{Kind: KindLaunchCreated, Key: "k", Doc: testDoc(DocKindTask, "")}},
		{"multiline key", Fact{Kind: KindLaunchCreated, Key: "k\nk", Doc: testDoc(DocKindTask, "x")}},
	}
	for _, testCase := range cases {
		if _, err := journal.Append(testCase.fact); err == nil {
			t.Fatalf("%s: append must fail", testCase.name)
		}
	}
	if facts := requireFact(t, journal, testDoc(DocKindTask, "x")); len(facts) != 0 {
		t.Fatalf("invalid facts must not be recorded, got %d", len(facts))
	}
}

func TestFactTimestampsUseProvidedClock(t *testing.T) {
	journal := Open(t.TempDir())
	stamp := time.Date(2026, 3, 4, 5, 6, 7, 8, time.UTC)
	journal.now = func() time.Time { return stamp }
	fact, err := journal.Append(Fact{Kind: KindLaunchCreated, Key: "k", Doc: testDoc(DocKindTask, "clock"), Launch: "ln"})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if !fact.At.Equal(stamp) {
		t.Fatalf("fact time = %v, want %v", fact.At, stamp)
	}
}
