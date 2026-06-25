package clickhouse

import (
	"strings"
	"testing"
)

// A ';' inside a comment must not split a statement: the real schema.sql has a
// comment that mentions "the source LSN; ReplacingMergeTree ...", which broke
// the naive split-then-strip implementation.
func TestStatementsIgnoresSemicolonsInComments(t *testing.T) {
	sql := `-- intro comment mentioning a version; and ReplacingMergeTree in prose
--   SELECT * FROM cdc.orders FINAL WHERE _is_deleted = 0;
CREATE TABLE x (id Int64) ENGINE = ReplacingMergeTree(v) ORDER BY id;
-- another; comment
CREATE TABLE y (id Int64) ENGINE = ReplacingMergeTree(v) ORDER BY id;`

	got := statements(sql)
	if len(got) != 2 {
		t.Fatalf("got %d statements, want 2: %#v", len(got), got)
	}
	for _, s := range got {
		if !strings.HasPrefix(s, "CREATE TABLE") {
			t.Errorf("statement does not start with CREATE TABLE: %q", s)
		}
	}
}

func TestStatementsParsesEmbeddedSchema(t *testing.T) {
	got := statements(schemaSQL)
	if len(got) != 2 {
		t.Fatalf("embedded schema produced %d statements, want 2", len(got))
	}
	for _, s := range got {
		if !strings.HasPrefix(s, "CREATE TABLE") {
			t.Errorf("statement does not start with CREATE TABLE: %q", s)
		}
	}
}
