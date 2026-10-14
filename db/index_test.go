package db

import (
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"testing"
)

func ids(t *testing.T, s *Session, q string) []Value {
	t.Helper()
	res, err := run(s, q)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	var out []Value
	for _, r := range res.Rows {
		out = append(out, r[0])
	}
	return out
}

// Every index lookup must return what a full scan returns, for the transaction asking, through
// updates of indexed columns, deletes, rollbacks, an old snapshot, checkpoints and restarts.
func TestIndexMatchesScan(t *testing.T) {
	old := maxLog
	maxLog = 2048
	defer func() { maxLog = old }()
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	s := db.NewSession("t")
	for _, q := range []string{
		"CREATE TABLE t (id integer PRIMARY KEY, a integer, b text UNIQUE)",
		"CREATE INDEX t_a ON t (a)",
		"CREATE INDEX t_ab ON t (a, b)",
	} {
		if _, err := run(s, q); err != nil {
			t.Fatal(err)
		}
	}
	rng := rand.New(rand.NewSource(1))
	snap := db.NewSession("snap")
	check := func(sess *Session) {
		for v := 0; v < 6; v++ {
			got := ids(t, sess, fmt.Sprintf("SELECT id FROM t WHERE a = %d ORDER BY id", v))
			want := ids(t, sess, fmt.Sprintf("SELECT id FROM t WHERE a + 0 = %d ORDER BY id", v))
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("a = %d: index gave %v, scan gave %v", v, got, want)
			}
			b := fmt.Sprintf("'b%d'", rng.Intn(40))
			got = ids(t, sess, fmt.Sprintf("SELECT id FROM t WHERE a = %d AND b = %s", v, b))
			want = ids(t, sess, fmt.Sprintf("SELECT id FROM t WHERE a + 0 = %d AND b || '' = %s", v, b))
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("a = %d, b = %s: index gave %v, scan gave %v", v, b, got, want)
			}
		}
		lo := rng.Intn(6)
		for _, q := range []string{
			"SELECT id FROM t WHERE a BETWEEN %d AND %d ORDER BY id",
			"SELECT id FROM t WHERE a > %d AND a <= %d ORDER BY id",
			"SELECT id FROM t WHERE %d < a AND %d.5 > a ORDER BY id",
			"SELECT a FROM t WHERE a >= %d OR a < %d ORDER BY a LIMIT 9",
			"SELECT a FROM t ORDER BY a LIMIT %d OFFSET %d",
			"SELECT a FROM t ORDER BY a DESC LIMIT %d OFFSET %d",
			"SELECT a, count(*) FROM t WHERE a < %d OR a IS NULL GROUP BY a ORDER BY a LIMIT %d",
		} {
			q = fmt.Sprintf(q, lo, lo+2)
			got := ids(t, sess, q)
			want := ids(t, sess, strings.NewReplacer("WHERE a", "WHERE a + 0", "AND a", "AND a + 0", "> a", "> a + 0", "OR a", "OR a + 0", "ORDER BY a", "ORDER BY a + 0").Replace(q))
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("%s: index gave %v, scan gave %v", q, got, want)
			}
		}
	}
	for step := 0; step < 3000; step++ {
		id, a, b := rng.Intn(60), rng.Intn(6), rng.Intn(40)
		var q string
		switch rng.Intn(5) {
		case 0, 1:
			q = fmt.Sprintf("INSERT INTO t VALUES (%d, %d, 'b%d')", id, a, b)
			if rng.Intn(8) == 0 {
				q = fmt.Sprintf("INSERT INTO t VALUES (%d, NULL, 'b%d')", id, b)
			}
		case 2:
			q = fmt.Sprintf("UPDATE t SET a = %d WHERE id = %d", a, id)
		case 3:
			q = fmt.Sprintf("DELETE FROM t WHERE a = %d AND id > %d", a, id)
		case 4:
			q = fmt.Sprintf("BEGIN; UPDATE t SET a = %d WHERE a = %d; ROLLBACK", a, rng.Intn(6))
		}
		run(s, q) // duplicate keys are expected to fail
		if s.tx != nil {
			run(s, "ROLLBACK")
		}
		switch {
		case step%500 == 100:
			run(snap, "BEGIN ISOLATION LEVEL REPEATABLE READ")
			check(snap)
		case step%500 == 400:
			check(snap)
			run(snap, "COMMIT")
		}
		if step%50 == 0 {
			check(s)
		}
		if step%1000 == 999 {
			db.Close()
			if db, err = Open(dir); err != nil {
				t.Fatal(err)
			}
			s, snap = db.NewSession("t"), db.NewSession("snap")
			check(s)
		}
	}
	// a unique index still refuses duplicates after all that
	res, _ := run(s, "SELECT b FROM t LIMIT 1")
	if len(res.Rows) == 1 {
		if _, err := run(s, fmt.Sprintf("INSERT INTO t VALUES (1000, 0, '%s')", res.Rows[0][0])); err == nil {
			t.Fatal("duplicate b accepted")
		}
	}
	db.Close()
}

// Two SERIALIZABLE transactions each count shift 7 through an index and add a doctor while the
// shift has fewer than 3: the shift must not end up with 4. Neither touches a row the other
// read, so only the read of the index range can catch it.
func TestIndexReadsSerializable(t *testing.T) {
	db, err := Open("")
	if err != nil {
		t.Fatal(err)
	}
	a, b := db.NewSession("a"), db.NewSession("b")
	for _, q := range []string{
		"CREATE TABLE doctors (id integer PRIMARY KEY, shift integer)",
		"CREATE INDEX doctors_shift ON doctors (shift)",
		"INSERT INTO doctors VALUES (1, 7), (2, 7), (3, 8)",
	} {
		if _, err := run(a, q); err != nil {
			t.Fatal(err)
		}
	}
	var errs []error
	for _, s := range []*Session{a, b} {
		run(s, "BEGIN ISOLATION LEVEL SERIALIZABLE")
		res, err := run(s, "SELECT count(*) FROM doctors WHERE shift = 7")
		if err != nil || res.Rows[0][0] != int64(2) {
			t.Fatal(res, err)
		}
	}
	for i, s := range []*Session{a, b} {
		_, err := run(s, fmt.Sprintf("INSERT INTO doctors VALUES (%d, 7)", 10+i))
		if err == nil {
			_, err = run(s, "COMMIT")
		}
		errs = append(errs, err)
	}
	res, _ := run(db.NewSession("c"), "SELECT count(*) FROM doctors WHERE shift = 7")
	if errs[0] == nil && errs[1] == nil || res.Rows[0][0] != int64(3) {
		t.Fatalf("phantom through an index: %v, %v on shift 7", errs, res.Rows[0][0])
	}
}

// Joins that look the joined table up through its primary key or an index must give what the same
// join gives when it has to load both tables.
func TestJoinLookupsMatchScan(t *testing.T) {
	old := maxLog
	maxLog = 4096
	defer func() { maxLog = old }()
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := db.NewSession("t")
	for _, q := range []string{
		"CREATE TABLE c (id integer PRIMARY KEY, country integer)",
		"CREATE TABLE o (id integer PRIMARY KEY, cid integer, amt integer)",
		"CREATE INDEX o_cid ON o (cid)",
	} {
		if _, err := run(s, q); err != nil {
			t.Fatal(err)
		}
	}
	rng := rand.New(rand.NewSource(3))
	for i := 0; i < 300; i++ {
		run(s, fmt.Sprintf("INSERT INTO c VALUES (%d, %d)", i, i%30))
	}
	for i := 0; i < 3000; i++ {
		run(s, fmt.Sprintf("INSERT INTO o VALUES (%d, %d, %d)", i, rng.Intn(320), rng.Intn(200)))
	}
	pairs := [][2]string{
		{"SELECT c.id, o.id FROM c JOIN o ON o.cid = c.id WHERE c.id = %d ORDER BY 1, 2",
			"SELECT c.id, o.id FROM c JOIN o ON o.cid + 0 = c.id WHERE c.id + 0 = %d ORDER BY 1, 2"},
		{"SELECT c.id, sum(o.amt) FROM c JOIN o ON o.cid = c.id WHERE c.country = %d GROUP BY c.id ORDER BY 1",
			"SELECT c.id, sum(o.amt) FROM c JOIN o ON o.cid + 0 = c.id WHERE c.country = %d GROUP BY c.id ORDER BY 1"},
		{"SELECT o.id, c.country FROM o, c WHERE c.id = o.cid AND o.amt = %d ORDER BY 1",
			"SELECT o.id, c.country FROM o, c WHERE c.id + 0 = o.cid AND o.amt = %d ORDER BY 1"},
	}
	for round := 0; round < 4; round++ {
		for _, p := range pairs {
			for k := 0; k < 15; k++ {
				v := rng.Intn(30)
				got, err1 := run(s, fmt.Sprintf(p[0], v))
				want, err2 := run(s, fmt.Sprintf(p[1], v))
				if err1 != nil || err2 != nil {
					t.Fatal(err1, err2)
				}
				if !reflect.DeepEqual(got.Rows, want.Rows) {
					t.Fatalf("%s with %d: lookups gave %v, scan gave %v", p[0], v, got.Rows, want.Rows)
				}
			}
		}
		for i := 0; i < 200; i++ {
			run(s, fmt.Sprintf("UPDATE o SET cid = %d WHERE id = %d", rng.Intn(320), rng.Intn(3000)))
		}
	}
}

// An inner join's ON used to be checked on its own table even when the planner put that table
// first, before the other table had a row, so a smaller ON table gave no rows at all.
func TestJoinOnSmallerTable(t *testing.T) {
	db, _ := Open("")
	s := db.NewSession("t")
	for _, q := range []string{
		"CREATE TABLE a (id integer PRIMARY KEY)",
		"CREATE TABLE b (id integer PRIMARY KEY, aid integer)",
		"INSERT INTO a VALUES (1), (2)",
		"INSERT INTO b VALUES (1, 1), (2, 1), (3, 2), (4, 2), (5, 3)",
	} {
		if _, err := run(s, q); err != nil {
			t.Fatal(err)
		}
	}
	res, err := run(s, "SELECT count(*) FROM b JOIN a ON a.id = b.aid")
	if err != nil || res.Rows[0][0] != int64(4) {
		t.Fatalf("got %v %v, want 4", res, err)
	}
}
