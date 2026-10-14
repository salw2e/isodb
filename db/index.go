package db

import (
	"bytes"
	"sort"

	"isodb/sql"
)

// An index is a B+tree in the page file with one entry per row, its index key followed by its
// primary key, for the values the page file holds. Rows in memory are covered by mem instead: every
// version they hold is listed under its key. A lookup reads both and rechecks each row it finds.
type Index struct {
	Name   string
	Cols   []int
	Unique bool
	root   pageID
	mem    map[string]map[*Row]bool
}

type ixKey struct {
	ix *Index
	k  string
}

func (ix *Index) key(vals []Value) (string, bool) {
	return uniqueKey(vals, ix.Cols)
}

// reindex brings mem up to date with the versions r holds now.
func (db *DB) reindex(r *Row) {
	if len(r.tb.indexes) == 0 {
		return
	}
	var now []ixKey
	if r.tb.rows[r.key] == r {
		for _, ix := range r.tb.indexes {
			for _, v := range r.vers {
				if v.dead {
					continue
				}
				if k, null := ix.key(v.vals); !null && !hasKey(now, ix, k) {
					now = append(now, ixKey{ix, k})
				}
			}
		}
	}
	for _, e := range r.ix {
		if !hasKey(now, e.ix, e.k) {
			if m := e.ix.mem[e.k]; m != nil {
				delete(m, r)
				if len(m) == 0 {
					delete(e.ix.mem, e.k)
				}
			}
		}
	}
	for _, e := range now {
		if !hasKey(r.ix, e.ix, e.k) {
			m := e.ix.mem[e.k]
			if m == nil {
				m = map[*Row]bool{}
				e.ix.mem[e.k] = m
			}
			m[r] = true
		}
	}
	r.ix = now
}

func hasKey(list []ixKey, ix *Index, k string) bool {
	for _, e := range list {
		if e.ix == ix && e.k == k {
			return true
		}
	}
	return false
}

// build fills a new index from the page file and the rows in memory.
func (db *DB) build(tb *Table, ix *Index) error {
	ix.mem = map[string]map[*Row]bool{}
	ix.root = 0
	for _, r := range tb.order {
		if tb.rows[r.key] == r {
			db.reindex(r)
		}
	}
	if db.pg == nil || tb.root == 0 {
		return nil
	}
	var perr error
	err := db.pg.scan(tb.root, func(pk, v []byte) bool {
		if k, null := ix.key(decodeRow(v)); !null {
			ix.root, perr = db.pg.put(ix.root, append([]byte(k), pk...), nil)
		}
		return perr == nil
	})
	if perr != nil {
		return perr
	}
	return err
}

// candidates returns every row that holds, or held in the page file, key k in ix, in key order.
// Rows only in the page file come as temporary values.
func (db *DB) candidates(tb *Table, ix *Index, k string) ([]*Row, error) {
	var out []*Row
	for r := range ix.mem[k] {
		out = append(out, r)
	}
	if db.pg != nil && ix.root != 0 {
		var ferr error
		err := db.pg.seek(ix.root, []byte(k), func(e, _ []byte) bool {
			if !bytes.HasPrefix(e, []byte(k)) {
				return false
			}
			pk := string(e[len(k):])
			if tb.rows[pk] != nil {
				return true
			}
			r, ok := db.fromDisk(tb, pk)
			if !ok {
				ferr = errCorrupt
				return false
			}
			r.transient = true
			out = append(out, r)
			return true
		})
		if ferr != nil {
			return nil, ferr
		}
		if err != nil {
			return nil, err
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].key < out[j].key })
	return out, nil
}

// indexed returns the rows visible to the session whose values have key k in ix.
func (s *Session) indexed(tb *Table, ix *Index, k string) ([][]Value, []*Row, error) {
	db := s.db
	db.scan(s.tx, tb) // under SERIALIZABLE an index read guards the whole table, like a scan
	rs, err := db.candidates(tb, ix, k)
	if err != nil {
		return nil, nil, err
	}
	var rows [][]Value
	var refs []*Row
	for _, r := range rs {
		vals, err := db.read(s.tx, tb, r)
		if err != nil {
			return nil, nil, err
		}
		if vals == nil {
			continue
		}
		if k2, null := ix.key(vals); !null && k2 == k {
			rows, refs = append(rows, vals), append(refs, r)
		}
	}
	return rows, refs, nil
}

// pickIndex finds an index whose every column eq sets equal to a value, preferring unique ones.
func pickIndex(tb *Table, eq func(name string) sql.Expr) (*Index, []sql.Expr) {
	var best *Index
	var bestX []sql.Expr
	for _, ix := range tb.indexes {
		xs := make([]sql.Expr, len(ix.Cols))
		ok := true
		for i, c := range ix.Cols {
			if xs[i] = eq(tb.Cols[c].Name); xs[i] == nil {
				ok = false
				break
			}
		}
		if ok && (best == nil || ix.Unique && !best.Unique || len(ix.Cols) > len(best.Cols) && ix.Unique == best.Unique) {
			best, bestX = ix, xs
		}
	}
	return best, bestX
}

// lookupKey evaluates the values an index lookup is for. ok is false when one is NULL: = matches
// nothing then.
func lookupKey(tb *Table, ix *Index, xs []sql.Expr, e *env) (string, bool, error) {
	vals := make([]Value, len(xs))
	for i, x := range xs {
		v, err := e.eval(x)
		if err != nil || v == nil {
			return "", false, err
		}
		if vals[i], err = coerce(v, tb.Cols[ix.Cols[i]].Kind); err != nil {
			return "", false, err
		}
	}
	return key(vals), true, nil
}

// probe looks a joined table up through its primary key (ix nil) or an index, once per row of
// the tables in deps, instead of loading it whole.
type probe struct {
	ix     *Index
	xs     []sql.Expr
	deps   map[int]bool
	lazy   bool
	filter []sql.Expr // conditions on this table alone, applied to what a lookup finds
}

func (s *Session) probeRows(tb *Table, pr *probe, e *env) ([][]Value, []*Row, error) {
	cols := tb.PK
	if pr.ix != nil {
		cols = pr.ix.Cols
	}
	vals := make([]Value, len(pr.xs))
	for i, x := range pr.xs {
		v, err := e.eval(x)
		if err != nil || v == nil {
			return nil, nil, err
		}
		if vals[i], err = coerce(v, tb.Cols[cols[i]].Kind); err != nil {
			return nil, nil, nil // a value of another type matches no key; the join condition still decides
		}
	}
	if pr.ix != nil {
		return s.indexed(tb, pr.ix, key(vals))
	}
	row, r, err := s.lookup(tb, key(vals))
	if err != nil || row == nil {
		return nil, nil, err
	}
	return [][]Value{row}, []*Row{r}, nil
}

// estimate guesses how many rows tb has from the leftmost path down its tree.
func (db *DB) estimate(tb *Table) int {
	n := len(tb.rows)
	if db.pg == nil || tb.root == 0 {
		return n
	}
	mult := 1
	nd, err := db.pg.load(tb.root)
	for err == nil && !nd.leaf {
		mult *= len(nd.kids)
		nd, err = db.pg.load(nd.kids[0])
	}
	if err != nil {
		return n
	}
	return n + mult*len(nd.keys)
}
