package db

import (
	"bytes"
	"math"
	"sort"

	"isodb/sql"
)

// An index is a B+tree in the page file with one entry per row, its index key (NULLs included)
// followed by its primary key, for the values the page file holds. Rows in memory are covered by mem instead: every
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

// key is the index key for a lookup or a uniqueness check; null reports a NULL in it.
func (ix *Index) key(vals []Value) (string, bool) {
	return uniqueKey(vals, ix.Cols)
}

// entry is the index key a row is stored under, NULLs included.
func (ix *Index) entry(vals []Value) string {
	k := make([]Value, len(ix.Cols))
	for i, c := range ix.Cols {
		k[i] = vals[c]
	}
	return key(k)
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
				if k := ix.entry(v.vals); !hasKey(now, ix, k) {
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
		ix.root, perr = db.pg.put(ix.root, append([]byte(ix.entry(decodeRow(v))), pk...), nil)
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
		if ix.entry(vals) == k {
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

// keyLen is the length of the first n values of an encoded key (see key).
func keyLen(b []byte, n int) int {
	at := 0
	for ; n > 0 && at < len(b); n-- {
		switch b[at] {
		case 1:
			at += 10
		default:
			i := bytes.IndexByte(b[at+1:], 0xff)
			if i < 0 {
				return len(b)
			}
			at += i + 2
		}
	}
	return min(at, len(b))
}

// scanIndex calls fn for every row visible to the session whose index entry lies in [lo, hi),
// in index order, or in reverse when desc. A nil hi has no upper end.
func (s *Session) scanIndex(tb *Table, ix *Index, lo, hi []byte, desc bool, fn func([]Value, *Row) error) error {
	db := s.db
	db.scan(s.tx, tb)
	in := func(e string) bool { return e >= string(lo) && (hi == nil || e < string(hi)) }
	type ment struct {
		e string
		r *Row
	}
	var mem []ment
	for k, rows := range ix.mem {
		if in(k) {
			for r := range rows {
				mem = append(mem, ment{k + r.key, r})
			}
		}
	}
	before := func(a, b string) bool { return a < b != desc && a != b }
	sort.Slice(mem, func(i, j int) bool { return before(mem[i].e, mem[j].e) })
	visit := func(r *Row, k string) error {
		vals, err := db.read(s.tx, tb, r)
		if err != nil || vals == nil || ix.entry(vals) != k {
			return err
		}
		return fn(vals, r)
	}
	mi := 0
	flush := func(upto string, all bool) error {
		for ; mi < len(mem) && (all || before(mem[mi].e, upto)); mi++ {
			if err := visit(mem[mi].r, mem[mi].e[:len(mem[mi].e)-len(mem[mi].r.key)]); err != nil {
				return err
			}
		}
		return nil
	}
	var ferr error
	disk := func(e, _ []byte) bool {
		if !in(string(e)) {
			return false
		}
		if ferr = flush(string(e), false); ferr != nil {
			return false
		}
		n := keyLen(e, len(ix.Cols))
		pk := string(e[n:])
		if tb.rows[pk] != nil {
			return true
		}
		r, ok := db.fromDisk(tb, pk)
		if !ok {
			ferr = errCorrupt
			return false
		}
		r.transient = true
		ferr = visit(r, string(e[:n]))
		return ferr == nil
	}
	if db.pg != nil && ix.root != 0 {
		var err error
		if desc {
			err = db.pg.seekRev(ix.root, hi, disk)
		} else {
			err = db.pg.seek(ix.root, lo, disk)
		}
		if ferr != nil {
			return ferr
		}
		if err != nil {
			return err
		}
	}
	return flush("", true)
}

// rangeScan reads one table through an index on an integer first column: the rows in a range of
// it, or all its rows in ORDER BY order so LIMIT can stop early.
type rangeScan struct {
	ix      *Index
	bounds  []bound
	ordered bool
	desc    bool
}

type bound struct {
	op string // the column is on the left: col op x
	x  sql.Expr
}

func planRange(src *source, where []sql.Expr, sel *sql.Select, order []sql.Order, l *layout, starts []int) *rangeScan {
	tb := src.table
	isCol := func(x sql.Expr, c int) bool {
		col, ok := x.(*sql.Col)
		return ok && col.Name == tb.Cols[c].Name && (col.Table == "" || col.Table == src.cols[0].table)
	}
	konst := func(x sql.Expr) bool {
		refs, _ := refsOf(x, l, starts)
		return len(refs) == 0 && !hasSubquery(x)
	}
	flip := map[string]string{"<": ">", "<=": ">=", ">": "<", ">=": "<="}
	boundsOn := func(c int) []bound {
		var out []bound
		for _, w := range where {
			switch x := w.(type) {
			case *sql.Binary:
				if _, ok := flip[x.Op]; !ok {
					continue
				}
				if isCol(x.L, c) && konst(x.R) {
					out = append(out, bound{x.Op, x.R})
				} else if isCol(x.R, c) && konst(x.L) {
					out = append(out, bound{flip[x.Op], x.L})
				}
			case *sql.Between:
				if !x.Not && isCol(x.X, c) && konst(x.Lo) && konst(x.Hi) {
					out = append(out, bound{">=", x.Lo}, bound{"<=", x.Hi})
				}
			}
		}
		return out
	}
	// ORDER BY one column of this table with a LIMIT, and nothing grouped
	oc := -1
	if len(order) == 1 && sel.Limit != nil && len(sel.GroupBy) == 0 && !sel.Distinct && !hasAggregate(sel.Having) && !hasAggregate(order[0].Expr) {
		if col, ok := order[0].Expr.(*sql.Col); ok && colIndex(tb, col.Name) >= 0 && isCol(col, colIndex(tb, col.Name)) {
			c := colIndex(tb, col.Name)
			oc = c
			for _, it := range sel.Items {
				if hasAggregate(it.Expr) {
					oc = -1
				}
				if c2, ok := it.Expr.(*sql.Col); it.Alias == col.Name && (!ok || c2.Name != col.Name) {
					oc = -1 // ORDER BY names an output column, not this one
				}
			}
		}
	}
	var best *rangeScan
	for _, ix := range tb.indexes {
		c := ix.Cols[0]
		if tb.Cols[c].Kind != "int" {
			continue // only integer keys sort the way ORDER BY does
		}
		b := boundsOn(c)
		if c == oc {
			return &rangeScan{ix: ix, bounds: b, ordered: true, desc: order[0].Desc}
		}
		if len(b) > 0 && best == nil {
			best = &rangeScan{ix: ix, bounds: b}
		}
	}
	return best
}

// scanRange runs a planned range scan. It reports false, having read nothing, when a plain scan
// is the better way: no usable bounds, or a range covering much of the table.
func (s *Session) scanRange(tb *Table, rs *rangeScan, e *env, fn func([]Value, *Row) error) (bool, error) {
	var lo, hi *int64
	for _, b := range rs.bounds {
		v, err := e.eval(b.x)
		if err != nil {
			return true, err
		}
		if v == nil {
			return true, nil // a comparison with NULL is never true
		}
		var n int64
		switch x := v.(type) {
		case int64:
			n = x
		case float64:
			if x != x || x < -9e18 || x > 9e18 {
				continue
			}
			n = int64(math.Floor(x))
		default:
			continue
		}
		// bounds are widened to whole integers; the WHERE conditions still decide each row
		switch b.op {
		case ">", ">=":
			if lo == nil || n > *lo {
				lo = &n
			}
		case "<", "<=":
			if n == math.MaxInt64 {
				continue
			}
			m := n + 1
			if hi == nil || m < *hi {
				hi = &m
			}
		}
	}
	if lo == nil && hi == nil && !rs.ordered {
		return false, nil
	}
	loB, hiB := []byte{1}, []byte(nil) // 1 starts the integers, after NULL (0)
	if lo != nil {
		loB = []byte(key([]Value{*lo}))
	}
	if hi != nil {
		if lo != nil && *lo >= *hi {
			return true, nil
		}
		hiB = []byte(key([]Value{*hi}))
	}
	if !rs.ordered {
		limit := max(s.db.estimate(tb)/10, 100)
		if s.db.countRange(rs.ix, loB, hiB, limit) >= limit {
			return false, nil
		}
	}
	if lo != nil || hi != nil {
		return true, s.scanIndex(tb, rs.ix, loB, hiB, rs.desc, fn)
	}
	// every row in order: NULLs sort last, so they come first going down
	nulls := func() error { return s.scanIndex(tb, rs.ix, []byte{0}, []byte{1}, rs.desc, fn) }
	if rs.desc {
		if err := nulls(); err != nil {
			return true, err
		}
	}
	if err := s.scanIndex(tb, rs.ix, []byte{1}, nil, rs.desc, fn); err != nil {
		return true, err
	}
	if !rs.desc {
		return true, nulls()
	}
	return true, nil
}

// countRange counts index entries in [lo, hi), stopping at limit.
func (db *DB) countRange(ix *Index, lo, hi []byte, limit int) int {
	n := 0
	for k, rows := range ix.mem {
		if k >= string(lo) && (hi == nil || k < string(hi)) {
			n += len(rows)
		}
	}
	if db.pg != nil && ix.root != 0 {
		db.pg.seek(ix.root, lo, func(e, _ []byte) bool {
			if hi != nil && string(e) >= string(hi) {
				return false
			}
			n++
			return n < limit
		})
	}
	return n
}
