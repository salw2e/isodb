package db

import (
	"math/rand"
	"sort"
	"sync"
	"strings"
	"time"

	"isodb/sql"
)

type Session struct {
	db       *DB
	tx       *Txn
	iso      int
	nextIso  int
	failed   bool
	params   []Value
	user     string
	rand     *rand.Rand
	settings map[string]string
	subs     map[*sql.Select]*subResult
	corr     map[*sql.Select]bool
}

type subResult struct {
	res     *result
	set     map[string]bool
	hasNull bool
}

// Result of one statement.
type Result struct {
	Cols  []string
	Kinds []string
	Rows  [][]Value
	Tag   string
}

func (r *Result) OIDs() []int32 {
	out := make([]int32, len(r.Kinds))
	for i, k := range r.Kinds {
		out[i] = oid(k)
	}
	return out
}

func Open(dir string) (*DB, error) {
	db := &DB{tables: map[string]*Table{}, indexes: map[string]string{}, indexText: map[string]string{}, views: map[string]*sql.Select{}, viewText: map[string]string{}, active: map[*Txn]bool{}}
	db.cond = sync.NewCond(&db.mu)
	if dir == "" {
		return db, nil
	}
	w, err := openWAL(db, dir)
	if err != nil {
		return nil, err
	}
	db.wal = w
	return db, nil
}

func (db *DB) Close() error {
	if db.wal != nil {
		return db.wal.close()
	}
	return nil
}

func (db *DB) NewSession(user string) *Session {
	return &Session{db: db, user: user, iso: ReadCommitted, nextIso: -1, rand: rand.New(rand.NewSource(time.Now().UnixNano())), settings: map[string]string{}}
}

func (db *DB) table(name string) (*Table, error) {
	tb := db.tables[name]
	if tb == nil {
		return nil, errf(codeUndefTable, "relation \"%s\" does not exist", name)
	}
	return tb, nil
}

// Status is the transaction state reported to the client: I idle, T in a transaction, E failed.
func (s *Session) Status() byte {
	if s.failed {
		return 'E'
	}
	if s.tx != nil {
		return 'T'
	}
	return 'I'
}

func (s *Session) Close() {
	s.db.mu.Lock()
	defer s.db.mu.Unlock()
	if s.tx != nil {
		s.db.abortTx(s.tx)
		s.tx = nil
	}
}

func (s *Session) now() string {
	return time.Now().UTC().Format("2006-01-02 15:04:05.999999+00")
}

func isoName(i int) string {
	switch i {
	case Serializable:
		return "serializable"
	case RepeatableRead:
		return "repeatable read"
	}
	return "read committed"
}

func isoLevel(s string) int {
	switch strings.ToLower(strings.Trim(s, "' ")) {
	case "serializable":
		return Serializable
	case "repeatable read", "repeatable_read":
		return RepeatableRead
	}
	return ReadCommitted
}

// Run executes one parsed statement. text is its source, kept for schema changes in the log.
func (s *Session) Run(st sql.Stmt, text string, params []Value) (*Result, error) {
	db := s.db
	db.mu.Lock()
	defer db.mu.Unlock()
	s.params = params
	s.subs, s.corr = nil, nil

	switch st := st.(type) {
	case *sql.Begin:
		if s.tx != nil {
			return &Result{Tag: "BEGIN"}, nil
		}
		iso := s.iso
		if s.nextIso >= 0 {
			iso, s.nextIso = s.nextIso, -1
		}
		if st.Iso != "" {
			iso = isoLevel(st.Iso)
		}
		s.tx = db.begin(iso, true)
		s.tx.start = -1
		return &Result{Tag: "BEGIN"}, nil
	case *sql.Commit:
		if s.tx == nil {
			return &Result{Tag: "COMMIT"}, nil
		}
		tx, failed := s.tx, s.failed
		s.tx, s.failed = nil, false
		if failed {
			db.abortTx(tx)
			return &Result{Tag: "ROLLBACK"}, nil
		}
		if err := db.commitTx(tx); err != nil {
			return nil, err
		}
		return &Result{Tag: "COMMIT"}, nil
	case *sql.Rollback:
		if s.tx != nil {
			db.abortTx(s.tx)
		}
		s.tx, s.failed = nil, false
		return &Result{Tag: "ROLLBACK"}, nil
	}
	if s.failed {
		return nil, errf(codeFailedTx, "current transaction is aborted, commands ignored until end of transaction block")
	}
	switch st := st.(type) {
	case *sql.SetTx:
		if st.Iso != "" {
			if s.tx != nil {
				s.tx.iso = isoLevel(st.Iso)
			} else {
				s.nextIso = isoLevel(st.Iso)
			}
		}
		return &Result{Tag: "SET"}, nil
	case *sql.SetVar:
		name := strings.ToLower(st.Name)
		s.settings[name] = st.Value
		if name == "default_transaction_isolation" || name == "transaction_isolation" {
			s.iso = isoLevel(st.Value)
		}
		return &Result{Tag: "SET"}, nil
	case *sql.Show:
		return s.show(st.Name), nil
	case *sql.CreateTable, *sql.DropTable, *sql.CreateIndex, *sql.DropIndex, *sql.CreateView, *sql.DropView:
		res, err := s.ddl(st, text)
		if err == nil && db.wal != nil && !db.recovering {
			err = db.wal.ddl(db, text)
		}
		return res, err
	}

	implicit := s.tx == nil
	if implicit {
		iso := s.iso
		if s.nextIso >= 0 {
			iso, s.nextIso = s.nextIso, -1
		}
		s.tx = db.begin(iso, false)
	}
	t := s.tx
	if t.start < 0 || t.iso == ReadCommitted {
		t.start = db.ts
	}
	res, err := s.exec(st)
	if implicit {
		s.tx = nil
		if err != nil {
			db.abortTx(t)
			return nil, err
		}
		if err := db.commitTx(t); err != nil {
			return nil, err
		}
		return res, nil
	}
	if err != nil {
		db.abortTx(t)
		s.failed = true
	}
	return res, err
}

func (s *Session) show(name string) *Result {
	name = strings.ToLower(name)
	v := ""
	switch name {
	case "transaction isolation level", "transaction_isolation":
		iso := s.iso
		if s.tx != nil {
			iso = s.tx.iso
		}
		v = isoName(iso)
	case "default_transaction_isolation":
		v = isoName(s.iso)
	case "server_version":
		v = "14.0"
	case "server_encoding", "client_encoding":
		v = "UTF8"
	case "standard_conforming_strings":
		v = "on"
	case "timezone":
		v = "UTC"
	case "datestyle":
		v = "ISO, MDY"
	default:
		v = s.settings[name]
	}
	return &Result{Cols: []string{name}, Kinds: []string{"text"}, Rows: [][]Value{{v}}, Tag: "SHOW"}
}

func (s *Session) exec(st sql.Stmt) (*Result, error) {
	switch st := st.(type) {
	case *sql.Select:
		res, err := s.query(st, nil)
		if err != nil {
			return nil, err
		}
		return &Result{Cols: res.cols, Kinds: res.kinds, Rows: res.rows, Tag: "SELECT " + itoa(len(res.rows))}, nil
	case *sql.Insert:
		return s.insert(st)
	case *sql.Update:
		return s.update(st)
	case *sql.Delete:
		return s.delete(st)
	case *sql.Empty:
		return &Result{}, nil
	}
	return nil, errf(codeFeature, "statement not supported")
}

func itoa(n int) string { return text(int64(n)) }

func (s *Session) ddl(st sql.Stmt, txt string) (*Result, error) {
	db := s.db
	switch st := st.(type) {
	case *sql.CreateTable:
		if db.tables[st.Name] != nil {
			if st.IfNotExists {
				return &Result{Tag: "CREATE TABLE"}, nil
			}
			return nil, errf(codeDupTable, "relation \"%s\" already exists", st.Name)
		}
		tb := &Table{Name: st.Name, rows: map[string]*Row{}, ddl: txt}
		seen := map[string]bool{}
		for i, c := range st.Cols {
			if seen[c.Name] {
				return nil, errf("42701", "column \"%s\" specified more than once", c.Name)
			}
			seen[c.Name] = true
			tb.Cols = append(tb.Cols, Column{Name: c.Name, Kind: kind(c.Type), Type: c.Type, NotNull: c.NotNull, Serial: c.Serial, Default: c.Default})
			if c.PK {
				tb.PK = append(tb.PK, i)
			}
			if c.Unique {
				tb.Uniques = append(tb.Uniques, []int{i})
			}
		}
		for _, n := range st.PK {
			i := colIndex(tb, n)
			if i < 0 {
				return nil, errf(codeUndefCol, "column \"%s\" named in key does not exist", n)
			}
			tb.PK = append(tb.PK, i)
			tb.Cols[i].NotNull = true
		}
		db.tables[st.Name] = tb
		db.ddl = append(db.ddl, txt)
		return &Result{Tag: "CREATE TABLE"}, nil
	case *sql.DropTable:
		for _, n := range st.Names {
			if db.tables[n] == nil {
				if st.IfExists {
					continue
				}
				return nil, errf(codeUndefTable, "table \"%s\" does not exist", n)
			}
			delete(db.tables, n)
			for i, t := range db.indexes {
				if t == n {
					delete(db.indexes, i)
					delete(db.indexText, i)
				}
			}
		}
		db.ddl = append(db.ddl, txt)
		return &Result{Tag: "DROP TABLE"}, nil
	case *sql.CreateIndex:
		tb, err := db.table(st.Table)
		if err != nil {
			return nil, err
		}
		if st.Name != "" && db.indexes[st.Name] != "" {
			if st.IfNotExists {
				return &Result{Tag: "CREATE INDEX"}, nil
			}
			return nil, errf(codeDupTable, "relation \"%s\" already exists", st.Name)
		}
		var cols []int
		for _, n := range st.Cols {
			i := colIndex(tb, n)
			if i < 0 {
				return nil, errf(codeUndefCol, "column \"%s\" does not exist", n)
			}
			cols = append(cols, i)
		}
		if st.Unique {
			seen := map[string]bool{}
			for _, r := range tb.order {
				v := latestCommitted(r)
				if v == nil || v.dead {
					continue
				}
				k, null := uniqueKey(v.vals, cols)
				if null {
					continue
				}
				if seen[k] {
					return nil, errf(codeUnique, "could not create unique index \"%s\"", st.Name)
				}
				seen[k] = true
			}
			tb.Uniques = append(tb.Uniques, cols)
		}
		if st.Name == "" {
			st.Name = st.Table + "_" + strings.Join(st.Cols, "_") + "_idx"
			txt = "CREATE " + map[bool]string{true: "UNIQUE ", false: ""}[st.Unique] + "INDEX " + st.Name + " ON " + st.Table + " (" + strings.Join(st.Cols, ", ") + ")"
		}
		db.indexes[st.Name] = st.Table
		db.indexText[st.Name] = txt
		db.ddl = append(db.ddl, txt)
		return &Result{Tag: "CREATE INDEX"}, nil
	case *sql.CreateView:
		if db.views[st.Name] != nil || db.tables[st.Name] != nil {
			if st.IfNotExists {
				return &Result{Tag: "CREATE VIEW"}, nil
			}
			return nil, errf(codeDupTable, "relation \"%s\" already exists", st.Name)
		}
		if _, _, err := s.describe(st.Sel); err != nil {
			return nil, err
		}
		db.views[st.Name], db.viewText[st.Name] = st.Sel, txt
		return &Result{Tag: "CREATE VIEW"}, nil
	case *sql.DropView:
		for _, n := range st.Names {
			if db.views[n] == nil {
				if st.IfExists {
					continue
				}
				return nil, errf(codeUndefTable, "view \"%s\" does not exist", n)
			}
			delete(db.views, n)
			delete(db.viewText, n)
		}
		return &Result{Tag: "DROP VIEW"}, nil
	case *sql.DropIndex:
		if db.indexes[st.Name] == "" && !st.IfExists {
			return nil, errf(codeUndefTable, "index \"%s\" does not exist", st.Name)
		}
		delete(db.indexes, st.Name)
		delete(db.indexText, st.Name)
		db.ddl = append(db.ddl, txt)
		return &Result{Tag: "DROP INDEX"}, nil
	}
	return nil, errf(codeFeature, "unsupported statement")
}

func (db *DB) indexDDL() []string {
	var out []string
	for _, t := range db.indexText {
		out = append(out, t)
	}
	sort.Strings(out)
	var views []string
	for _, t := range db.viewText {
		views = append(views, t)
	}
	sort.Strings(views)
	return append(out, views...)
}

func latestCommitted(r *Row) *Version {
	for i := len(r.vers) - 1; i >= 0; i-- {
		if r.vers[i].tx.state == committed {
			return r.vers[i]
		}
	}
	return nil
}

func colIndex(tb *Table, name string) int {
	for i, c := range tb.Cols {
		if c.Name == name {
			return i
		}
	}
	return -1
}

func uniqueKey(vals []Value, cols []int) (string, bool) {
	k := make([]Value, len(cols))
	for i, c := range cols {
		if vals[c] == nil {
			return "", true
		}
		k[i] = vals[c]
	}
	return key(k), false
}

func (tb *Table) keyOf(vals []Value) string {
	k := make([]Value, len(tb.PK))
	for i, c := range tb.PK {
		k[i] = vals[c]
	}
	return key(k)
}

// checkUnique looks for another live row with the same values in a unique column set.
func (s *Session) checkUnique(tb *Table, self *Row, vals []Value) error {
	for _, cols := range tb.Uniques {
		k, null := uniqueKey(vals, cols)
		if null {
			continue
		}
		for _, r := range tb.order {
			if r == self || len(r.vers) == 0 {
				continue
			}
			v := r.vers[len(r.vers)-1]
			if v.tx != s.tx && v.tx.state != committed {
				if v.tx.state == active || v.tx.state == committing {
					if err := s.db.lock(s.tx, tb, r); err != nil {
						return err
					}
				}
				v = latest(s.tx, r)
				if v == nil {
					continue
				}
			}
			if v.dead {
				continue
			}
			if k2, _ := uniqueKey(v.vals, cols); k2 == k {
				return errf(codeUnique, "duplicate key value violates unique constraint on %s", tb.Name)
			}
		}
	}
	return nil
}

func (s *Session) rowValues(tb *Table, cols []int, exprs []sql.Expr, e *env) ([]Value, error) {
	vals := make([]Value, len(tb.Cols))
	given := make([]bool, len(tb.Cols))
	for i, c := range cols {
		x := exprs[i]
		if f, ok := x.(*sql.Func); ok && f.Name == "$default" {
			continue
		}
		v, err := e.eval(x)
		if err != nil {
			return nil, err
		}
		vals[c], given[c] = v, true
	}
	for i, c := range tb.Cols {
		if given[i] {
			continue
		}
		if c.Serial {
			tb.nextID++
			vals[i] = tb.nextID
			if s.tx.serials == nil {
				s.tx.serials = map[*Table]bool{}
			}
			s.tx.serials[tb] = true
		} else if c.Default != nil {
			v, err := e.eval(c.Default)
			if err != nil {
				return nil, err
			}
			vals[i] = v
		}
	}
	return s.finish(tb, vals)
}

func (s *Session) finish(tb *Table, vals []Value) ([]Value, error) {
	for i, c := range tb.Cols {
		v, err := coerce(vals[i], c.Kind)
		if err != nil {
			return nil, err
		}
		if v == nil && c.NotNull {
			return nil, errf(codeNotNull, "null value in column \"%s\" of relation \"%s\" violates not-null constraint", c.Name, tb.Name)
		}
		vals[i] = v
	}
	return vals, nil
}

func (s *Session) returning(tb *Table, items []sql.SelItem, rows [][]Value, res *Result) error {
	if items == nil {
		return nil
	}
	l := newLayout(s.tableCols(tb, tb.Name))
	sel := &sql.Select{Items: items}
	r, err := s.project(sel, l, rows, nil, nil, false)
	if err != nil {
		return err
	}
	res.Cols, res.Kinds, res.Rows = r.cols, r.kinds, r.rows
	return nil
}

func (s *Session) insert(st *sql.Insert) (*Result, error) {
	db := s.db
	tb, err := db.table(st.Table)
	if err != nil {
		return nil, err
	}
	var cols []int
	if st.Cols == nil {
		for i := range tb.Cols {
			cols = append(cols, i)
		}
	} else {
		for _, n := range st.Cols {
			i := colIndex(tb, n)
			if i < 0 {
				return nil, errf(codeUndefCol, "column \"%s\" of relation \"%s\" does not exist", n, tb.Name)
			}
			cols = append(cols, i)
		}
	}
	rows := st.Rows
	if st.Select != nil {
		res, err := s.query(st.Select, nil)
		if err != nil {
			return nil, err
		}
		for _, r := range res.rows {
			row := make([]sql.Expr, len(r))
			for i, v := range r {
				row[i] = &sql.Lit{V: v}
			}
			rows = append(rows, row)
		}
	}
	e := &env{s: s, params: s.params}
	n := 0
	var out [][]Value
	for _, exprs := range rows {
		if len(exprs) > len(cols) || st.Select != nil && len(exprs) != len(cols) {
			return nil, errf(codeSyntax, "INSERT has more expressions than target columns")
		}
		vals, err := s.rowValues(tb, cols[:len(exprs)], exprs, e)
		if err != nil {
			return nil, err
		}
		var k string
		if len(tb.PK) == 0 {
			tb.nextID++
			k = key([]Value{tb.nextID})
			if s.tx.serials == nil {
				s.tx.serials = map[*Table]bool{}
			}
			s.tx.serials[tb] = true
		} else {
			k = tb.keyOf(vals)
		}
		r := db.row(tb, k)
		if err := db.lock(s.tx, tb, r); err != nil {
			return nil, err
		}
		if v := latest(s.tx, r); v != nil && !v.dead {
			if s.tx.iso == Serializable && !s.tx.visible(v) {
				return nil, errf(codeSerialization, "could not serialize access due to concurrent insert")
			}
			if st.Conflict != nil {
				if st.Conflict.Nothing {
					continue
				}
				nv, err := s.setValues(tb, v.vals, st.Conflict.Sets, vals)
				if err != nil {
					return nil, err
				}
				if err := s.checkUnique(tb, r, nv); err != nil {
					return nil, err
				}
				db.write(s.tx, tb, r, nv, false)
				n++
				out = append(out, nv)
				continue
			}
			return nil, errf(codeUnique, "duplicate key value violates unique constraint \"%s_pkey\"", tb.Name)
		}
		if err := s.checkUnique(tb, r, vals); err != nil {
			return nil, err
		}
		db.write(s.tx, tb, r, vals, false)
		n++
		out = append(out, vals)
	}
	res := &Result{Tag: "INSERT 0 " + itoa(n)}
	return res, s.returning(tb, st.Returning, out, res)
}

// setValues applies SET clauses to a row; excluded holds the row an upsert tried to insert.
func (s *Session) setValues(tb *Table, old []Value, sets []sql.Set, excluded []Value) ([]Value, error) {
	cols := s.tableCols(tb, tb.Name)
	if excluded != nil {
		for _, c := range tb.Cols {
			cols = append(cols, colInfo{"excluded", c.Name, c.Kind})
		}
	}
	e := &env{l: newLayout(cols), row: append(append([]Value(nil), old...), excluded...), s: s, params: s.params}
	nv := append([]Value(nil), old...)
	for _, set := range sets {
		i := colIndex(tb, set.Col)
		if i < 0 {
			return nil, errf(codeUndefCol, "column \"%s\" of relation \"%s\" does not exist", set.Col, tb.Name)
		}
		if f, ok := set.Expr.(*sql.Func); ok && f.Name == "$default" {
			nv[i] = nil
			if tb.Cols[i].Default != nil {
				v, err := e.eval(tb.Cols[i].Default)
				if err != nil {
					return nil, err
				}
				nv[i] = v
			}
			continue
		}
		v, err := e.eval(set.Expr)
		if err != nil {
			return nil, err
		}
		nv[i] = v
	}
	return s.finish(tb, nv)
}

type match struct {
	r    *Row
	vals []Value
}

// matching returns the rows of tb that satisfy where, with their values as the transaction sees them.
func (s *Session) matching(tb *Table, alias string, where sql.Expr) ([]match, *env, error) {
	if alias == "" {
		alias = tb.Name
	}
	l := newLayout(s.tableCols(tb, alias))
	e := &env{l: l, s: s, params: s.params}
	var cands []match
	done := false
	if len(tb.PK) == 1 {
		pk := tb.Cols[tb.PK[0]].Name
		for _, c := range splitAnd(where, nil) {
			b, ok := c.(*sql.Binary)
			if !ok || b.Op != "=" {
				continue
			}
			for _, side := range [][2]sql.Expr{{b.L, b.R}, {b.R, b.L}} {
				col, ok := side[0].(*sql.Col)
				if !ok || col.Name != pk || col.Table != "" && col.Table != alias {
					continue
				}
				refs, _ := refsOf(side[1], l, []int{0})
				if len(refs) > 0 || hasSubquery(side[1]) {
					continue
				}
				v, err := e.eval(side[1])
				if err != nil {
					return nil, nil, err
				}
				if v != nil {
					if v, err = coerce(v, tb.Cols[tb.PK[0]].Kind); err != nil {
						return nil, nil, err
					}
					vals, r, err := s.lookup(tb, key([]Value{v}))
					if err != nil {
						return nil, nil, err
					}
					if vals != nil {
						cands = append(cands, match{r, vals})
					}
				}
				done = true
				break
			}
			if done {
				break
			}
		}
	}
	if !done {
		rows, refs, err := s.visibleRows(tb)
		if err != nil {
			return nil, nil, err
		}
		for i := range rows {
			cands = append(cands, match{refs[i], rows[i]})
		}
	}
	var out []match
	for _, m := range cands {
		ok, err := s.test(e, m.vals, where)
		if err != nil {
			return nil, nil, err
		}
		if ok {
			out = append(out, m)
		}
	}
	return out, e, nil
}

func (s *Session) test(e *env, row []Value, where sql.Expr) (bool, error) {
	if where == nil {
		return true, nil
	}
	e.row = row
	v, err := e.eval(where)
	if err != nil {
		return false, err
	}
	b, null, err := truth(v)
	return b && !null, err
}

// relock takes the write lock on a matched row. Read committed then continues from the newest
// committed version, re-checking the WHERE clause like Postgres does; snapshot levels fail instead.
func (s *Session) relock(tb *Table, m match, e *env, where sql.Expr) ([]Value, error) {
	if err := s.db.lock(s.tx, tb, m.r); err != nil {
		return nil, err
	}
	if s.tx.iso != ReadCommitted {
		return m.vals, nil
	}
	v := latest(s.tx, m.r)
	if v == nil || v.dead {
		return nil, nil
	}
	ok, err := s.test(e, v.vals, where)
	if err != nil || !ok {
		return nil, err
	}
	return v.vals, nil
}

func (s *Session) update(st *sql.Update) (*Result, error) {
	db := s.db
	tb, err := db.table(st.Table)
	if err != nil {
		return nil, err
	}
	ms, e, err := s.matching(tb, st.Alias, st.Where)
	if err != nil {
		return nil, err
	}
	n := 0
	var out [][]Value
	for _, m := range ms {
		old, err := s.relock(tb, m, e, st.Where)
		if err != nil {
			return nil, err
		}
		if old == nil {
			continue
		}
		nv, err := s.setValues(tb, old, st.Sets, nil)
		if err != nil {
			return nil, err
		}
		r := m.r
		if len(tb.PK) > 0 && tb.keyOf(nv) != r.key {
			r2 := db.row(tb, tb.keyOf(nv))
			if err := db.lock(s.tx, tb, r2); err != nil {
				return nil, err
			}
			if v := latest(s.tx, r2); v != nil && !v.dead {
				return nil, errf(codeUnique, "duplicate key value violates unique constraint \"%s_pkey\"", tb.Name)
			}
			db.write(s.tx, tb, r, nil, true)
			r = r2
		}
		if err := s.checkUnique(tb, r, nv); err != nil {
			return nil, err
		}
		db.write(s.tx, tb, r, nv, false)
		n++
		out = append(out, nv)
	}
	res := &Result{Tag: "UPDATE " + itoa(n)}
	return res, s.returning(tb, st.Returning, out, res)
}

func (s *Session) delete(st *sql.Delete) (*Result, error) {
	db := s.db
	tb, err := db.table(st.Table)
	if err != nil {
		return nil, err
	}
	ms, e, err := s.matching(tb, st.Alias, st.Where)
	if err != nil {
		return nil, err
	}
	n := 0
	var out [][]Value
	for _, m := range ms {
		old, err := s.relock(tb, m, e, st.Where)
		if err != nil {
			return nil, err
		}
		if old == nil {
			continue
		}
		db.write(s.tx, tb, m.r, nil, true)
		n++
		out = append(out, old)
	}
	res := &Result{Tag: "DELETE " + itoa(n)}
	return res, s.returning(tb, st.Returning, out, res)
}

// Describe reports the result columns of a statement without running it.
func (s *Session) Describe(st sql.Stmt) ([]string, []string, error) {
	s.db.mu.Lock()
	defer s.db.mu.Unlock()
	var sel *sql.Select
	var tb *Table
	var items []sql.SelItem
	switch st := st.(type) {
	case *sql.Select:
		sel = st
	case *sql.Insert:
		items, tb = st.Returning, s.db.tables[st.Table]
	case *sql.Update:
		items, tb = st.Returning, s.db.tables[st.Table]
	case *sql.Delete:
		items, tb = st.Returning, s.db.tables[st.Table]
	case *sql.Show:
		return []string{st.Name}, []string{"text"}, nil
	default:
		return nil, nil, nil
	}
	if sel == nil {
		if items == nil || tb == nil {
			return nil, nil, nil
		}
		sel = &sql.Select{Items: items, From: []sql.TableRef{{Name: tb.Name}}}
	}
	return s.describe(sel)
}

func (s *Session) describe(sel *sql.Select) ([]string, []string, error) {
	var all []colInfo
	for _, tr := range sel.From {
		if tr.Sub != nil {
			names, kinds, err := s.describe(tr.Sub)
			if err != nil {
				return nil, nil, err
			}
			for i, n := range names {
				all = append(all, colInfo{tr.Alias, n, kinds[i]})
			}
			continue
		}
		alias := tr.Alias
		if alias == "" {
			alias = tr.Name
		}
		if v := s.db.views[tr.Name]; v != nil && s.db.tables[tr.Name] == nil {
			names, kinds, err := s.describe(v)
			if err != nil {
				return nil, nil, err
			}
			for i, n := range names {
				all = append(all, colInfo{alias, n, kinds[i]})
			}
			continue
		}
		tb, err := s.db.table(tr.Name)
		if err != nil {
			return nil, nil, err
		}
		all = append(all, s.tableCols(tb, alias)...)
	}
	l := newLayout(all)
	var names, kinds []string
	for _, it := range sel.Items {
		if it.Star {
			for _, c := range all {
				if it.Table == "" || it.Table == c.table {
					names, kinds = append(names, c.name), append(kinds, c.kind)
				}
			}
			continue
		}
		name := it.Alias
		if name == "" {
			switch x := it.Expr.(type) {
			case *sql.Col:
				name = x.Name
			case *sql.Func:
				name = x.Name
			default:
				name = "?column?"
			}
		}
		k := kindOf(it.Expr, l)
		if k == "" {
			k = "text"
		}
		names, kinds = append(names, name), append(kinds, k)
	}
	return names, kinds, nil
}

// ParamKinds guesses the kind of each $n parameter from the column it is compared with or stored in.
func (s *Session) ParamKinds(st sql.Stmt, n int) []string {
	s.db.mu.Lock()
	defer s.db.mu.Unlock()
	out := make([]string, n)
	cols := map[string]string{}
	addTable := func(name string) {
		if tb := s.db.tables[name]; tb != nil {
			for _, c := range tb.Cols {
				if _, ok := cols[c.Name]; !ok {
					cols[c.Name] = c.Kind
				}
			}
		}
	}
	set := func(x sql.Expr, k string) {
		if p, ok := x.(*sql.Param); ok && p.N >= 1 && p.N <= n && out[p.N-1] == "" {
			out[p.N-1] = k
		}
	}
	scan := func(x sql.Expr) {
		walk(x, func(e sql.Expr) {
			switch y := e.(type) {
			case *sql.Binary:
				for _, pair := range [][2]sql.Expr{{y.L, y.R}, {y.R, y.L}} {
					if c, ok := pair[0].(*sql.Col); ok && cols[c.Name] != "" {
						set(pair[1], cols[c.Name])
					}
				}
			case *sql.Cast:
				set(y.X, kind(y.Type))
			case *sql.In:
				if c, ok := y.X.(*sql.Col); ok && cols[c.Name] != "" {
					for _, it := range y.List {
						set(it, cols[c.Name])
					}
				}
			}
		})
	}
	switch st := st.(type) {
	case *sql.Insert:
		addTable(st.Table)
		if tb := s.db.tables[st.Table]; tb != nil {
			names := st.Cols
			if names == nil {
				for _, c := range tb.Cols {
					names = append(names, c.Name)
				}
			}
			for _, row := range st.Rows {
				for i, x := range row {
					if i < len(names) {
						set(x, cols[names[i]])
					}
				}
			}
		}
		if st.Conflict != nil {
			for _, x := range st.Conflict.Sets {
				set(x.Expr, cols[x.Col])
			}
		}
	case *sql.Update:
		addTable(st.Table)
		for _, x := range st.Sets {
			set(x.Expr, cols[x.Col])
			scan(x.Expr)
		}
		scan(st.Where)
	case *sql.Delete:
		addTable(st.Table)
		scan(st.Where)
	case *sql.Select:
		for sel := st; sel != nil; sel = sel.Right {
			for _, tr := range sel.From {
				addTable(tr.Name)
			}
			scan(sel.Where)
			scan(sel.Having)
			for _, it := range sel.Items {
				scan(it.Expr)
			}
			set(sel.Limit, "int")
			set(sel.Offset, "int")
		}
	}
	for i := range out {
		if out[i] == "" {
			out[i] = "text"
		}
	}
	return out
}
