package db

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"

	"isodb/sql"
)

// Committed rows live in one B+tree per table in the page file. Rows changed since the last
// checkpoint, or read under SERIALIZABLE, are also kept in memory with their version chains; the
// in-memory copy wins. A row that is only in the page file is visible to every transaction.

var baseTx = &Txn{state: committed}

const ddlKey = "\x00ddl"

func encodeRow(vals []Value) []byte {
	b := binary.AppendUvarint(nil, uint64(len(vals)))
	for _, v := range vals {
		b = putValue(b, v)
	}
	return b
}

func decodeRow(b []byte) []Value {
	r := &reader{b: b}
	n := r.uvarint()
	vals := make([]Value, 0, n)
	for i := uint64(0); i < n && !r.err; i++ {
		vals = append(vals, r.value())
	}
	if r.err {
		panic(errCorrupt)
	}
	return vals
}

func (db *DB) fromDisk(tb *Table, k string) (*Row, bool) {
	r := &Row{tb: tb, key: k}
	if db.pg == nil || tb.root == 0 {
		return r, false
	}
	v, ok, err := db.pg.get(tb.root, []byte(k))
	if err != nil {
		panic(err)
	}
	if ok {
		base := &Version{tx: baseTx, vals: decodeRow(v)}
		r.vers, r.disk = []*Version{base}, base
	}
	return r, ok
}

// row returns the in-memory row for k, bringing it in from the page file when it is only there.
func (db *DB) row(tb *Table, k string) *Row {
	if r := tb.rows[k]; r != nil {
		return r
	}
	r, _ := db.fromDisk(tb, k)
	tb.rows[k] = r
	tb.order = append(tb.order, r)
	return r
}

// own turns a row met during a scan into the in-memory row, before it is locked or written.
func (db *DB) own(tb *Table, r *Row) *Row {
	if r.transient {
		return db.row(tb, r.key)
	}
	return r
}

// each calls fn for every row of tb. Rows that are only in the page file come as temporary values.
func (db *DB) each(tb *Table, fn func(*Row) error) error {
	if db.pg != nil && tb.root != 0 {
		var ferr error
		err := db.pg.scan(tb.root, func(k, v []byte) bool {
			if _, ok := tb.rows[string(k)]; ok {
				return true
			}
			base := &Version{tx: baseTx, vals: decodeRow(v)}
			ferr = fn(&Row{tb: tb, key: string(k), vers: []*Version{base}, disk: base, transient: true})
			return ferr == nil
		})
		if ferr != nil {
			return ferr
		}
		if err != nil {
			return err
		}
	}
	for _, r := range tb.order {
		if tb.rows[r.key] != r || len(r.vers) == 0 {
			continue
		}
		if err := fn(r); err != nil {
			return err
		}
	}
	return nil
}

// trim forgets an in-memory row that holds nothing beyond what the page file has.
func (db *DB) trim(r *Row) {
	if db.pg == nil || r.tb == nil || len(r.readers) > 0 || db.waiting > 0 || r.tb.rows[r.key] != r {
		return
	}
	if len(r.vers) == 0 && r.disk == nil || len(r.vers) == 1 && r.vers[0] == r.disk {
		delete(r.tb.rows, r.key)
	}
}

func cacheSize() int {
	if n, err := strconv.Atoi(os.Getenv("ISODB_CACHE_PAGES")); err == nil && n > 0 {
		return n
	}
	return 8192 // 32 MB
}

// openPages loads the schema and table roots of the last checkpoint.
func (db *DB) openPages(dir string) error {
	pg, err := openPager(filepath.Join(dir, "pages"), cacheSize())
	if err != nil {
		return err
	}
	db.pg = pg
	cat := pageID(pg.m.catalog)
	if cat == 0 {
		return nil
	}
	v, ok, err := pg.get(cat, []byte(ddlKey))
	if err != nil {
		return err
	}
	if ok {
		s := db.NewSession("recovery")
		r := &reader{b: v}
		for len(r.b) > 0 && !r.err {
			text := r.str()
			stmts, texts, _, err := sql.Parse(text)
			if err != nil {
				return err
			}
			for i, st := range stmts {
				if _, err := s.ddl(st, texts[i]); err != nil {
					return fmt.Errorf("schema %q: %w", text, err)
				}
			}
		}
	}
	err = pg.scan(cat, func(k, v []byte) bool {
		if tb := db.tables[string(k)]; tb != nil && len(v) == 16 {
			tb.root = pageID(binary.LittleEndian.Uint64(v))
			tb.nextID = int64(binary.LittleEndian.Uint64(v[8:]))
		}
		return true
	})
	db.ts = int64(pg.m.ts)
	return err
}

// freeTree gives back every page of a dropped table's tree.
func (p *pager) freeTree(id pageID) error {
	n, err := p.load(id)
	if err != nil {
		return err
	}
	if !n.leaf {
		for _, k := range append([]pageID(nil), n.kids...) {
			if err := p.freeTree(k); err != nil {
				return err
			}
		}
	} else {
		for _, o := range n.ovf {
			if o != 0 {
				if err := p.releaseChain(o); err != nil {
					return err
				}
			}
		}
	}
	p.release(id)
	return nil
}

// flush writes every committed change into the page file and makes it the new checkpoint. Rows
// that every running transaction can already see in the page file leave memory.
func (db *DB) flush() error {
	pg := db.pg
	for _, root := range db.dropped {
		if err := pg.freeTree(root); err != nil {
			return err
		}
	}
	db.dropped = nil
	h := db.horizon()
	names := make([]string, 0, len(db.tables))
	for n := range db.tables {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		tb := db.tables[n]
		keep := tb.order[:0]
		for _, r := range tb.order {
			if tb.rows[r.key] != r {
				continue
			}
			v := latestCommitted(r)
			if v != nil && v != r.disk {
				var err error
				if v.dead {
					tb.root, err = pg.del(tb.root, []byte(r.key))
				} else {
					tb.root, err = pg.put(tb.root, []byte(r.key), encodeRow(v.vals))
				}
				if err != nil {
					return err
				}
				r.disk = v
			}
			if db.settled(r, h) {
				delete(tb.rows, r.key)
				continue
			}
			keep = append(keep, r)
		}
		tb.order = keep
	}

	cat := pageID(pg.m.catalog)
	var stale [][]byte
	if err := pg.scan(cat, func(k, v []byte) bool {
		if string(k) != ddlKey && db.tables[string(k)] == nil {
			stale = append(stale, append([]byte(nil), k...))
		}
		return true
	}); err != nil {
		return err
	}
	var err error
	for _, k := range stale {
		if cat, err = pg.del(cat, k); err != nil {
			return err
		}
	}
	for _, n := range names {
		tb := db.tables[n]
		v := binary.LittleEndian.AppendUint64(nil, uint64(tb.root))
		v = binary.LittleEndian.AppendUint64(v, uint64(tb.nextID))
		if cat, err = pg.put(cat, []byte(n), v); err != nil {
			return err
		}
	}
	var schema []byte
	add := func(text string) {
		schema = binary.AppendUvarint(schema, uint64(len(text)))
		schema = append(schema, text...)
	}
	for _, n := range names {
		add(db.tables[n].ddl)
	}
	for _, t := range db.indexDDL() {
		add(t)
	}
	if cat, err = pg.put(cat, []byte(ddlKey), schema); err != nil {
		return err
	}
	return pg.commit(uint64(db.ts), cat)
}

// settled reports a row whose newest committed version is in the page file and visible to every
// running transaction, with nothing uncommitted and nobody's read lock on it.
func (db *DB) settled(r *Row, h int64) bool {
	if len(r.readers) > 0 || db.waiting > 0 {
		return false
	}
	if len(r.vers) == 0 {
		return true
	}
	newest := r.vers[len(r.vers)-1]
	return newest == r.disk && newest.tx.commit <= h
}
