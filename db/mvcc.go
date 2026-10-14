package db

import (
	"sync"

	"isodb/sql"
)

const (
	ReadCommitted = iota
	RepeatableRead
	Serializable
)

const (
	active = iota
	committed
	aborted
	committing // its log record is queued but not yet on disk
)

type Column struct {
	Name    string
	Kind    string
	Type    string
	NotNull bool
	Serial  bool
	Default sql.Expr
}

type Table struct {
	Name    string
	Cols    []Column
	PK      []int
	Uniques [][]int
	rows    map[string]*Row
	order   []*Row
	nextID  int64
	readers map[*Txn]bool
	ddl     string
	root    pageID
}

type Row struct {
	tb      *Table
	key     string
	vers      []*Version
	readers   map[*Txn]bool
	disk      *Version // the version the page file holds
	transient bool     // met during a scan of the page file, not kept in memory
}

type Version struct {
	tx   *Txn
	vals []Value
	dead bool
}

type Txn struct {
	id         int64
	start      int64 // snapshot: sees commits with ts <= start
	commit     int64
	state      int
	iso        int
	in, out    bool
	explicit   bool
	failed     bool
	writes     []*Row
	written    map[*Row]bool
	readRows   []*Row
	readTables []*Table
	waitFor    *Txn
	lsn        int64
	serials    map[*Table]bool
	db         *DB
}

type DB struct {
	mu       sync.RWMutex // shared by plain reads outside SERIALIZABLE, exclusive for everything else
	txMu     sync.Mutex   // guards nextTx and active while statements share mu
	cond     *sync.Cond
	tables   map[string]*Table
	indexes  map[string]string // index name -> table
	indexText map[string]string
	views     map[string]*sql.Select
	viewText  map[string]string
	ts       int64 // every commit up to ts is durable and visible
	nextTS   int64 // last timestamp handed out
	pending  []*Txn
	nextTx   int64
	active   map[*Txn]bool
	retained []*Txn
	wal      *wal
	pg       *pager
	dropped  []pageID
	waiting  int
	ddl      []string
	recovering bool
}

func (t *Txn) visible(v *Version) bool {
	return v.tx == t || v.tx.state == committed && v.tx.commit <= t.start
}

// read returns the row's values as seen by t, or nil. Serializable readers record what they read and
// every newer version they had to skip (a rw-antidependency on its writer).
func (db *DB) read(t *Txn, tb *Table, r *Row) ([]Value, error) {
	var out []Value
	for i := len(r.vers) - 1; i >= 0; i-- {
		v := r.vers[i]
		if t.visible(v) {
			if !v.dead {
				out = v.vals
			}
			break
		}
		if t.iso == Serializable && v.tx.state != aborted {
			if err := db.rw(t, t, v.tx); err != nil {
				return nil, err
			}
		}
	}
	if t.iso == Serializable {
		if r.readers == nil {
			r.readers = map[*Txn]bool{}
		}
		if !r.readers[t] {
			r.readers[t] = true
			t.readRows = append(t.readRows, r)
		}
	}
	return out, nil
}

// scan marks a predicate read of the whole table: any later insert or update in it conflicts.
func (db *DB) scan(t *Txn, tb *Table) {
	if t.iso != Serializable {
		return
	}
	if tb.readers == nil {
		tb.readers = map[*Txn]bool{}
	}
	if !tb.readers[t] {
		tb.readers[t] = true
		t.readTables = append(t.readTables, tb)
	}
}

func concurrent(a, b *Txn) bool {
	if a.state == committed && a.commit <= b.start {
		return false
	}
	if b.state == committed && b.commit <= a.start {
		return false
	}
	return true
}

// rw records the rw-antidependency reader -> writer (reader saw a version older than writer's).
// The transaction that would complete a pivot (an in-edge and an out-edge) is not allowed to: the
// current transaction gets a serialization failure instead. This is the dangerous-structure test of
// Cahill et al., applied conservatively.
func (db *DB) rw(cur, reader, writer *Txn) error {
	if reader == writer || reader.iso != Serializable || writer.iso != Serializable {
		return nil
	}
	if reader.state == aborted || writer.state == aborted || !concurrent(reader, writer) {
		return nil
	}
	if reader.out && writer.in {
		return nil
	}
	if reader.in || writer.out {
		return errf(codeSerialization, "could not serialize access due to read/write dependencies among transactions")
	}
	reader.out = true
	writer.in = true
	return nil
}

// lock waits until no other transaction has an uncommitted version of r, then checks the
// write-write rule for snapshot transactions.
func (db *DB) lock(t *Txn, tb *Table, r *Row) error {
	for {
		if len(r.vers) == 0 {
			break
		}
		v := r.vers[len(r.vers)-1]
		w := v.tx
		if w == t || w.state == aborted {
			break
		}
		if w.state == active || w.state == committing {
			for x := w; x != nil; x = x.waitFor {
				if x == t {
					return errf(codeDeadlock, "deadlock detected")
				}
			}
			t.waitFor = w
			db.waiting++
			db.cond.Wait()
			db.waiting--
			t.waitFor = nil
			if t.state != active {
				return errf(codeSerialization, "transaction was aborted")
			}
			continue
		}
		if t.iso != ReadCommitted && w.commit > t.start {
			return errf(codeSerialization, "could not serialize access due to concurrent update")
		}
		break
	}
	if t.iso == Serializable {
		for rd := range r.readers {
			if err := db.rw(t, rd, t); err != nil {
				return err
			}
		}
		for rd := range tb.readers {
			if err := db.rw(t, rd, t); err != nil {
				return err
			}
		}
	}
	return nil
}

// latest is the newest committed or own version, used after lock for read committed updates.
func latest(t *Txn, r *Row) *Version {
	for i := len(r.vers) - 1; i >= 0; i-- {
		v := r.vers[i]
		if v.tx == t || v.tx.state == committed {
			return v
		}
	}
	return nil
}

// write installs a new version of r. lock must have succeeded first.
func (db *DB) write(t *Txn, tb *Table, r *Row, vals []Value, dead bool) {
	if n := len(r.vers); n > 0 && r.vers[n-1].tx == t {
		r.vers[n-1].vals, r.vers[n-1].dead = vals, dead
	} else {
		r.vers = append(r.vers, &Version{tx: t, vals: vals, dead: dead})
	}
	if !t.written[r] {
		if t.written == nil {
			t.written = map[*Row]bool{}
		}
		t.written[r] = true
		t.writes = append(t.writes, r)
	}
}

func (db *DB) begin(iso int, explicit bool) *Txn {
	db.txMu.Lock()
	defer db.txMu.Unlock()
	db.nextTx++
	t := &Txn{id: db.nextTx, start: db.ts, iso: iso, explicit: explicit, db: db}
	db.active[t] = true
	return t
}

// endRead ends a transaction that wrote nothing and holds no read locks, under a shared mu.
// Cleanup of other transactions waits for the next exclusive commit.
func (db *DB) endRead(t *Txn, state int) {
	db.txMu.Lock()
	defer db.txMu.Unlock()
	if t.state != active {
		return
	}
	if state == committed {
		t.commit = db.ts
	}
	t.state = state
	delete(db.active, t)
}

// commitTx makes t durable and visible. With a log, the record is queued for the log writer, which
// syncs many queued commits at once (group commit); the commit returns once the record is on disk
// and every earlier commit is too.
func (db *DB) commitTx(t *Txn) error {
	if t.state != active {
		return nil
	}
	if len(t.writes) == 0 && len(t.serials) == 0 {
		t.commit = db.ts
		db.finish(t)
		return nil
	}
	db.nextTS++
	t.commit = db.nextTS
	if db.wal == nil || db.recovering {
		db.ts = t.commit
		db.finish(t)
		return nil
	}
	t.lsn = db.wal.enqueue(db.wal.commitRecord(db, t))
	t.state = committing
	db.pending = append(db.pending, t)
	for t.state != committed {
		db.cond.Wait()
	}
	return nil
}

func (db *DB) finish(t *Txn) {
	t.state = committed
	delete(db.active, t)
	if len(t.readRows) > 0 || len(t.readTables) > 0 {
		db.retained = append(db.retained, t)
	}
	db.cleanup(t)
	db.cond.Broadcast()
}

// publish marks every queued commit whose record is on disk as committed, in timestamp order.
func (db *DB) publish(flushed int64) {
	i := 0
	for ; i < len(db.pending) && db.pending[i].lsn <= flushed; i++ {
		t := db.pending[i]
		db.ts = t.commit
		db.finish(t)
	}
	db.pending = db.pending[i:]
	db.cond.Broadcast()
}

func (db *DB) abortTx(t *Txn) {
	if t.state != active {
		return
	}
	t.state = aborted
	for _, r := range t.writes {
		if n := len(r.vers); n > 0 && r.vers[n-1].tx == t {
			r.vers = r.vers[:n-1]
		}
	}
	db.release(t)
	for _, r := range t.writes {
		db.trim(r)
	}
	delete(db.active, t)
	db.cleanup(nil)
	db.cond.Broadcast()
}

func (db *DB) release(t *Txn) {
	for _, r := range t.readRows {
		delete(r.readers, t)
		db.trim(r)
	}
	for _, tb := range t.readTables {
		delete(tb.readers, t)
	}
	t.readRows, t.readTables = nil, nil
}

func (db *DB) horizon() int64 {
	min := db.ts
	for a := range db.active {
		if a.start < min {
			min = a.start
		}
	}
	return min
}

// cleanup drops read locks nobody can conflict with any more and old row versions nobody can see.
func (db *DB) cleanup(t *Txn) {
	h := db.horizon()
	keep := db.retained[:0]
	for _, r := range db.retained {
		if r.commit <= h {
			db.release(r)
		} else {
			keep = append(keep, r)
		}
	}
	db.retained = keep
	if t == nil {
		return
	}
	for _, r := range t.writes {
		// keep the newest version every snapshot >= h can see, drop the older ones
		for i := len(r.vers) - 1; i > 0; i-- {
			v := r.vers[i]
			if v.tx.state == committed && v.tx.commit <= h {
				r.vers = r.vers[i:]
				break
			}
		}
		if db.pg == nil && len(r.vers) == 1 && r.vers[0].dead && r.vers[0].tx.state == committed && r.vers[0].tx.commit <= h && len(r.readers) == 0 {
			r.vers = nil
		}
	}
}
