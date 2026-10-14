package db

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"isodb/sql"
)

// The write-ahead log is a sequence of records: [length u32][crc32c u32][payload].
// A commit is durable once its record is written and fsynced. On start the last checkpoint is
// opened, then every log record newer than it is replayed; a torn record at the end is cut off.
// When the log passes maxLog bytes the committed state is written to the page file (a checkpoint).

var maxLog = func() int64 {
	if n, err := strconv.ParseInt(os.Getenv("ISODB_MAXLOG"), 10, 64); err == nil && n > 0 {
		return n
	}
	return 64 << 20
}()

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

type wal struct {
	db    *DB
	dir   string
	f     *os.File
	size  int64
	alloc int64 // bytes of the file zero-filled so far
	fault float64

	mu      sync.Mutex
	cond    *sync.Cond
	buf     []byte // records waiting for the log writer
	queued  int64  // bytes ever queued
	flushed int64  // bytes ever written and synced
	stop    bool
	done    chan struct{}
}

func openWAL(db *DB, dir string) (*wal, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	w := &wal{db: db, dir: dir, done: make(chan struct{})}
	w.cond = sync.NewCond(&w.mu)
	if s := os.Getenv("ISODB_FAULT"); s != "" {
		w.fault, _ = strconv.ParseFloat(s, 64)
	}
	db.recovering = true
	if err := db.openPages(dir); err != nil {
		return nil, fmt.Errorf("page file: %w", err)
	}
	snapTS := db.ts
	path := filepath.Join(dir, "wal")
	b, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	good, records, err := db.replay(b, snapTS)
	if err != nil {
		return nil, err
	}
	db.recovering = false
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	if int64(len(b)) != good {
		if !zero(b[good:]) {
			fmt.Fprintf(os.Stderr, "isodb: cut a torn record off the end of the log (%d of %d bytes kept)\n", good, len(b))
		}
		if err := f.Truncate(good); err != nil {
			return nil, err
		}
		if err := f.Sync(); err != nil {
			return nil, err
		}
	}
	if _, err := f.Seek(good, io.SeekStart); err != nil {
		return nil, err
	}
	w.f, w.size, w.alloc = f, good, good
	if records > 0 {
		if err := w.checkpoint(db); err != nil {
			return nil, err
		}
	}
	db.nextTS = db.ts
	go w.writer()
	return w, nil
}

// replay applies records with a timestamp above after; it returns how many bytes were valid.
func (db *DB) replay(b []byte, after int64) (int64, int, error) {
	off, n := int64(0), 0
	s := db.NewSession("recovery")
	for {
		if int64(len(b))-off < 8 {
			return off, n, nil
		}
		ln := int64(binary.LittleEndian.Uint32(b[off:]))
		sum := binary.LittleEndian.Uint32(b[off+4:])
		if int64(len(b))-off-8 < ln {
			return off, n, nil
		}
		p := b[off+8 : off+8+ln]
		if crc32.Checksum(p, castagnoli) != sum {
			return off, n, nil
		}
		r := &reader{b: p}
		typ := r.byte()
		ts := int64(r.u64())
		if r.err {
			return off, n, nil
		}
		if ts > after {
			if err := db.apply(s, typ, ts, r); err != nil {
				return 0, 0, err
			}
			n++
		}
		off += 8 + ln
	}
}

func (db *DB) apply(s *Session, typ byte, ts int64, r *reader) error {
	switch typ {
	case 'D':
		text := r.str()
		stmts, texts, _, err := sql.Parse(text)
		if err != nil {
			return err
		}
		for i, st := range stmts {
			if _, err := s.ddl(st, texts[i]); err != nil {
				return fmt.Errorf("replaying %q: %w", text, err)
			}
		}
	case 'C':
		tx := &Txn{state: committed, commit: ts}
		rows := r.uvarint()
		for i := uint64(0); i < rows && !r.err; i++ {
			name, k := r.str(), r.str()
			dead := r.byte() == 1
			nv := r.uvarint()
			var vals []Value
			for j := uint64(0); j < nv && !r.err; j++ {
				vals = append(vals, r.value())
			}
			if tb := db.tables[name]; tb != nil {
				row := db.row(tb, k)
				row.vers = []*Version{{tx: tx, vals: vals, dead: dead}}
				if dead && db.pg == nil {
					// with a page file the deletion has to stay until a checkpoint removes the row there
					row.vers = nil
				}
			}
		}
		counters := r.uvarint()
		for i := uint64(0); i < counters && !r.err; i++ {
			name, v := r.str(), int64(r.u64())
			if tb := db.tables[name]; tb != nil && v > tb.nextID {
				tb.nextID = v
			}
		}
		if r.err {
			return fmt.Errorf("corrupt commit record")
		}
	default:
		return fmt.Errorf("unknown log record %q", typ)
	}
	if ts > db.ts {
		db.ts = ts
	}
	return nil
}

func frame(p []byte) []byte {
	out := make([]byte, 8, 8+len(p))
	binary.LittleEndian.PutUint32(out, uint32(len(p)))
	binary.LittleEndian.PutUint32(out[4:], crc32.Checksum(p, castagnoli))
	return append(out, p...)
}

// enqueue adds a record for the log writer and returns the log position just after it.
func (w *wal) enqueue(rec []byte) int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.buf = append(w.buf, rec...)
	w.queued += int64(len(rec))
	w.cond.Signal()
	return w.queued
}

// writer writes and syncs whatever is queued, then publishes the commits it made durable.
// Commits that arrive during a sync share the next one.
func (w *wal) writer() {
	defer close(w.done)
	for {
		w.mu.Lock()
		for len(w.buf) == 0 && !w.stop {
			w.cond.Wait()
		}
		if len(w.buf) == 0 {
			w.mu.Unlock()
			return
		}
		b, end := w.buf, w.queued
		w.buf = nil
		w.mu.Unlock()

		if w.fault > 0 && rand.Float64() < w.fault {
			// simulated power cut: only part of the batch reaches the disk
			w.f.WriteAt(b[:rand.Intn(len(b))], w.size)
			w.f.Sync()
			os.Exit(137)
		}
		if err := w.append(b); err != nil {
			fatal(err)
		}
		db := w.db
		db.mu.Lock()
		w.mu.Lock()
		w.flushed = end
		w.size += int64(len(b))
		w.mu.Unlock()
		db.publish(end)
		if w.size > maxLog {
			if err := w.checkpoint(db); err != nil {
				fatal(err)
			}
		}
		db.mu.Unlock()
	}
}

const walChunk = 1 << 20

// append writes b after the last record. The log is zero-filled a chunk ahead, so a commit only
// syncs data (fdatasync), not a changed file size; that is about 3x faster on this machine's disk.
func (w *wal) append(b []byte) error {
	if end := w.size + int64(len(b)); end > w.alloc {
		n := (end - w.alloc + walChunk - 1) / walChunk * walChunk
		if _, err := w.f.WriteAt(make([]byte, n), w.alloc); err != nil {
			return err
		}
		if err := w.f.Sync(); err != nil {
			return err
		}
		w.alloc += n
	}
	if _, err := w.f.WriteAt(b, w.size); err != nil {
		return err
	}
	return datasync(w.f)
}

func zero(b []byte) bool {
	for _, c := range b {
		if c != 0 {
			return false
		}
	}
	return true
}

// a log write that fails leaves the log in an unknown state, so stop like Postgres does
func fatal(err error) {
	fmt.Fprintln(os.Stderr, "isodb: log write failed:", err)
	os.Exit(1)
}

func (w *wal) ddl(db *DB, text string) error {
	db.nextTS++
	t := &Txn{commit: db.nextTS, state: committing}
	p := []byte{'D'}
	p = binary.LittleEndian.AppendUint64(p, uint64(t.commit))
	p = binary.AppendUvarint(p, uint64(len(text)))
	p = append(p, text...)
	t.lsn = w.enqueue(frame(p))
	db.pending = append(db.pending, t)
	for t.state != committed {
		db.cond.Wait()
	}
	return nil
}

func commitRecord(ts int64, rows []*Row, counters map[*Table]bool, val func(*Row) *Version) []byte {
	p := []byte{'C'}
	p = binary.LittleEndian.AppendUint64(p, uint64(ts))
	p = binary.AppendUvarint(p, uint64(len(rows)))
	for _, r := range rows {
		v := val(r)
		tb := r.tb
		p = binary.AppendUvarint(p, uint64(len(tb.Name)))
		p = append(p, tb.Name...)
		p = binary.AppendUvarint(p, uint64(len(r.key)))
		p = append(p, r.key...)
		if v == nil || v.dead {
			p = append(p, 1, 0)
			continue
		}
		p = append(p, 0)
		p = binary.AppendUvarint(p, uint64(len(v.vals)))
		for _, x := range v.vals {
			p = putValue(p, x)
		}
	}
	names := make([]*Table, 0, len(counters))
	for tb := range counters {
		names = append(names, tb)
	}
	p = binary.AppendUvarint(p, uint64(len(names)))
	for _, tb := range names {
		p = binary.AppendUvarint(p, uint64(len(tb.Name)))
		p = append(p, tb.Name...)
		p = binary.LittleEndian.AppendUint64(p, uint64(tb.nextID))
	}
	return frame(p)
}

func (w *wal) commitRecord(db *DB, t *Txn) []byte {
	var rows []*Row
	for _, r := range t.writes {
		if db.tables[r.tb.Name] == r.tb {
			rows = append(rows, r)
		}
	}
	return commitRecord(t.commit, rows, t.serials, func(r *Row) *Version {
		v := r.vers[len(r.vers)-1]
		if v.tx != t {
			return nil
		}
		return v
	})
}

// checkpoint writes the committed state to the page file, then empties the log.
func (w *wal) checkpoint(db *DB) error {
	if err := db.flush(); err != nil {
		return err
	}
	if err := w.f.Truncate(0); err != nil {
		return err
	}
	w.size, w.alloc = 0, 0
	return w.f.Sync()
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// close lets the writer finish what is queued, then writes a final snapshot.
func (w *wal) close() error {
	w.mu.Lock()
	w.stop = true
	w.cond.Signal()
	w.mu.Unlock()
	<-w.done
	w.db.mu.Lock()
	defer w.db.mu.Unlock()
	if err := w.checkpoint(w.db); err != nil {
		return err
	}
	return w.f.Close()
}
