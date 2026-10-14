package db

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"

	"isodb/sql"
)

// The write-ahead log is a sequence of records: [length u32][crc32c u32][payload].
// A commit is durable once its record is written and fsynced. On start the snapshot is loaded,
// then every log record newer than it is replayed; a torn record at the end is cut off.
// When the log passes maxLog bytes the whole committed state is written to a new snapshot.

const maxLog = 64 << 20

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

type wal struct {
	db    *DB
	dir   string
	f     *os.File
	size  int64
	fault float64
}

func openWAL(db *DB, dir string) (*wal, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	w := &wal{db: db, dir: dir}
	if s := os.Getenv("ISODB_FAULT"); s != "" {
		w.fault, _ = strconv.ParseFloat(s, 64)
	}
	db.recovering = true
	snapTS := int64(0)
	if b, err := os.ReadFile(filepath.Join(dir, "snapshot")); err == nil {
		if _, _, err := db.replay(b, -1); err != nil {
			return nil, fmt.Errorf("snapshot: %w", err)
		}
		snapTS = db.ts
	} else if !os.IsNotExist(err) {
		return nil, err
	}
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
		fmt.Fprintf(os.Stderr, "isodb: cut a torn record off the end of the log (%d of %d bytes kept)\n", good, len(b))
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
	w.f, w.size = f, good
	if records > 0 {
		if err := w.checkpoint(db); err != nil {
			return nil, err
		}
	}
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
				if dead {
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

func (w *wal) append(rec []byte) error {
	if w.fault > 0 && rand.Float64() < w.fault {
		// simulated power cut: only part of the record reaches the disk
		w.f.Write(rec[:rand.Intn(len(rec))])
		w.f.Sync()
		os.Exit(137)
	}
	if _, err := w.f.Write(rec); err != nil {
		return err
	}
	if err := w.f.Sync(); err != nil {
		return err
	}
	w.size += int64(len(rec))
	return nil
}

func (w *wal) ddl(db *DB, text string) error {
	db.ts++
	p := []byte{'D'}
	p = binary.LittleEndian.AppendUint64(p, uint64(db.ts))
	p = binary.AppendUvarint(p, uint64(len(text)))
	p = append(p, text...)
	return w.append(frame(p))
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

func (w *wal) commit(db *DB, t *Txn, ts int64) error {
	var rows []*Row
	for _, r := range t.writes {
		if db.tables[r.tb.Name] == r.tb {
			rows = append(rows, r)
		}
	}
	rec := commitRecord(ts, rows, t.serials, func(r *Row) *Version {
		v := r.vers[len(r.vers)-1]
		if v.tx != t {
			return nil
		}
		return v
	})
	if err := w.append(rec); err != nil {
		return err
	}
	if w.size > maxLog {
		return w.checkpoint(db)
	}
	return nil
}

// checkpoint writes every table and its committed rows to a new snapshot, then empties the log.
func (w *wal) checkpoint(db *DB) error {
	var buf []byte
	names := make([]string, 0, len(db.tables))
	for n := range db.tables {
		names = append(names, n)
	}
	sort.Strings(names)
	ddl := func(text string) {
		p := []byte{'D'}
		p = binary.LittleEndian.AppendUint64(p, 0)
		p = binary.AppendUvarint(p, uint64(len(text)))
		buf = append(buf, frame(append(p, text...))...)
	}
	for _, n := range names {
		ddl(db.tables[n].ddl)
	}
	for _, text := range db.indexDDL() {
		ddl(text)
	}
	for _, n := range names {
		tb := db.tables[n]
		var rows []*Row
		for _, r := range tb.order {
			if v := latestCommitted(r); v != nil && !v.dead {
				rows = append(rows, r)
			}
		}
		buf = append(buf, commitRecord(0, rows, map[*Table]bool{tb: true}, latestCommitted)...)
	}
	// the last record carries the snapshot's timestamp
	p := []byte{'C'}
	p = binary.LittleEndian.AppendUint64(p, uint64(db.ts))
	p = append(p, 0, 0)
	buf = append(buf, frame(p)...)

	tmp := filepath.Join(w.dir, "snapshot.tmp")
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(buf); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	f.Close()
	if err := os.Rename(tmp, filepath.Join(w.dir, "snapshot")); err != nil {
		return err
	}
	if err := syncDir(w.dir); err != nil {
		return err
	}
	if err := w.f.Truncate(0); err != nil {
		return err
	}
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	w.size = 0
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

func (w *wal) close() error {
	if err := w.checkpoint(w.db); err != nil {
		return err
	}
	return w.f.Close()
}
