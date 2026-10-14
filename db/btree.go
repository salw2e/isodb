package db

import (
	"bytes"
	"container/list"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"sort"
	"sync"
)

// The page file holds copy-on-write B+trees. Pages are never changed in place once a checkpoint has
// referenced them: a checkpoint writes new copies of every page it changes, syncs them, then writes
// one of the two meta pages. A crash before the meta write leaves the previous checkpoint intact.

const (
	pageSize  = 4096
	maxKey    = 1024
	inlineMax = 1024
	metaMagic = 0x6973_6f64_6270_6731 // "isodbpg1"
)

type pageID uint64

type node struct {
	id    pageID
	leaf  bool
	keys  [][]byte
	vals  [][]byte   // leaf values
	ovf   []pageID   // leaf: first overflow page of a large value, or 0
	kids  []pageID   // internal: kids[i] holds keys below keys[i]; kids[len(keys)] the rest
	dirty bool       // written by the running checkpoint, so it may change in place
	elem  *list.Element
}

type meta struct {
	seq, ts, catalog, free, pages uint64
}

type pager struct {
	f     *os.File
	m     meta
	mu    sync.Mutex
	cache map[pageID]*node
	lru   *list.List
	limit int
	free  []pageID // pages no checkpoint on disk refers to
	freed []pageID // pages the running checkpoint stopped using; reusable after it
	dirty map[pageID]*node
	reads int64
}

var errCorrupt = errors.New("page file is corrupt")

func openPager(path string, cachePages int) (*pager, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	p := &pager{f: f, cache: map[pageID]*node{}, lru: list.New(), limit: cachePages, dirty: map[pageID]*node{}}
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if st.Size() == 0 {
		p.m = meta{pages: 2}
		return p, nil
	}
	var best *meta
	for slot := 0; slot < 2; slot++ {
		b := make([]byte, pageSize)
		if _, err := f.ReadAt(b, int64(slot)*pageSize); err != nil {
			continue
		}
		if m, ok := decodeMeta(b); ok && (best == nil || m.seq > best.seq) {
			mm := m
			best = &mm
		}
	}
	if best == nil {
		return nil, errCorrupt
	}
	p.m = *best
	if p.free, err = p.readList(pageID(p.m.free)); err != nil {
		return nil, err
	}
	return p, nil
}

func decodeMeta(b []byte) (meta, bool) {
	if binary.LittleEndian.Uint64(b) != metaMagic {
		return meta{}, false
	}
	if crc32.Checksum(b[:48], castagnoli) != binary.LittleEndian.Uint32(b[48:]) {
		return meta{}, false
	}
	u := func(i int) uint64 { return binary.LittleEndian.Uint64(b[8+8*i:]) }
	return meta{seq: u(0), ts: u(1), catalog: u(2), free: u(3), pages: u(4)}, true
}

func (p *pager) alloc() pageID {
	if n := len(p.free); n > 0 {
		id := p.free[n-1]
		p.free = p.free[:n-1]
		return id
	}
	id := pageID(p.m.pages)
	p.m.pages++
	return id
}

func (p *pager) release(id pageID) {
	if id == 0 {
		return
	}
	if n := p.dirty[id]; n != nil {
		// a page this checkpoint created and no longer needs can be reused at once
		delete(p.dirty, id)
		p.drop(n)
		p.free = append(p.free, id)
		return
	}
	if n := p.cache[id]; n != nil {
		p.drop(n)
	}
	p.freed = append(p.freed, id)
}

func (p *pager) drop(n *node) {
	delete(p.cache, n.id)
	if n.elem != nil {
		p.lru.Remove(n.elem)
		n.elem = nil
	}
}

// load may run from several reading statements at once, so the cache is guarded by p.mu.
func (p *pager) load(id pageID) (*node, error) {
	p.mu.Lock()
	if n := p.cache[id]; n != nil {
		if n.elem != nil {
			p.lru.MoveToFront(n.elem)
		}
		p.mu.Unlock()
		return n, nil
	}
	p.mu.Unlock()
	b := make([]byte, pageSize)
	if _, err := p.f.ReadAt(b, int64(id)*pageSize); err != nil {
		return nil, err
	}
	n, err := p.decode(id, b)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.reads++
	if c := p.cache[id]; c != nil {
		return c, nil
	}
	p.cache[id] = n
	n.elem = p.lru.PushFront(n)
	for p.lru.Len() > p.limit {
		old := p.lru.Back().Value.(*node)
		p.drop(old)
	}
	return n, nil
}

func (p *pager) decode(id pageID, b []byte) (*node, error) {
	n := &node{id: id, leaf: b[0] == 1}
	if b[0] != 1 && b[0] != 2 {
		return nil, errCorrupt
	}
	cnt := int(binary.LittleEndian.Uint16(b[1:]))
	at := 3
	if !n.leaf {
		n.kids = append(n.kids, pageID(binary.LittleEndian.Uint64(b[at:])))
		at += 8
	}
	for i := 0; i < cnt; i++ {
		kl := int(binary.LittleEndian.Uint16(b[at:]))
		n.keys = append(n.keys, append([]byte(nil), b[at+2:at+2+kl]...))
		at += 2 + kl
		if !n.leaf {
			n.kids = append(n.kids, pageID(binary.LittleEndian.Uint64(b[at:])))
			at += 8
			continue
		}
		if b[at] == 0 {
			vl := int(binary.LittleEndian.Uint16(b[at+1:]))
			n.vals = append(n.vals, append([]byte(nil), b[at+3:at+3+vl]...))
			n.ovf = append(n.ovf, 0)
			at += 3 + vl
			continue
		}
		vl := int(binary.LittleEndian.Uint32(b[at+1:]))
		first := pageID(binary.LittleEndian.Uint64(b[at+5:]))
		at += 13
		v, err := p.readOverflow(first, vl)
		if err != nil {
			return nil, err
		}
		n.vals = append(n.vals, v)
		n.ovf = append(n.ovf, first)
	}
	return n, nil
}

func entrySize(n *node, i int) int {
	if !n.leaf {
		return 2 + len(n.keys[i]) + 8
	}
	if len(n.vals[i]) > inlineMax {
		return 2 + len(n.keys[i]) + 13
	}
	return 2 + len(n.keys[i]) + 3 + len(n.vals[i])
}

func size(n *node) int {
	s := 3
	if !n.leaf {
		s += 8
	}
	for i := range n.keys {
		s += entrySize(n, i)
	}
	return s
}

func (p *pager) encode(n *node) []byte {
	b := make([]byte, pageSize)
	if n.leaf {
		b[0] = 1
	} else {
		b[0] = 2
	}
	binary.LittleEndian.PutUint16(b[1:], uint16(len(n.keys)))
	at := 3
	if !n.leaf {
		binary.LittleEndian.PutUint64(b[at:], uint64(n.kids[0]))
		at += 8
	}
	for i, k := range n.keys {
		binary.LittleEndian.PutUint16(b[at:], uint16(len(k)))
		at += 2 + copy(b[at+2:], k)
		if !n.leaf {
			binary.LittleEndian.PutUint64(b[at:], uint64(n.kids[i+1]))
			at += 8
			continue
		}
		if n.ovf[i] == 0 {
			binary.LittleEndian.PutUint16(b[at+1:], uint16(len(n.vals[i])))
			at += 3 + copy(b[at+3:], n.vals[i])
			continue
		}
		b[at] = 1
		binary.LittleEndian.PutUint32(b[at+1:], uint32(len(n.vals[i])))
		binary.LittleEndian.PutUint64(b[at+5:], uint64(n.ovf[i]))
		at += 13
	}
	return b
}

// overflow chains hold values too big for a leaf: [next u64][len u16][data]

func (p *pager) writeOverflow(v []byte) (pageID, error) {
	var first, prev pageID
	var prevBuf []byte
	for off := 0; off < len(v); {
		id := p.alloc()
		n := min(len(v)-off, pageSize-10)
		b := make([]byte, pageSize)
		binary.LittleEndian.PutUint16(b[8:], uint16(n))
		copy(b[10:], v[off:off+n])
		off += n
		if prev != 0 {
			binary.LittleEndian.PutUint64(prevBuf, uint64(id))
			if _, err := p.f.WriteAt(prevBuf, int64(prev)*pageSize); err != nil {
				return 0, err
			}
		} else {
			first = id
		}
		prev, prevBuf = id, b
	}
	if prev != 0 {
		if _, err := p.f.WriteAt(prevBuf, int64(prev)*pageSize); err != nil {
			return 0, err
		}
	}
	return first, nil
}

func (p *pager) chain(first pageID, fn func(b []byte)) error {
	for id := first; id != 0; {
		b := make([]byte, pageSize)
		if _, err := p.f.ReadAt(b, int64(id)*pageSize); err != nil {
			return err
		}
		fn(b)
		id = pageID(binary.LittleEndian.Uint64(b))
	}
	return nil
}

func (p *pager) readOverflow(first pageID, n int) ([]byte, error) {
	out := make([]byte, 0, n)
	err := p.chain(first, func(b []byte) {
		l := int(binary.LittleEndian.Uint16(b[8:]))
		out = append(out, b[10:10+l]...)
	})
	if len(out) != n {
		return nil, errCorrupt
	}
	return out, err
}

func (p *pager) chainIDs(first pageID) ([]pageID, error) {
	var ids []pageID
	b := make([]byte, 8)
	for id := first; id != 0; id = pageID(binary.LittleEndian.Uint64(b)) {
		ids = append(ids, id)
		if _, err := p.f.ReadAt(b, int64(id)*pageSize); err != nil {
			return nil, err
		}
	}
	return ids, nil
}

func (p *pager) releaseChain(first pageID) error {
	ids, err := p.chainIDs(first)
	for _, id := range ids {
		p.release(id)
	}
	return err
}

// a list of page ids stored as an overflow chain of little-endian u64s
func (p *pager) readList(first pageID) ([]pageID, error) {
	if first == 0 {
		return nil, nil
	}
	var b []byte
	err := p.chain(first, func(pg []byte) {
		l := int(binary.LittleEndian.Uint16(pg[8:]))
		b = append(b, pg[10:10+l]...)
	})
	var out []pageID
	for i := 0; i+8 <= len(b); i += 8 {
		out = append(out, pageID(binary.LittleEndian.Uint64(b[i:])))
	}
	return out, err
}

// writable returns a node the running checkpoint may change: the node itself if it already made
// it, otherwise a copy on a new page.
func (p *pager) writable(n *node) *node {
	if n.dirty {
		return n
	}
	c := &node{id: p.alloc(), leaf: n.leaf, dirty: true}
	c.keys = append([][]byte(nil), n.keys...)
	c.vals = append([][]byte(nil), n.vals...)
	c.ovf = append([]pageID(nil), n.ovf...)
	c.kids = append([]pageID(nil), n.kids...)
	p.release(n.id)
	p.dirty[c.id] = c
	p.cache[c.id] = c
	return c
}

func (p *pager) fresh(leaf bool) *node {
	n := &node{id: p.alloc(), leaf: leaf, dirty: true}
	p.dirty[n.id] = n
	p.cache[n.id] = n
	return n
}

func search(n *node, k []byte) int {
	return sort.Search(len(n.keys), func(i int) bool { return bytes.Compare(n.keys[i], k) > 0 })
}

// get returns the value stored under k in the tree rooted at root.
func (p *pager) get(root pageID, k []byte) ([]byte, bool, error) {
	if root == 0 {
		return nil, false, nil
	}
	n, err := p.load(root)
	if err != nil {
		return nil, false, err
	}
	for !n.leaf {
		if n, err = p.load(n.kids[search(n, k)]); err != nil {
			return nil, false, err
		}
	}
	i := sort.Search(len(n.keys), func(i int) bool { return bytes.Compare(n.keys[i], k) >= 0 })
	if i < len(n.keys) && bytes.Equal(n.keys[i], k) {
		return n.vals[i], true, nil
	}
	return nil, false, nil
}

// scan calls fn for every key in order until it returns false.
func (p *pager) scan(root pageID, fn func(k, v []byte) bool) error {
	if root == 0 {
		return nil
	}
	_, err := p.walk(root, fn)
	return err
}

func (p *pager) walk(id pageID, fn func(k, v []byte) bool) (bool, error) {
	n, err := p.load(id)
	if err != nil {
		return false, err
	}
	if n.leaf {
		keys, vals := n.keys, n.vals
		for i := range keys {
			if !fn(keys[i], vals[i]) {
				return false, nil
			}
		}
		return true, nil
	}
	for _, kid := range append([]pageID(nil), n.kids...) {
		if ok, err := p.walk(kid, fn); err != nil || !ok {
			return ok, err
		}
	}
	return true, nil
}

type piece struct {
	sep []byte
	id  pageID
}

// put sets k to v in the tree and returns the new root.
func (p *pager) put(root pageID, k, v []byte) (pageID, error) {
	if root == 0 {
		root = p.fresh(true).id
	}
	parts, err := p.putIn(root, k, v)
	if err != nil {
		return 0, err
	}
	return p.joinRoot(parts), nil
}

func (p *pager) joinRoot(parts []piece) pageID {
	if len(parts) == 1 {
		return parts[0].id
	}
	r := p.fresh(false)
	r.kids = append(r.kids, parts[0].id)
	for _, x := range parts[1:] {
		r.keys = append(r.keys, x.sep)
		r.kids = append(r.kids, x.id)
	}
	return p.splitIfFull(r, false)[0].id
}

func (p *pager) putIn(id pageID, k, v []byte) ([]piece, error) {
	n0, err := p.load(id)
	if err != nil {
		return nil, err
	}
	n := p.writable(n0)
	if n.leaf {
		i := sort.Search(len(n.keys), func(i int) bool { return bytes.Compare(n.keys[i], k) >= 0 })
		var o pageID
		if len(v) > inlineMax {
			if o, err = p.writeOverflow(v); err != nil {
				return nil, err
			}
		}
		if i < len(n.keys) && bytes.Equal(n.keys[i], k) {
			if n.ovf[i] != 0 {
				if err := p.releaseChain(n.ovf[i]); err != nil {
					return nil, err
				}
			}
			n.vals[i], n.ovf[i] = v, o
			return p.splitIfFull(n, false), nil
		}
		n.keys = insertAt(n.keys, i, append([]byte(nil), k...))
		n.vals = insertAt(n.vals, i, v)
		n.ovf = insertAt(n.ovf, i, o)
		return p.splitIfFull(n, i == len(n.keys)-1), nil
	}
	ci := search(n, k)
	parts, err := p.putIn(n.kids[ci], k, v)
	if err != nil {
		return nil, err
	}
	p.replaceKid(n, ci, parts)
	return p.splitIfFull(n, ci == len(n.kids)-len(parts) && len(parts) > 1), nil
}

func (p *pager) replaceKid(n *node, ci int, parts []piece) {
	kids := append(append([]pageID(nil), n.kids[:ci]...), parts[0].id)
	keys := append([][]byte(nil), n.keys[:ci]...)
	for _, x := range parts[1:] {
		keys = append(keys, x.sep)
		kids = append(kids, x.id)
	}
	n.keys = append(keys, n.keys[ci:]...)
	n.kids = append(kids, n.kids[ci+1:]...)
}

func insertAt[T any](s []T, i int, v T) []T {
	var zero T
	s = append(s, zero)
	copy(s[i+1:], s[i:])
	s[i] = v
	return s
}

// splitIfFull splits a node that no longer fits a page into pieces that do. When the overflow came
// from appending at the end (keys inserted in order), the old page stays full and only the last
// entry moves to the new one, so a table filled in key order packs its pages instead of half-filling them.
func (p *pager) splitIfFull(n *node, tail bool) []piece {
	if size(n) <= pageSize {
		return []piece{{nil, n.id}}
	}
	half := size(n) / 2
	acc, at := 3, 0
	for at < len(n.keys)-1 && acc < half {
		acc += entrySize(n, at)
		at++
	}
	if tail && len(n.keys) > 1 {
		at = len(n.keys) - 1
	}
	if at == 0 {
		at = 1
	}
	r := p.fresh(n.leaf)
	var sep []byte
	if n.leaf {
		sep = n.keys[at]
		r.keys = append(r.keys, n.keys[at:]...)
		r.vals = append(r.vals, n.vals[at:]...)
		r.ovf = append(r.ovf, n.ovf[at:]...)
		n.keys, n.vals, n.ovf = n.keys[:at:at], n.vals[:at:at], n.ovf[:at:at]
	} else {
		sep = n.keys[at]
		r.keys = append(r.keys, n.keys[at+1:]...)
		r.kids = append(r.kids, n.kids[at+1:]...)
		n.keys, n.kids = n.keys[:at:at], n.kids[:at+1:at+1]
	}
	out := p.splitIfFull(n, false)
	rest := p.splitIfFull(r, false)
	rest[0].sep = sep
	return append(out, rest...)
}

// del removes k from the tree and returns the new root (0 when the tree is empty).
func (p *pager) del(root pageID, k []byte) (pageID, error) {
	if root == 0 {
		return 0, nil
	}
	id, err := p.delIn(root, k)
	if err != nil || id == 0 {
		return 0, err
	}
	for {
		n, err := p.load(id)
		if err != nil {
			return 0, err
		}
		if n.leaf || len(n.kids) > 1 {
			return id, nil
		}
		// a root with one child is just that child
		id = n.kids[0]
		p.release(n.id)
	}
}

func (p *pager) delIn(id pageID, k []byte) (pageID, error) {
	n0, err := p.load(id)
	if err != nil {
		return 0, err
	}
	if n0.leaf {
		i := sort.Search(len(n0.keys), func(i int) bool { return bytes.Compare(n0.keys[i], k) >= 0 })
		if i == len(n0.keys) || !bytes.Equal(n0.keys[i], k) {
			return id, nil
		}
		n := p.writable(n0)
		if n.ovf[i] != 0 {
			if err := p.releaseChain(n.ovf[i]); err != nil {
				return 0, err
			}
		}
		n.keys = append(n.keys[:i:i], n.keys[i+1:]...)
		n.vals = append(n.vals[:i:i], n.vals[i+1:]...)
		n.ovf = append(n.ovf[:i:i], n.ovf[i+1:]...)
		if len(n.keys) == 0 {
			p.release(n.id)
			return 0, nil
		}
		return n.id, nil
	}
	ci := search(n0, k)
	kid, err := p.delIn(n0.kids[ci], k)
	if err != nil {
		return 0, err
	}
	if kid == n0.kids[ci] {
		return id, nil
	}
	n := p.writable(n0)
	if kid != 0 {
		n.kids[ci] = kid
		return n.id, nil
	}
	// the child became empty: drop it and the separator next to it
	if len(n.kids) == 1 {
		p.release(n.id)
		return 0, nil
	}
	if ci == 0 {
		n.keys, n.kids = n.keys[1:], n.kids[1:]
	} else {
		n.keys = append(n.keys[:ci-1:ci-1], n.keys[ci:]...)
		n.kids = append(n.kids[:ci:ci], n.kids[ci+1:]...)
	}
	return n.id, nil
}

// commit writes every page the checkpoint made, syncs, then switches to the new meta page.
func (p *pager) commit(ts uint64, catalog pageID) error {
	ids := make([]pageID, 0, len(p.dirty))
	for id := range p.dirty {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		if _, err := p.f.WriteAt(p.encode(p.dirty[id]), int64(id)*pageSize); err != nil {
			return err
		}
	}
	// the free list for the next checkpoint: what is free now plus what this one released
	listIDs, err := p.chainIDs(pageID(p.m.free))
	if err != nil {
		return err
	}
	// the list itself goes on pages that are free now; pages the previous checkpoint still uses
	// (freed and the old list) must not be overwritten before the new meta page is written
	total := len(p.free) + len(p.freed) + len(listIDs)
	need := (total*8)/(pageSize-10) + 1
	if need > len(p.free) {
		need = len(p.free)
	}
	reserved := p.free[len(p.free)-need:]
	next := append(append(append([]pageID(nil), p.free[:len(p.free)-need]...), p.freed...), listIDs...)
	var lb []byte
	for _, id := range next {
		lb = binary.LittleEndian.AppendUint64(lb, uint64(id))
	}
	p.free = append([]pageID(nil), reserved...)
	head := pageID(0)
	if len(lb) > 0 {
		var err error
		if head, err = p.writeOverflow(lb); err != nil {
			return err
		}
	}
	if err := p.f.Sync(); err != nil {
		return err
	}
	m := meta{seq: p.m.seq + 1, ts: ts, catalog: uint64(catalog), free: uint64(head), pages: p.m.pages}
	b := make([]byte, pageSize)
	binary.LittleEndian.PutUint64(b, metaMagic)
	for i, v := range []uint64{m.seq, m.ts, m.catalog, m.free, m.pages} {
		binary.LittleEndian.PutUint64(b[8+8*i:], v)
	}
	binary.LittleEndian.PutUint32(b[48:], crc32.Checksum(b[:48], castagnoli))
	if _, err := p.f.WriteAt(b, int64(m.seq%2)*pageSize); err != nil {
		return err
	}
	if err := p.f.Sync(); err != nil {
		return err
	}
	p.m = m
	p.free = append(next, p.free...) // reserved pages the list did not need
	p.freed = nil
	for id, n := range p.dirty {
		n.dirty = false
		if n.elem == nil {
			n.elem = p.lru.PushFront(n)
		}
		delete(p.dirty, id)
	}
	for p.lru.Len() > p.limit {
		p.drop(p.lru.Back().Value.(*node))
	}
	return nil
}
