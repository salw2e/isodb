package db

import (
	"bytes"
	"fmt"
	"math/rand"
	"path/filepath"
	"sort"
	"testing"
)

// check compares the tree with the map it should hold.
func check(t *testing.T, p *pager, root pageID, want map[string][]byte) {
	t.Helper()
	var keys []string
	err := p.scan(root, func(k, v []byte) bool {
		keys = append(keys, string(k))
		if !bytes.Equal(want[string(k)], v) {
			t.Fatalf("key %q: got %d bytes, want %d", k, len(v), len(want[string(k)]))
		}
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	if !sort.StringsAreSorted(keys) {
		t.Fatal("scan out of order")
	}
	if len(keys) != len(want) {
		t.Fatalf("scan found %d keys, want %d", len(keys), len(want))
	}
	for k, v := range want {
		got, ok, err := p.get(root, []byte(k))
		if err != nil || !ok || !bytes.Equal(got, v) {
			t.Fatalf("get %q: ok=%v err=%v", k, ok, err)
		}
	}
}

func value(r *rand.Rand) []byte {
	n := r.Intn(60)
	if r.Intn(20) == 0 {
		n = 1000 + r.Intn(9000) // spills into overflow pages
	}
	v := make([]byte, n)
	r.Read(v)
	return v
}

func TestBTreeMatchesMap(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pages")
	p, err := openPager(path, 64)
	if err != nil {
		t.Fatal(err)
	}
	r := rand.New(rand.NewSource(1))
	want := map[string][]byte{}
	var root pageID
	for round := 0; round < 30; round++ {
		for i := 0; i < 500; i++ {
			k := []byte(fmt.Sprintf("key-%05d", r.Intn(3000)))
			if r.Intn(4) == 0 {
				if root, err = p.del(root, k); err != nil {
					t.Fatal(err)
				}
				delete(want, string(k))
			} else {
				v := value(r)
				if root, err = p.put(root, k, v); err != nil {
					t.Fatal(err)
				}
				want[string(k)] = v
			}
		}
		check(t, p, root, want)
		if err := p.commit(uint64(round), root); err != nil {
			t.Fatal(err)
		}
		p.f.Close()
		if p, err = openPager(path, 64); err != nil {
			t.Fatal(err)
		}
		if pageID(p.m.catalog) != root {
			t.Fatalf("reopened root %d, want %d", p.m.catalog, root)
		}
		check(t, p, root, want)
	}
}

func TestCrashBeforeMetaKeepsLastCheckpoint(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pages")
	p, _ := openPager(path, 16)
	want := map[string][]byte{}
	var root pageID
	var err error
	for i := 0; i < 2000; i++ {
		k := []byte(fmt.Sprintf("k%04d", i))
		want[string(k)] = []byte("v1")
		root, _ = p.put(root, k, []byte("v1"))
	}
	if err = p.commit(1, root); err != nil {
		t.Fatal(err)
	}
	// change everything and write the pages, but "crash" before the meta page
	newRoot := root
	for i := 0; i < 2000; i++ {
		newRoot, _ = p.put(newRoot, []byte(fmt.Sprintf("k%04d", i)), []byte("v2"))
	}
	for id, n := range p.dirty {
		p.f.WriteAt(p.encode(n), int64(id)*pageSize)
	}
	p.f.Sync()
	p.f.Close()
	if p, err = openPager(path, 16); err != nil {
		t.Fatal(err)
	}
	check(t, p, pageID(p.m.catalog), want)
}

func TestFreedPagesAreReused(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pages")
	p, _ := openPager(path, 32)
	var root pageID
	for i := 0; i < 3000; i++ {
		root, _ = p.put(root, []byte(fmt.Sprintf("k%05d", i)), make([]byte, 40))
	}
	p.commit(0, root)
	var sizes []uint64
	for round := 0; round < 40; round++ {
		for i := 0; i < 3000; i += 7 {
			root, _ = p.put(root, []byte(fmt.Sprintf("k%05d", i)), make([]byte, 40))
		}
		if err := p.commit(uint64(round), root); err != nil {
			t.Fatal(err)
		}
		sizes = append(sizes, p.m.pages)
	}
	if sizes[len(sizes)-1] > sizes[10]+sizes[10]/5 {
		t.Fatalf("file keeps growing: %d pages after 10 rounds, %d after 40", sizes[10], sizes[len(sizes)-1])
	}
}
