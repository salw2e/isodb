package db

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"isodb/sql"
)

func run(s *Session, q string) (*Result, error) {
	stmts, texts, _, err := sql.Parse(q)
	if err != nil {
		return nil, err
	}
	var res *Result
	for i, st := range stmts {
		if res, err = s.Run(st, texts[i], nil); err != nil {
			return nil, err
		}
	}
	return res, nil
}

// Readers share the lock while writers move money between accounts and checkpoints push rows
// through a tiny page cache. Every snapshot must see the same total. Run with -race.
func TestParallelReadsSeeConsistentTotals(t *testing.T) {
	t.Setenv("ISODB_CACHE_PAGES", "8")
	old := maxLog
	maxLog = 4096
	defer func() { maxLog = old }()
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	const n = 300
	setup := db.NewSession("t")
	if _, err := run(setup, "CREATE TABLE acc (id integer PRIMARY KEY, bal integer)"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		if _, err := run(setup, fmt.Sprintf("INSERT INTO acc VALUES (%d, 100)", i)); err != nil {
			t.Fatal(err)
		}
	}

	stop := time.Now().Add(2 * time.Second)
	var wg sync.WaitGroup
	errs := make(chan error, 100)
	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			s, rng := db.NewSession("w"), rand.New(rand.NewSource(int64(w)))
			for time.Now().Before(stop) {
				a, b := rng.Intn(n), rng.Intn(n)
				q := fmt.Sprintf("BEGIN; UPDATE acc SET bal = bal - 7 WHERE id = %d; UPDATE acc SET bal = bal + 7 WHERE id = %d; COMMIT", a, b)
				if _, err := run(s, q); err != nil {
					run(s, "ROLLBACK")
				}
			}
		}(w)
	}
	reads := 0
	var mu sync.Mutex
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			s := db.NewSession("r")
			if r%2 == 1 {
				run(s, "SET default_transaction_isolation = 'repeatable read'")
			}
			for time.Now().Before(stop) {
				q := "SELECT sum(bal), count(*) FROM acc"
				if r%2 == 1 {
					q = "BEGIN; SELECT sum(bal) FROM acc WHERE id >= 0; SELECT bal FROM acc WHERE id = 5; SELECT sum(bal), count(*) FROM acc; COMMIT"
				}
				stmts, texts, _, _ := sql.Parse(q)
				for i, st := range stmts {
					res, err := s.Run(st, texts[i], nil)
					if err != nil {
						errs <- err
						return
					}
					if len(res.Rows) == 1 && len(res.Rows[0]) == 2 && (res.Rows[0][0] != int64(100*n) || res.Rows[0][1] != int64(n)) {
						errs <- fmt.Errorf("snapshot saw %v", res.Rows[0])
						return
					}
				}
				mu.Lock()
				reads++
				mu.Unlock()
			}
		}(r)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if reads < 10 {
		t.Fatalf("only %d reads finished", reads)
	}
}
