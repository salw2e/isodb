// bench measures transactions per second against a running isodb (or Postgres).
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
)

func main() {
	url := flag.String("url", "postgres://bench@127.0.0.1:15433/x?sslmode=disable", "connection string")
	clients := flag.Int("clients", 10, "concurrent clients")
	rows := flag.Int("rows", 10000, "rows in the table")
	secs := flag.Int("secs", 10, "seconds per test")
	setup := flag.Bool("setup", true, "create and fill the table first")
	load := flag.Int("load", 0, "instead of benchmarking, insert this many rows into table big")
	payload := flag.Int("payload", 200, "bytes of text per loaded row")
	flag.Parse()
	ctx := context.Background()
	if *load > 0 {
		loadRows(ctx, *url, *load, *payload)
		return
	}
	if *setup {
		c, err := pgx.Connect(ctx, *url)
		check(err)
		_, err = c.Exec(ctx, "DROP TABLE IF EXISTS bench")
		check(err)
		_, err = c.Exec(ctx, "CREATE TABLE bench (id INTEGER PRIMARY KEY, balance INTEGER, note TEXT)")
		check(err)
		for i := 0; i < *rows; i += 500 {
			b := &pgx.Batch{}
			for j := i; j < i+500 && j < *rows; j++ {
				b.Queue("INSERT INTO bench VALUES ($1, 0, 'row')", j)
			}
			check(c.SendBatch(ctx, b).Close())
		}
		c.Close(ctx)
	}
	for _, t := range []struct{ name, sql string }{
		{"update (one commit each)", "UPDATE bench SET balance = balance + 1 WHERE id = $1"},
		{"point read", "SELECT balance FROM bench WHERE id = $1"},
	} {
		var n atomic.Int64
		var wg sync.WaitGroup
		stop := time.Now().Add(time.Duration(*secs) * time.Second)
		for i := 0; i < *clients; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				c, err := pgx.Connect(ctx, *url)
				check(err)
				defer c.Close(ctx)
				r := rand.New(rand.NewSource(rand.Int63()))
				for time.Now().Before(stop) {
					if _, err := c.Exec(ctx, t.sql, r.Intn(*rows)); err == nil {
						n.Add(1)
					}
				}
			}()
		}
		wg.Wait()
		fmt.Printf("%-26s %8.0f per second (%d clients)\n", t.name, float64(n.Load())/float64(*secs), *clients)
	}
}

func check(err error) {
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func loadRows(ctx context.Context, url string, n, size int) {
	c, err := pgx.Connect(ctx, url)
	check(err)
	defer c.Close(ctx)
	_, err = c.Exec(ctx, "CREATE TABLE IF NOT EXISTS big (id INTEGER PRIMARY KEY, grp INTEGER, payload TEXT)")
	check(err)
	text := make([]byte, size)
	for i := range text {
		text[i] = 'a' + byte(i%26)
	}
	start := time.Now()
	for i := 0; i < n; i += 1000 {
		tx, err := c.Begin(ctx)
		check(err)
		b := &pgx.Batch{}
		for j := i; j < i+1000 && j < n; j++ {
			b.Queue("INSERT INTO big VALUES ($1, $2, $3)", j, j%100, string(text))
		}
		check(tx.SendBatch(ctx, b).Close())
		check(tx.Commit(ctx))
		if (i/1000)%500 == 0 {
			fmt.Printf("%d rows, %.0f rows/s\n", i, float64(i)/time.Since(start).Seconds())
		}
	}
	fmt.Printf("loaded %d rows in %s\n", n, time.Since(start).Round(time.Second))
}
