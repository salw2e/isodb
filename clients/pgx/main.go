package main

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func must(err error) {
	if err != nil {
		fmt.Println("FAIL:", err)
		os.Exit(1)
	}
}

func main() {
	ctx := context.Background()
	c, err := pgx.Connect(ctx, "postgres://isodb@127.0.0.1:15433/x?sslmode=disable")
	must(err)
	defer c.Close(ctx)
	_, err = c.Exec(ctx, "DROP TABLE IF EXISTS items")
	must(err)
	_, err = c.Exec(ctx, "CREATE TABLE items (id BIGINT PRIMARY KEY, name TEXT, price DOUBLE PRECISION, ok BOOLEAN)")
	must(err)
	for i := 1; i <= 5; i++ {
		_, err = c.Exec(ctx, "INSERT INTO items VALUES ($1, $2, $3, $4)", i, fmt.Sprintf("item %d", i), float64(i)*1.5, i%2 == 0)
		must(err)
	}
	var n int64
	var sum float64
	must(c.QueryRow(ctx, "SELECT count(*), sum(price) FROM items WHERE ok = $1", true).Scan(&n, &sum))
	fmt.Println("count/sum:", n, sum)
	rows, err := c.Query(ctx, "SELECT id, name, price, ok FROM items WHERE id > $1 ORDER BY id DESC LIMIT $2", 2, 2)
	must(err)
	for rows.Next() {
		var id int64
		var name string
		var price float64
		var ok bool
		must(rows.Scan(&id, &name, &price, &ok))
		fmt.Println(id, name, price, ok)
	}
	must(rows.Err())
	tx, err := c.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	must(err)
	_, err = tx.Exec(ctx, "UPDATE items SET price = price + 1 WHERE id = $1", 1)
	must(err)
	must(tx.Commit(ctx))
	var p float64
	must(c.QueryRow(ctx, "SELECT price FROM items WHERE id = 1").Scan(&p))
	fmt.Println("price after:", p)
	_, err = c.Exec(ctx, "INSERT INTO items VALUES (1, 'dup', 0, false)")
	var pe *pgconn.PgError
	fmt.Println("duplicate:", errors.As(err, &pe) && pe.Code == "23505", err)
	batch := &pgx.Batch{}
	batch.Queue("SELECT 1 + $1::int", 41)
	batch.Queue("SELECT name FROM items WHERE id = $1", 3)
	br := c.SendBatch(ctx, batch)
	var a int64
	var s string
	must(br.QueryRow().Scan(&a))
	must(br.QueryRow().Scan(&s))
	must(br.Close())
	fmt.Println("batch:", a, s)
}
