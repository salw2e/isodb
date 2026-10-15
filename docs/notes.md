# Notes

## Results

| Check | Result |
|---|---|
| Elle list-append, 20,000 SERIALIZABLE transactions from 10 clients | strict serializable, no anomalies |
| The same workload at REPEATABLE READ | Elle finds G2-item (write skew), as snapshot isolation allows |
| Elle list-append while the server is killed with `kill -9` 13 times | strict serializable |
| `kill -9` at random moments, 15 rounds, a checkpoint every 16 KB of log | 48,485 acknowledged inserts and 15,992 deletes kept, bank total unchanged |
| Simulated power cut that tears the last log write, 15 rounds | 9,749 acknowledged inserts and 3,210 deletes kept |
| 5,000,000 rows (1 GB) with the server limited to 256 MB of memory, a checkpoint every 16 MB of log | correct counts and sums, 186 MB peak; loads at 42,000 rows/s, sums the table from a cold cache in 7.8 s |
| [sqllogictest](https://www.sqlite.org/sqllogictest), SQLite's SQL test suite | 5,675,123 of 5,675,180 (99.999%) |
| Clients | psql, psycopg2, pgx (prepared statements, binary values, batches) |

Of the 57 sqllogictest misses, 34 use features isodb does not have (triggers, SQLite's REPLACE and REINDEX, `x''` blob literals) and 3 put a LEFT JOIN inside parentheses. In 19, real Postgres 17 gives the same answer as isodb and the expected answer is SQLite's: Postgres raises division by zero where SQLite returns NULL, and rounds where SQLite truncates when casting to integer. In the last one, Postgres folds away a division by zero that isodb still evaluates.

Each test was also run against a deliberately broken build to make sure it notices: with the serializable checks switched off, Elle reports G2-item; with every 50th commit left out of the log, acknowledged with nothing written, or deletes lost during recovery, the crash tests fail.

## Speed

Measured on a 2-core VPS against Postgres 17 with default settings, on the same disk and with the same client. The queries run on 100,000 customers and 1,000,000 orders, with indexes on `orders.customer_id`, `orders.created` and `customers.country`, cache warm.

| | isodb | Postgres 17 |
|---|---|---|
| Order by primary key | 0.17 ms | 0.23 ms |
| Orders of one customer | 0.47 ms | 0.29 ms |
| A customer joined to their orders | 0.45 ms | 0.40 ms |
| Orders in a `created` range (1,000 rows) | 3.1 ms | 0.47 ms |
| Latest 10 orders (`ORDER BY created DESC LIMIT 10`) | 0.17 ms | 0.22 ms |
| `count(*)` and `sum` over all orders | 407 ms | 66 ms |
| Revenue per country (join of both tables, `GROUP BY`) | 1,463 ms | 398 ms |
| Top 5 customers in one country | 251 ms | 92 ms |
| Commits per second, 10 clients | 1,238 | 1,261 |
| Commits per second, 50 clients | 6,182 | 5,148 |

Lookups and `LIMIT` queries are close to Postgres; queries that touch many rows are 3 to 7 times slower. Commits sync the log with `fdatasync` on a file zero-filled ahead, as Postgres does; on this disk that takes 4 ms against 11.5 ms for a file that grows.

## How it works

- Storage: one B+tree per table in a page file. A checkpoint writes every page it changes to a new place, syncs, then writes one of two alternating meta pages, so a crash during a checkpoint leaves the previous one intact. Pages a checkpoint stops using are reused by the next one. A fixed-size page cache keeps memory bounded.
- Recent changes: rows written since the last checkpoint stay in memory with all their versions, on top of the B+trees. A checkpoint folds them into the page file and drops the ones every running transaction can already see.
- Log: every commit is a checksummed record in a write-ahead log. Commits are queued and a background writer syncs them in batches (group commit). The log file is zero-filled a megabyte ahead so a sync only has to flush data, not a new file size; a commit returns once its record is on disk and becomes visible once every earlier commit has. Recovery opens the last checkpoint, replays the log and cuts off a torn last record.
- Isolation: READ COMMITTED (the default, as in Postgres), REPEATABLE READ (snapshot isolation, first updater wins) and SERIALIZABLE (serializable snapshot isolation after [Cahill et al.](https://dl.acm.org/doi/10.1145/1620585.1620587): reads leave non-blocking markers, read-write dependencies between concurrent transactions are tracked, and a transaction that would get both an incoming and an outgoing dependency is aborted).
- Indexes: one B+tree per index in the page file, keyed by the indexed values (NULL included) followed by the primary key, and updated by each checkpoint. Rows changed since then are found through a map in memory that lists every version they hold. A lookup reads both and rechecks each row against the transaction's snapshot, so an index never needs versions of its own.
- Queries: a hand-written parser and executor. A query on one table reads it row by row and aggregates with running totals, so its memory grows with the result, not the table. A table filtered by `=` on its primary key or on every column of an index, or by a range on an integer index column, is read through the index; `ORDER BY` such a column with `LIMIT` reads the index in order and stops early. A join streams its biggest table past hash tables of the others, or looks a table up by primary key or index for each row driving it when those rows are few next to the table's size. Constant NULL conditions are folded the way Postgres folds them.

## Limits

- Plain reads at READ COMMITTED and REPEATABLE READ run in parallel. Writes, `SELECT ... FOR UPDATE` and anything under SERIALIZABLE run one at a time behind a global lock; transactions interleave between statements.
- A join holds every table but the biggest in memory, unless it can look them up by primary key or index.
- Ranges and `ORDER BY` use an index only on integer columns, because text keys are not stored in SQL sort order. `CREATE INDEX` keeps the new index in memory until the next checkpoint.
- A checkpoint pauses other statements while it writes: 12 s after loading a million rows with three indexes. Rows changed since the last checkpoint stay in memory, so memory grows with `ISODB_MAXLOG` (64 MB of log by default). The page file never shrinks.
- Types are integer, double, text and boolean. `numeric` is stored as a double, dates as text.
- SERIALIZABLE aborts more often than Postgres, because the pivot rule is the conservative one.
- No system catalogs (so `psql`'s `\d` does not work), no triggers, no authentication, no TLS.

## Running the checks

```
go build -o isodb . && go build -o slt ./cmd/slt && go test ./db
./slt path/to/sqllogictest/test/select1.test

pip install -r jepsen/requirements.txt
./isodb -data /tmp/d &
python jepsen/listappend.py --txns 20000 --clients 10 --out h.edn
java -jar elle-cli-standalone.jar -m list-append -c strict-serializable -f edn h.edn

ISODB_MAXLOG=16384 python jepsen/crash.py --rounds 15
ISODB_MAXLOG=16384 python jepsen/crash.py --rounds 15 --fault 0.001
ELLE=elle-cli-standalone.jar jepsen/elle-crash.sh
```

[elle-cli](https://github.com/ligurio/elle-cli) is the command-line front end to Elle. `ISODB_MAXLOG` sets how much log triggers a checkpoint, `ISODB_CACHE_PAGES` the page cache size.
