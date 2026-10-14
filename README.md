# isodb

A SQL database written from scratch in Go. It speaks the PostgreSQL wire protocol, so `psql` and Postgres drivers connect to it unchanged. Tables are stored in copy-on-write B+trees on disk and can be much larger than memory. SERIALIZABLE isolation is checked with Jepsen's Elle, including while the server is being killed.

No dependencies outside the Go standard library.

## Getting started

```
go build -o isodb .
./isodb -data data -listen 127.0.0.1:15433
```

Then connect with any Postgres client:

```
$ psql -h 127.0.0.1 -p 15433 -d demo
demo=> CREATE TABLE accounts (id serial PRIMARY KEY, owner text, balance integer);
CREATE TABLE
demo=> INSERT INTO accounts (owner, balance) VALUES ('ana', 100), ('ben', 50);
INSERT 0 2
demo=> BEGIN ISOLATION LEVEL SERIALIZABLE;
BEGIN
demo=*> UPDATE accounts SET balance = balance - 30 WHERE owner = 'ana';
UPDATE 1
demo=*> UPDATE accounts SET balance = balance + 30 WHERE owner = 'ben';
UPDATE 1
demo=*> COMMIT;
COMMIT
demo=> SELECT owner, balance FROM accounts ORDER BY owner;
 owner | balance
-------+---------
 ana   |      70
 ben   |      80
(2 rows)
```

## Results

| Check | Result |
|---|---|
| Elle list-append, 20,000 SERIALIZABLE transactions from 10 clients | strict serializable, no anomalies |
| The same workload at REPEATABLE READ | Elle finds G2-item (write skew), as snapshot isolation allows |
| Elle list-append while the server is killed with `kill -9` 13 times | strict serializable |
| `kill -9` at random moments, 15 rounds, a checkpoint every 16 KB of log | 48,485 acknowledged inserts and 15,992 deletes kept, bank total unchanged |
| Simulated power cut that tears the last log write, 15 rounds | 9,749 acknowledged inserts and 3,210 deletes kept |
| 5,000,000 rows (1 GB) with the server limited to 256 MB of memory | correct counts and sums, 186 MB peak, 1.2 GB page file |
| [sqllogictest](https://www.sqlite.org/sqllogictest), SQLite's SQL test suite | 5,675,123 of 5,675,180 (99.999%) |
| Clients | psql, psycopg2, pgx (prepared statements, binary values, batches) |

Of the 57 sqllogictest misses, 34 use features isodb does not have (triggers, SQLite's REPLACE and REINDEX, `x''` blob literals) and 3 put a LEFT JOIN inside parentheses. In 19, real Postgres 17 gives the same answer as isodb and the expected answer is SQLite's: Postgres raises division by zero where SQLite returns NULL, and rounds where SQLite truncates when casting to integer. In the last one, Postgres folds away a division by zero that isodb still evaluates.

Each test was also run against a deliberately broken build to make sure it notices: with the serializable checks switched off, Elle reports G2-item; with every 50th commit left out of the log, acknowledged with nothing written, or deletes lost during recovery, the crash tests fail.

## Speed

Measured on a 2-core VPS, with Postgres 17 (default settings) on the same disk and the same benchmark client for comparison.

| | isodb | Postgres 17 |
|---|---|---|
| Commits per second, 10 clients | 1,238 | 1,261 |
| Commits per second, 50 clients | 6,182 | 5,148 |
| Point reads per second, 10 clients | 20,500 | 9,300 |
| Bulk load | 29,000 rows per second | |
| `count(*)` and sums over 5,000,000 rows, cold cache | 15 s | |
| Lookup by primary key in that table | 2 ms | |
| Lookup by primary key while another client scans 400,000 rows | 0.2 ms | |
| Lookup by an indexed column, 200,000 rows | 0.25 ms (206 ms without the index) | |
| Loading 20,000 rows into a table with a `UNIQUE` column | 0.3 s (63 s before unique checks used an index) | |

isodb reads faster because it does much less per query: no planner, no permission checks, no system catalogs. Commits went from 416 to 1,238 per second once the log was zero-filled ahead and synced with `fdatasync`, the way Postgres does it; a sync on this disk takes 11.5 ms when the file grows and 4 ms when it does not.

## How it works

- **Storage**: one B+tree per table in a page file. A checkpoint writes every page it changes to a new place, syncs, then writes one of two alternating meta pages, so a crash during a checkpoint leaves the previous one intact. Pages a checkpoint stops using are reused by the next one. A fixed-size page cache keeps memory bounded.
- **Recent changes**: rows written since the last checkpoint stay in memory with all their versions, on top of the B+trees. A checkpoint folds them into the page file and drops the ones every running transaction can already see.
- **Log**: every commit is a checksummed record in a write-ahead log. Commits are queued and a background writer syncs them in batches (group commit). The log file is zero-filled a megabyte ahead so a sync only has to flush data, not a new file size; a commit returns once its record is on disk and becomes visible once every earlier commit has. Recovery opens the last checkpoint, replays the log and cuts off a torn last record.
- **Isolation**: READ COMMITTED (the default, as in Postgres), REPEATABLE READ (snapshot isolation, first updater wins) and SERIALIZABLE (serializable snapshot isolation after [Cahill et al.](https://dl.acm.org/doi/10.1145/1620585.1620587): reads leave non-blocking markers, read-write dependencies between concurrent transactions are tracked, and a transaction that would get both an incoming and an outgoing dependency is aborted).
- **Indexes**: one B+tree per index in the page file, keyed by the indexed values followed by the primary key, and updated by each checkpoint. Rows changed since then are found through a map in memory that lists every version they hold. A lookup reads both and rechecks each row against the transaction's snapshot, so an index never needs versions of its own.
- **Queries**: a hand-written parser and executor. A query on one table reads it row by row and aggregates with running totals, so its memory grows with the result, not the table. A table filtered by `=` on its primary key, or on every column of an index, is read through it instead of scanned. Joins are ordered by size and use hash joins, or look a table up by primary key or index for each row driving it when those rows are few next to the table's size. Constant NULL conditions are folded the way Postgres folds them.

## Limits

- Plain reads at READ COMMITTED and REPEATABLE READ run in parallel. Writes, `SELECT ... FOR UPDATE` and anything under SERIALIZABLE run one at a time behind a global lock; transactions interleave between statements.
- A join reads each table into memory unless it can look the table up by primary key or index from a few rows of the others; only single-table queries stream.
- Indexes are only used for `=` on every column of the index; ranges, `ORDER BY` and join conditions still scan. `CREATE INDEX` keeps the new index in memory until the next checkpoint.
- A checkpoint pauses other statements while it writes. The page file never shrinks.
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
