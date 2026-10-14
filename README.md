# isodb

A SQL database written from scratch in Go. It speaks the PostgreSQL wire protocol, so `psql` and Postgres drivers connect to it unchanged. Its SERIALIZABLE isolation is checked with Jepsen's Elle, the checker used in Jepsen's database analyses: no anomalies in tens of thousands of concurrent transactions, including while the server is being killed.

```
go build -o isodb .
./isodb -data data -listen 127.0.0.1:15433
psql -h 127.0.0.1 -p 15433
```

No dependencies outside the Go standard library.

## Results

| Check | Result |
|---|---|
| Elle list-append, 20,000 SERIALIZABLE transactions from 10 clients | strict serializable, no anomalies |
| The same workload at REPEATABLE READ | Elle finds G2-item (write skew), as snapshot isolation allows |
| Elle list-append while the server is killed with `kill -9` 20 times | strict serializable |
| `kill -9` at random moments, 15 rounds | 84,074 acknowledged inserts and 21,326 transfers, nothing lost, bank total unchanged |
| Simulated power cut that tears the last log write, 15 rounds | 31,434 acknowledged inserts, nothing lost |
| [sqllogictest](https://www.sqlite.org/sqllogictest), SQLite's SQL test suite | 5,675,121 of 5,675,180 (99.999%) |
| Clients | psql, psycopg2, pgx (prepared statements, binary values, batches) |

Of the 59 sqllogictest misses, 34 use features isodb does not have (triggers, SQLite's REPLACE and REINDEX, `x''` blob literals) and 3 put a LEFT JOIN inside parentheses. In 19, real Postgres 17 gives the same answer as isodb and the expected answer is SQLite's: Postgres raises division by zero where SQLite returns NULL, and rounds where SQLite truncates when casting to integer. In the last 3, Postgres folds away a division by zero that isodb still evaluates.

The tests were checked against deliberately broken builds. With the serializable checks switched off, Elle reports G2-item anomalies. With every 50th commit left out of the log, the crash test reports lost inserts.

## How it works

- **Rows** live in memory with multiple versions. Each version belongs to the transaction that wrote it, and a transaction sees the versions committed before its snapshot.
- **Durability**: every commit is written to a log with a checksum per record and fsynced before the client is told. At 64 MB the committed state is written to a snapshot and the log starts over. Recovery loads the snapshot, replays the log and cuts off a torn last record.
- **Isolation**: READ COMMITTED (the default, as in Postgres), REPEATABLE READ (snapshot isolation, first updater wins) and SERIALIZABLE (serializable snapshot isolation after [Cahill et al.](https://dl.acm.org/doi/10.1145/1620585.1620587): reads leave non-blocking markers, read-write dependencies between concurrent transactions are tracked, and a transaction that would get both an incoming and an outgoing dependency is aborted).
- **SQL**: hand-written parser and executor. Joins are ordered by size, use hash joins for equality conditions and primary key lookups. Subqueries, aggregates, GROUP BY, UNION/INTERSECT/EXCEPT, views, `ON CONFLICT` and `RETURNING` are supported.

## Limits

- All data has to fit in memory.
- Statements run one at a time behind a global lock; transactions interleave between statements.
- Only primary keys are indexed. `CREATE INDEX` is accepted and enforces `UNIQUE`, but queries still scan.
- Types are integer, double, text and boolean. `numeric` is stored as a double, dates as text.
- SERIALIZABLE aborts more often than Postgres, because the pivot rule is the conservative one.
- No system catalogs (so `psql`'s `\d` does not work), no triggers, no authentication, no TLS.

## Running the checks

```
go build -o isodb . && go build -o slt ./cmd/slt
./slt path/to/sqllogictest/test/select1.test

pip install -r jepsen/requirements.txt
./isodb -data /tmp/d &
python jepsen/listappend.py --txns 20000 --clients 10 --out h.edn
java -jar elle-cli-standalone.jar -m list-append -c strict-serializable -f edn h.edn

python jepsen/crash.py --rounds 15
python jepsen/crash.py --rounds 15 --fault 0.0005
ELLE=elle-cli-standalone.jar jepsen/elle-crash.sh
```

[elle-cli](https://github.com/ligurio/elle-cli) is the command-line front end to Elle.
