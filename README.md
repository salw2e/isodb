# isodb

SQL database written in Go. It uses the Postgres wire protocol, so psql and normal Postgres drivers can connect to it. Tables are stored as B+trees on disk, and SERIALIZABLE was tested with Jepsen's Elle, also while the server was being killed.

It only uses the Go standard library.

```
go build -o isodb .
./isodb -data data -listen 127.0.0.1:15433
psql -h 127.0.0.1 -p 15433 -d demo
```

```
demo=> CREATE TABLE accounts (id serial PRIMARY KEY, owner text, balance integer);
demo=> INSERT INTO accounts (owner, balance) VALUES ('ana', 100), ('ben', 50);
demo=> BEGIN ISOLATION LEVEL SERIALIZABLE;
demo=*> UPDATE accounts SET balance = balance - 30 WHERE owner = 'ana';
demo=*> UPDATE accounts SET balance = balance + 30 WHERE owner = 'ben';
demo=*> COMMIT;
```

Supports READ COMMITTED, REPEATABLE READ and SERIALIZABLE, indexes, joins and GROUP BY. Column types are integer, double, text and boolean.

Not there yet: system catalogs (so `\d` in psql doesn't work), triggers, auth and TLS. Writes run one at a time behind a global lock.

It passes 99.999% of SQLite's sqllogictest suite. The Elle and crash test results, speed against Postgres 17 and how the storage works are in [docs/notes.md](docs/notes.md).

```
go test ./db
```
