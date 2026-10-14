"""Runs concurrent list-append transactions against isodb and writes a Jepsen history for Elle.

Each transaction is 1-4 micro-operations on a few hot keys: append a unique number to a key's list,
or read the list. Elle then checks the history for isolation anomalies.
"""
import argparse, random, threading, time, sys
import psycopg2, psycopg2.errors

ap = argparse.ArgumentParser()
ap.add_argument("--port", type=int, default=15433)
ap.add_argument("--iso", default="SERIALIZABLE")
ap.add_argument("--clients", type=int, default=8)
ap.add_argument("--txns", type=int, default=2000)
ap.add_argument("--keys", type=int, default=5)
ap.add_argument("--out", default="history.edn")
ap.add_argument("--table", default="lists")
args = ap.parse_args()

def connect():
    c = psycopg2.connect(host="127.0.0.1", port=args.port, user="jepsen", dbname="x", connect_timeout=5)
    c.autocommit = True
    return c

setup = connect()
setup.cursor().execute(f"DROP TABLE IF EXISTS {args.table}")
setup.cursor().execute(f"CREATE TABLE {args.table} (k INTEGER PRIMARY KEY, v TEXT)")
setup.close()

lock = threading.Lock()
history = []
counter = [0]
done = [0]

def record(op):
    with lock:
        op["index"] = len(history)
        op["time"] = time.monotonic_ns()
        history.append(op)

def next_value():
    with lock:
        counter[0] += 1
        return counter[0]

def edn(v):
    if v is None:
        return "nil"
    if isinstance(v, list):
        return "[" + " ".join(edn(x) for x in v) + "]"
    if isinstance(v, str):
        return ":" + v
    return str(v)

def worker(proc):
    conn = None
    while True:
        with lock:
            if done[0] >= args.txns:
                return
            done[0] += 1
        if conn is None:
            try:
                conn = connect()
            except Exception:
                time.sleep(0.2)
                continue
        base = random.randint(0, 1000) // 100  # keys drift slowly so lists stay short
        mops = []
        for _ in range(random.randint(1, 4)):
            k = base * args.keys + random.randrange(args.keys)
            if random.random() < 0.5:
                mops.append(["append", k, next_value()])
            else:
                mops.append(["r", k, None])
        record({"type": "invoke", "process": proc, "value": [list(m) for m in mops]})
        cur = conn.cursor()
        try:
            cur.execute(f"BEGIN ISOLATION LEVEL {args.iso}")
            result = []
            for f, k, v in mops:
                if f == "r":
                    cur.execute(f"SELECT v FROM {args.table} WHERE k = %s", (k,))
                    row = cur.fetchone()
                    result.append(["r", k, [int(x) for x in row[0].split(",")] if row else []])
                else:
                    cur.execute(f"SELECT v FROM {args.table} WHERE k = %s", (k,))
                    row = cur.fetchone()
                    if row is None:
                        cur.execute(f"INSERT INTO {args.table} (k, v) VALUES (%s, %s)", (k, str(v)))
                    else:
                        cur.execute(f"UPDATE {args.table} SET v = v || %s WHERE k = %s", ("," + str(v), k))
                    result.append(["append", k, v])
            cur.execute("COMMIT")
            record({"type": "ok", "process": proc, "value": result})
        except (psycopg2.errors.SerializationFailure, psycopg2.errors.DeadlockDetected, psycopg2.errors.UniqueViolation) as e:
            try:
                cur.execute("ROLLBACK")
            except Exception:
                conn = None
            record({"type": "fail", "process": proc, "value": mops, "error": type(e).__name__})
        except Exception as e:
            # connection lost: the transaction may or may not have committed
            record({"type": "info", "process": proc, "value": mops, "error": type(e).__name__})
            try:
                conn.close()
            except Exception:
                pass
            conn = None
            proc += args.clients  # a crashed process never comes back under the same id

threads = [threading.Thread(target=worker, args=(p,)) for p in range(args.clients)]
for t in threads:
    t.start()
for t in threads:
    t.join()

with open(args.out, "w") as f:
    for op in history:
        f.write("{:index %d, :time %d, :type :%s, :process %d, :value %s}\n" % (op["index"], op["time"], op["type"], op["process"], edn(op["value"])))
counts = {}
for op in history:
    counts[op["type"]] = counts.get(op["type"], 0) + 1
print("history:", counts, file=sys.stderr)
