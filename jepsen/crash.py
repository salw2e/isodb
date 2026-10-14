"""Kills isodb at random moments and checks that nothing acknowledged was lost.

Each round starts the server, runs bank transfers (total must never change) and numbered inserts
(every acknowledged insert must survive), then kills the server with SIGKILL, or lets a simulated
power cut tear the log (ISODB_FAULT), restarts it and checks both invariants.
"""
import argparse, os, random, shutil, signal, subprocess, sys, threading, time
import psycopg2, psycopg2.errors

ap = argparse.ArgumentParser()
ap.add_argument("--rounds", type=int, default=20)
ap.add_argument("--port", type=int, default=15434)
ap.add_argument("--fault", default="", help="ISODB_FAULT probability: simulated power cut that tears a log write")
ap.add_argument("--dir", default="/tmp/isodb-crash")
args = ap.parse_args()

ACCOUNTS, START = 10, 1000
binary = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "isodb")

def start():
    env = dict(os.environ)
    if args.fault:
        env["ISODB_FAULT"] = args.fault
    p = subprocess.Popen([binary, "-listen", f"127.0.0.1:{args.port}", "-data", args.dir], env=env,
                         stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    for _ in range(100):
        try:
            psycopg2.connect(host="127.0.0.1", port=args.port, user="crash", dbname="x").close()
            return p
        except Exception:
            time.sleep(0.05)
    raise SystemExit("server did not start: " + p.stderr.read().decode())

def connect():
    c = psycopg2.connect(host="127.0.0.1", port=args.port, user="crash", dbname="x")
    c.autocommit = True
    return c

shutil.rmtree(args.dir, ignore_errors=True)
p = start()
c = connect().cursor()
c.execute("CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER NOT NULL)")
c.execute("CREATE TABLE log (id INTEGER PRIMARY KEY, note TEXT)")
for i in range(ACCOUNTS):
    c.execute("INSERT INTO accounts VALUES (%s, %s)", (i, START))
acked = set()
deleted = set()
maybe_deleted = set()  # the server died before saying whether the delete happened
next_id = [0]
lock = threading.Lock()
stats = {"transfers": 0, "inserts": 0, "kills": 0, "torn": 0}

def transfers(stop):
    conn = None
    while not stop.is_set():
        try:
            conn = conn or connect()
            cur = conn.cursor()
            a, b = random.sample(range(ACCOUNTS), 2)
            cur.execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
            cur.execute("SELECT balance FROM accounts WHERE id = %s", (a,))
            bal = cur.fetchone()[0]
            amt = random.randint(1, 50)
            if bal >= amt:
                cur.execute("UPDATE accounts SET balance = balance - %s WHERE id = %s", (amt, a))
                # id + 0 cannot use the primary key, so this one finds its row with a full scan
                where = "id + 0" if random.random() < 0.5 else "id"
                cur.execute(f"UPDATE accounts SET balance = balance + %s WHERE {where} = %s", (amt, b))
            cur.execute("COMMIT")
            with lock:
                stats["transfers"] += 1
        except (psycopg2.errors.SerializationFailure, psycopg2.errors.DeadlockDetected):
            try:
                cur.execute("ROLLBACK")
            except Exception:
                conn = None
        except Exception:
            conn = None
            time.sleep(0.05)

def inserts(stop):
    conn = None
    while not stop.is_set():
        with lock:
            next_id[0] += 1
            i = next_id[0]
        try:
            conn = conn or connect()
            conn.cursor().execute("INSERT INTO log VALUES (%s, %s)", (i, "row %d" % i))
            with lock:
                acked.add(i)
                stats["inserts"] += 1
            if i % 3 == 0:
                gone = i - 2
                with lock:
                    maybe_deleted.add(gone)
                cur = conn.cursor()
                cur.execute("DELETE FROM log WHERE id = %s", (gone,))
                with lock:
                    maybe_deleted.discard(gone)
                if cur.rowcount == 1:
                    with lock:
                        deleted.add(gone)
                        stats["deletes"] = stats.get("deletes", 0) + 1
        except Exception:
            conn = None
            time.sleep(0.05)

def check():
    cur = connect().cursor()
    cur.execute("SELECT sum(balance), count(*) FROM accounts")
    total, n = cur.fetchone()
    if total != ACCOUNTS * START or n != ACCOUNTS:
        raise SystemExit(f"FAIL: accounts sum {total} count {n}, want {ACCOUNTS * START} {ACCOUNTS}")
    cur.execute("SELECT id FROM log")
    have = {r[0] for r in cur.fetchall()}
    lost = acked - deleted - maybe_deleted - have
    if lost:
        raise SystemExit(f"FAIL: {len(lost)} acknowledged inserts lost, e.g. {sorted(lost)[:5]}")
    back = deleted & have
    if back:
        raise SystemExit(f"FAIL: {len(back)} acknowledged deletes undone, e.g. {sorted(back)[:5]}")

for rnd in range(args.rounds):
    stop = threading.Event()
    ts = [threading.Thread(target=transfers, args=(stop,)) for _ in range(4)] + [threading.Thread(target=inserts, args=(stop,)) for _ in range(2)]
    for t in ts:
        t.start()
    deadline = time.time() + random.uniform(0.3, 2.0)
    while time.time() < deadline and p.poll() is None:
        time.sleep(0.02)
    if p.poll() is None:
        p.send_signal(signal.SIGKILL)
        stats["kills"] += 1
    else:
        stats["torn"] += 1
    p.wait()
    stop.set()
    for t in ts:
        t.join()
    p = start()
    try:
        check()
    except SystemExit:
        p.kill()
        raise
    print(f"round {rnd + 1}: ok, {len(acked)} acknowledged inserts and {len(deleted)} deletes kept, total balance intact", flush=True)

p.send_signal(signal.SIGTERM)
p.wait()
print("PASS", stats)
