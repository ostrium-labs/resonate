#!/usr/bin/env python3
"""Chaos run for the TiKV store: no lost and no duplicated settlement.

Clients create promises and settle each one through a server on TiKV. Each
settle attempt carries a distinct value, so a retried settle that landed twice
would show up. Meanwhile the run kills the server with SIGKILL and restarts it,
restarts PD, and restarts TiKV.

At the end every promise is read back. Every create and every settle that was
acknowledged must be there, each promise must have been settled exactly once,
and its value must be the one in the first acknowledged answer.

  chaos.py --server-bin target/release/resonate --pd 127.0.0.1:23790 \
           --pd-container rs-fw1-pd --tikv-container rs-fw1-tikv

Standard library only. Exits non-zero on any violation.
"""

import argparse
import base64
import json
import os
import random
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request

VERSION = "2026-04-01"


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Server:
    def __init__(self, args, port, root):
        self.args, self.port, self.root, self.proc = args, port, root, None

    def start(self):
        env = dict(os.environ)
        env.update(
            {
                "RESONATE_SERVERS__ACTIVE": "server_tikv",
                "RESONATE_SERVERS__SERVER_TIKV__PD": json.dumps(self.args.pd.split(",")),
                "RESONATE_SERVERS__SERVER_TIKV__KEYSPACE": self.args.keyspace,
                "RESONATE_SERVERS__SERVER_TIKV__ROOT": self.root,
                "RESONATE_GATEWAYS__GATEWAY_HTTP__BIND": f"127.0.0.1:{self.port}",
                "RESONATE_GATEWAYS__GATEWAY_METRICS__BIND": f"127.0.0.1:{free_port()}",
            }
        )
        log = open(self.args.log, "ab")
        # A server that cannot reach PD at startup exits; a supervisor would
        # restart it, and so does this, until it is ready or two minutes pass.
        deadline = time.time() + 120
        while time.time() < deadline:
            if self.proc is None or self.proc.poll() is not None:
                self.proc = subprocess.Popen(
                    [self.args.server_bin, "serve"], env=env, stdout=log, stderr=log
                )
            try:
                urllib.request.urlopen(f"http://127.0.0.1:{self.port}/ready", timeout=1)
                return
            except Exception:
                time.sleep(0.5)
        raise RuntimeError("server did not become ready within 2 minutes")

    def kill9(self):
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGKILL)
            self.proc.wait()
        self.proc = None

    def stop(self):
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=20)
            except subprocess.TimeoutExpired:
                self.kill9()


def call(port, kind, data, timeout=5):
    body = json.dumps(
        {"kind": kind, "head": {"corrId": str(random.getrandbits(48)), "version": VERSION}, "data": data}
    ).encode()
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}/", data=body, headers={"content-type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            out = json.loads(r.read())
    except urllib.error.HTTPError as e:
        try:
            out = json.loads(e.read())
        except Exception:
            return None
    except Exception:
        return None  # no answer: the request may or may not have applied
    return out["head"]["status"], out["data"]


def b64(s):
    return base64.b64encode(s.encode()).decode()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--server-bin", required=True)
    ap.add_argument("--pd", required=True)
    ap.add_argument("--keyspace", default="resonate_chaos")
    ap.add_argument("--pd-container", required=True)
    ap.add_argument("--tikv-container", required=True)
    ap.add_argument("--promises", type=int, default=600)
    ap.add_argument("--clients", type=int, default=8)
    ap.add_argument("--log", default="chaos-server.log")
    args = ap.parse_args()
    runtime = shutil.which("podman") or shutil.which("docker")

    run = f"{int(time.time())}-{os.getpid()}"
    root = f"chaos-{run}/"
    port = free_port()
    server = Server(args, port, root)
    server.start()
    far = int(time.time() * 1000) + 86_400_000

    created, first_settle_ack = {}, {}
    acked_values = {}  # id -> set of values seen in acknowledged settle answers
    lock = threading.Lock()
    work = list(range(args.promises))
    random.shuffle(work)
    stats = {"no_answer": 0, "unavailable": 0}

    def client(items):
        for i in items:
            pid = f"chaos-{run}-{i}"
            while True:  # create until acknowledged
                r = call(port, "promise.create", {"id": pid, "timeoutAt": far, "param": {}, "tags": {}})
                if r and r[0] in (200, 201):
                    with lock:
                        created[pid] = True
                    break
                with lock:
                    stats["no_answer" if r is None else "unavailable"] += 1
                time.sleep(0.2)
            attempt = 0
            while True:  # settle until acknowledged, a new value per attempt
                attempt += 1
                value = b64(f"{i}-a{attempt}")
                r = call(port, "promise.settle", {"id": pid, "state": "resolved", "value": {"data": value}})
                if r and r[0] == 200:
                    got = r[1]["promise"]["value"].get("data")
                    with lock:
                        first_settle_ack.setdefault(pid, got)
                        acked_values.setdefault(pid, set()).add(got)
                    break
                with lock:
                    stats["no_answer" if r is None else "unavailable"] += 1
                time.sleep(0.2)

    def chaos():
        steps = [
            (4, "kill -9 the server and restart it", lambda: (server.kill9(), server.start())),
            (10, "restart PD", lambda: subprocess.run([runtime, "restart", "-t", "0", args.pd_container], check=True)),
            (18, "kill -9 the server and restart it", lambda: (server.kill9(), server.start())),
            (24, "restart TiKV", lambda: subprocess.run([runtime, "restart", "-t", "0", args.tikv_container], check=True)),
        ]
        start = time.time()
        for at, name, act in steps:
            time.sleep(max(0, start + at - time.time()))
            print(f"[chaos] t={time.time() - start:5.1f}s {name}", flush=True)
            act()

    chunks = [work[k :: args.clients] for k in range(args.clients)]
    threads = [threading.Thread(target=client, args=(c,)) for c in chunks]
    chaos_thread = threading.Thread(target=chaos)
    t0 = time.time()
    for t in threads + [chaos_thread]:
        t.start()
    for t in threads + [chaos_thread]:
        t.join()
    print(f"[chaos] workload done in {time.time() - t0:.1f}s: {stats}", flush=True)

    bad = []
    for i in range(args.promises):
        pid = f"chaos-{run}-{i}"
        r = None
        for _ in range(50):
            r = call(port, "promise.get", {"id": pid})
            if r:
                break
            time.sleep(0.2)
        if not r or r[0] != 200:
            bad.append(f"{pid}: lost (get answered {r and r[0]})")
            continue
        p = r[1]["promise"]
        final = p["value"].get("data")
        if p["state"] != "resolved":
            bad.append(f"{pid}: state {p['state']}")
        if final != first_settle_ack.get(pid):
            bad.append(f"{pid}: final value {final!r} != first acknowledged {first_settle_ack.get(pid)!r}")
        if len(acked_values.get(pid, ())) != 1:
            bad.append(f"{pid}: acknowledged values differ: {acked_values.get(pid)}")
    server.stop()
    print(f"[chaos] {args.promises} promises, root {root}: {len(bad)} violations")
    for b in bad[:20]:
        print("  " + b)
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
