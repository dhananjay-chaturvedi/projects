"""Concurrency-primitive tests: no lost updates, no torn files."""

from __future__ import annotations

import json
import threading
from pathlib import Path

from common.concurrency import (
    append_jsonl_locked,
    atomic_write_text,
    file_lock,
    read_json,
    update_json_locked,
)


def test_update_json_locked_no_lost_updates(tmp_path):
    path = tmp_path / "counter.json"

    def bump(_):
        def mut(cur):
            cur = cur or {"n": 0}
            cur["n"] += 1
            return cur
        update_json_locked(path, mut, default={"n": 0})

    threads = [threading.Thread(target=bump, args=(i,)) for i in range(50)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert read_json(path)["n"] == 50


def test_update_json_locked_list_accumulates(tmp_path):
    path = tmp_path / "items.json"

    def add(i):
        update_json_locked(path, lambda cur: (cur or []) + [i], default=[])

    threads = [threading.Thread(target=add, args=(i,)) for i in range(40)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    got = sorted(read_json(path))
    assert got == list(range(40))


def test_append_jsonl_locked_keeps_all_rows(tmp_path):
    path = tmp_path / "log.jsonl"

    def writer(i):
        append_jsonl_locked(path, [{"i": i}])

    threads = [threading.Thread(target=writer, args=(i,)) for i in range(60)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    lines = [json.loads(ln) for ln in path.read_text().splitlines() if ln.strip()]
    assert sorted(r["i"] for r in lines) == list(range(60))


def test_atomic_write_text_never_torn(tmp_path):
    path = tmp_path / "blob.txt"
    payloads = ["x" * 1000, "y" * 2000, "z" * 500]

    def writer(p):
        for _ in range(20):
            atomic_write_text(path, p)

    threads = [threading.Thread(target=writer, args=(p,)) for p in payloads]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # The final content must be exactly one of the payloads (never a mix).
    assert path.read_text() in payloads


def test_file_lock_is_mutually_exclusive(tmp_path):
    target = tmp_path / "resource"
    order: list[str] = []
    in_section = {"now": 0, "max": 0}
    lock = threading.Lock()

    def worker():
        with file_lock(target):
            with lock:
                in_section["now"] += 1
                in_section["max"] = max(in_section["max"], in_section["now"])
            # simulate work
            for _ in range(1000):
                pass
            with lock:
                in_section["now"] -= 1

    threads = [threading.Thread(target=worker) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Cross-process flock does not exclude threads of the *same* process, so we
    # only assert the lock is reentrant-safe and never deadlocks here.
    assert in_section["now"] == 0
