"""Stress test: concurrent commits + branch deletes to reproduce corruption.

Usage:
    python tests/stress_kill.py [--rounds 500] [--branches 6] [--writers 4]

Models the real uvicorn failure mode: multiple request handlers sharing
a disk-backed store, where one handler calls delete_branch (which triggers
clean_orphans with min_age=0) while others are actively committing.

The mark-and-sweep in clean_orphans is NOT atomic across all its store
operations. Between the mark phase and sweep phase, another thread can
create new commits that get incorrectly swept because min_age=0 skips
the age guard.
"""

import argparse
import os
import random
import sys
import tempfile
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from kvgit.encoding import loads
from kvgit.kv.disk import Disk
from kvgit.versioned.keyset import Keyset
from kvgit.versioned.kv import (
    BRANCH_HEAD,
    COMMIT_ROOT,
    PARENT_COMMIT,
    VersionedKV,
    _load_root,
)


def verify_store(backend: Disk) -> list[str]:
    """Verify every branch's full commit chain. Returns list of errors."""
    errors = []

    prefix = BRANCH_HEAD.replace("%s", "")
    branches = []
    for key in backend.keys():
        if isinstance(key, str) and key.startswith(prefix):
            name = key[len(prefix) :]
            if name:
                branches.append(name)

    for branch in branches:
        head_bytes = backend.get(BRANCH_HEAD % branch)
        if head_bytes is None:
            errors.append(f"[{branch}] HEAD key missing")
            continue

        try:
            head_hash = loads(head_bytes)
        except Exception as e:
            errors.append(f"[{branch}] HEAD decode failed: {e}")
            continue

        if not isinstance(head_hash, str):
            errors.append(f"[{branch}] HEAD not a string: {head_hash!r}")
            continue

        visited = set()
        queue = [head_hash]
        while queue:
            commit_hash = queue.pop()
            if commit_hash in visited:
                continue
            visited.add(commit_hash)

            # Commit must have a root pointer
            if backend.get(COMMIT_ROOT % commit_hash) is None:
                errors.append(
                    f"[{branch}] commit {commit_hash[:12]} missing commit root"
                )
                continue

            # Walk the keyset HAMT and verify every entry's blob exists
            root = _load_root(backend, commit_hash)
            if root is None:
                errors.append(
                    f"[{branch}] commit {commit_hash[:12]} root pointer corrupt"
                )
                continue

            try:
                ks = Keyset(backend, root=root)
                for user_key, entry in ks.items():
                    if backend.get(entry.blob) is None:
                        errors.append(
                            f"[{branch}] commit {commit_hash[:12]} "
                            f"blob missing for key '{user_key}'"
                        )
            except Exception as e:
                errors.append(
                    f"[{branch}] commit {commit_hash[:12]} keyset walk failed: {e}"
                )
                continue

            # Walk parents
            parent_bytes = backend.get(PARENT_COMMIT % commit_hash)
            if parent_bytes is not None:
                try:
                    parents_raw = loads(parent_bytes)
                except Exception:
                    parents_raw = None
                if isinstance(parents_raw, str):
                    queue.append(parents_raw)
                elif isinstance(parents_raw, list):
                    queue.extend(p for p in parents_raw if isinstance(p, str))

    return errors


def writer_thread(
    backend: Disk, branch: str, stop: threading.Event, error_log: list
) -> None:
    """Continuously commit to a branch until told to stop."""
    while not stop.is_set():
        try:
            worker = VersionedKV(backend, branch=branch)
            data = {f"key-{random.randint(0, 20)}": os.urandom(64)}
            worker.commit(updates=data, on_conflict="abandon")
        except (ValueError, Exception):
            pass
        # Tiny sleep to avoid pure busy-loop but keep pressure high
        time.sleep(random.uniform(0, 0.001))


def deleter_thread(
    backend: Disk, branches_to_delete: list[str], stop: threading.Event, error_log: list
) -> None:
    """Delete branches (triggering clean_orphans min_age=0)."""
    for branch in branches_to_delete:
        if stop.is_set():
            break
        try:
            worker = VersionedKV(backend, branch="main")
            worker.delete_branch(branch)
        except (ValueError, Exception):
            pass


def run_one_round(store_dir: str, num_branches: int, num_writers: int) -> list[str]:
    """Run one round of concurrent commits + deletes. Returns errors."""
    backend = Disk(store_dir)

    # Ensure we have enough branches
    try:
        vkv = VersionedKV(backend, branch="main")
    except ValueError:
        return ["Cannot open main branch"]

    existing = set(vkv.list_branches())

    # Create branches for this round
    write_branches = []
    delete_branches = []
    for i in range(num_branches):
        name = f"wb-{random.randint(0, 10000)}"
        if name not in existing:
            try:
                vkv.create_branch(name)
                # Add some commit history
                w = VersionedKV(backend, branch=name)
                for c in range(3):
                    w.commit(
                        updates={f"key-{c}": os.urandom(64)},
                        on_conflict="abandon",
                    )
                existing.add(name)
            except (ValueError, Exception):
                pass
        write_branches.append(name)

    for i in range(num_branches):
        name = f"db-{random.randint(0, 10000)}"
        if name not in existing:
            try:
                vkv.create_branch(name)
                w = VersionedKV(backend, branch=name)
                for c in range(3):
                    w.commit(
                        updates={f"key-{c}": os.urandom(64)},
                        on_conflict="abandon",
                    )
                existing.add(name)
            except (ValueError, Exception):
                pass
        delete_branches.append(name)

    # Verify integrity before concurrent work
    pre_errors = verify_store(backend)
    if pre_errors:
        return [f"PRE-ROUND: {e}" for e in pre_errors]

    # Launch concurrent writers and a deleter
    stop = threading.Event()
    error_log: list[str] = []
    threads = []

    # Writer threads hammer commits on the write branches
    for branch in write_branches[:num_writers]:
        t = threading.Thread(
            target=writer_thread,
            args=(backend, branch, stop, error_log),
            daemon=True,
        )
        threads.append(t)

    # Deleter thread deletes branches (triggers clean_orphans min_age=0)
    dt = threading.Thread(
        target=deleter_thread,
        args=(backend, delete_branches, stop, error_log),
        daemon=True,
    )
    threads.append(dt)

    # Start all threads
    for t in threads:
        t.start()

    # Let them run concurrently
    dt.join(timeout=10.0)
    stop.set()
    for t in threads:
        t.join(timeout=5.0)

    # Verify integrity after concurrent work
    return verify_store(backend)


def run_stress(rounds: int, num_branches: int, num_writers: int) -> bool:
    """Run the stress test. Returns True if corruption was found.

    Uses a fresh store per round so each round runs in bounded time
    regardless of how min_age affects orphan cleanup.
    """
    for round_num in range(1, rounds + 1):
        with tempfile.TemporaryDirectory(prefix="kvgit_stress_") as tmpdir:
            store_dir = os.path.join(tmpdir, "store")

            # Create fresh store
            backend = Disk(store_dir)
            VersionedKV(backend, branch="main").commit(
                updates={"init": b"seed"}, on_conflict="abandon"
            )
            del backend

            errors = run_one_round(store_dir, num_branches, num_writers)

            if errors:
                print(f"\n{'=' * 60}")
                print(f"CORRUPTION DETECTED in round {round_num}")
                print(f"{'=' * 60}")
                for err in errors:
                    print(f"  {err}")
                print(f"{'=' * 60}\n")
                return True

            if round_num % 10 == 0:
                print(f"  round {round_num}/{rounds} OK")

    print(f"\nAll {rounds} rounds passed — no corruption detected.")
    return False


def main():
    parser = argparse.ArgumentParser(
        description="Stress test kvgit: concurrent commits + branch deletes"
    )
    parser.add_argument(
        "--rounds", type=int, default=500, help="Number of rounds (default: 500)"
    )
    parser.add_argument(
        "--branches",
        type=int,
        default=4,
        help="Branches per role per round (default: 4)",
    )
    parser.add_argument(
        "--writers",
        type=int,
        default=4,
        help="Number of concurrent writer threads (default: 4)",
    )
    args = parser.parse_args()

    print("kvgit concurrency stress test")
    print(f"  rounds={args.rounds}, branches={args.branches}, writers={args.writers}")
    print(f"  PID={os.getpid()}\n")

    found = run_stress(args.rounds, args.branches, args.writers)
    sys.exit(1 if found else 0)


if __name__ == "__main__":
    main()
