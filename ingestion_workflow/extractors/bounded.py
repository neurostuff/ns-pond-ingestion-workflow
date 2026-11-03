"""Each item run in a child process the parent kills when it goes over its limits.

Docling's RSS grew without bound on one PDF (7rsekeh66ykb, a 7-page CJK review)
until the kernel killed the whole extract run at 47 GB, and the ~380 articles
batched with it went unrecorded. A child the parent can kill makes that one
article's failure instead.

A child serves one item after another, so a warm model is loaded once per child,
not once per item. A child that is killed is replaced before the next item.
Memory is read from /proc, so off Linux only the time limit holds.
"""

from __future__ import annotations

import logging
import os
import queue
import threading
import time
from dataclasses import dataclass
from multiprocessing.context import BaseContext
from typing import Any, Callable, Iterator, List, Optional, Sequence, Tuple

from ingestion_workflow.utils.isolation import error_text

logger = logging.getLogger(__name__)

#: How often the parent reads a busy child's memory.
POLL_SECONDS = 0.2
_PAGE_BYTES = os.sysconf("SC_PAGE_SIZE")
_READY = "ready"


@dataclass(frozen=True)
class Limits:
    #: Resident memory of the child and its descendants.
    memory_mb: int
    #: Wall time for one item, from when the child is handed it.
    timeout_s: float


class Stopped(RuntimeError):
    """The child stopped before answering: killed over a limit, or dead of its own."""


def bounded_map(
    items: Sequence[Any],
    worker: Callable[..., Any],
    args: Tuple[Any, ...],
    *,
    workers: int,
    limits: Limits,
    context: BaseContext,
    initializer: Optional[Callable[..., None]] = None,
    initargs: Tuple[Any, ...] = (),
) -> Iterator[Tuple[int, Any, Optional[BaseException]]]:
    """`worker(item, *args)` for each item, in `workers` children, as each answers.

    Yields `(index, result, None)`, or `(index, None, error)` when the worker raised
    (a `RuntimeError` holding its `error_text`) or its child was `Stopped`.
    `initializer(slot, *initargs)` runs in each child before its first item; `slot`
    is the child's place in `range(workers)`, kept by its replacements."""
    todo: "queue.Queue[Tuple[int, Any]]" = queue.Queue()
    for entry in enumerate(items):
        todo.put(entry)
    answers: "queue.Queue[Tuple[int, Any, Optional[BaseException]]]" = queue.Queue()
    children = [
        _Child(context, worker, limits, initializer, (slot, *initargs))
        for slot in range(max(1, min(workers, len(items))))
    ]
    threads = [
        threading.Thread(target=_drain, args=(child, todo, args, answers), daemon=True)
        for child in children
    ]
    try:
        for thread in threads:
            thread.start()
        for _ in range(len(items)):
            yield answers.get()
    finally:
        # An early exit leaves items queued: emptied, each thread stops at its next.
        while True:
            try:
                todo.get_nowait()
            except queue.Empty:
                break
        for child in children:
            child.close()


def _drain(child: "_Child", todo: queue.Queue, args: Tuple[Any, ...], answers: queue.Queue) -> None:
    while True:
        try:
            index, item = todo.get_nowait()
        except queue.Empty:
            return
        try:
            answers.put((index, child.run((item, *args)), None))
        except Exception as exc:  # noqa: BLE001 - this item's failure alone
            answers.put((index, None, exc))


class _Child:
    """One child process at a time in one slot."""

    def __init__(self, context, worker, limits, initializer, initargs) -> None:
        self._context = context
        self._target = (worker, initializer, initargs)
        self._limits = limits
        self._process = None
        self._conn = None
        self._lock = threading.Lock()

    def run(self, call: Tuple[Any, ...]) -> Any:
        if self._process is None:
            self._start()
        self._conn.send(call)
        ok, value = self._answer()
        if not ok:
            raise RuntimeError(value)
        return value

    def close(self) -> None:
        with self._lock:
            process, conn = self._process, self._conn
            self._process = self._conn = None
        if process is None:
            return
        try:
            conn.send(None)
        except (OSError, ValueError):
            pass
        process.join(timeout=5)
        if process.is_alive():
            process.kill()
            process.join()
        conn.close()

    def _start(self) -> None:
        mine, theirs = self._context.Pipe()
        process = self._context.Process(target=_serve, args=(theirs, *self._target))
        process.start()
        theirs.close()
        with self._lock:
            self._process, self._conn = process, mine
        # Imports and the initializer are not the item's, but are bounded all the same:
        # a child that cannot start would otherwise be waited on forever.
        if self._answer() != _READY:
            raise Stopped("worker process started wrong")

    def _answer(self) -> Any:
        deadline = time.monotonic() + self._limits.timeout_s
        while True:
            if self._conn.poll(POLL_SECONDS):
                try:
                    return self._conn.recv()
                except EOFError:
                    code = self._kill()
                    raise Stopped(f"process died with exit code {code}") from None
            used = _rss_mb(self._process.pid)
            if used > self._limits.memory_mb:
                self._kill()
                raise Stopped(
                    f"killed after using {used:,} MB, over its "
                    f"{self._limits.memory_mb:,} MB memory limit"
                )
            if time.monotonic() > deadline:
                self._kill()
                raise Stopped(f"killed after {self._limits.timeout_s:g} s, its time limit")

    def _kill(self) -> Optional[int]:
        with self._lock:
            process, conn = self._process, self._conn
            self._process = self._conn = None
        process.kill()
        process.join()
        conn.close()
        return process.exitcode


def _serve(conn, worker, initializer, initargs) -> None:
    if initializer is not None:
        initializer(*initargs)
    conn.send(_READY)
    while True:
        try:
            call = conn.recv()
        except EOFError:
            return
        if call is None:
            return
        try:
            conn.send((True, worker(*call)))
        except Exception as exc:  # noqa: BLE001 - sent to the parent as the item's
            conn.send((False, error_text(exc)))


def _rss_mb(pid: int) -> int:
    """Resident memory of `pid` and its descendants, in MB; 0 where /proc cannot say."""
    total = 0
    for each in _tree(pid):
        try:
            with open(f"/proc/{each}/statm") as handle:
                total += int(handle.read().split()[1]) * _PAGE_BYTES
        except (OSError, ValueError, IndexError):
            continue
    return total // 2**20


def _tree(pid: int) -> List[int]:
    found, pending = [], [pid]
    while pending:
        each = pending.pop()
        found.append(each)
        try:
            tasks = os.listdir(f"/proc/{each}/task")
        except OSError:
            continue
        for task in tasks:
            try:
                with open(f"/proc/{each}/task/{task}/children") as handle:
                    pending.extend(int(child) for child in handle.read().split())
            except (OSError, ValueError):
                continue
    return found


__all__ = ["Limits", "Stopped", "bounded_map"]
