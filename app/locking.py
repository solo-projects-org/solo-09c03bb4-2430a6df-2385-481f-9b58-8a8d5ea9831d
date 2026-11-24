"""进程间互斥锁（flock）：发布与净重批次落库各自使用，互不阻塞读票。"""

from __future__ import annotations

import contextlib
import fcntl
import os
import time
from pathlib import Path


class LockTimeout(RuntimeError):
    """等待文件锁超时。"""


class FileLock:
    def __init__(self, path: Path, timeout: float, timeout_message: str):
        self._path = path
        self._timeout = timeout
        self._timeout_message = timeout_message
        self._fd: int | None = None

    def __enter__(self) -> "FileLock":
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._fd = os.open(self._path, os.O_CREAT | os.O_RDWR, 0o644)
        deadline = time.monotonic() + max(self._timeout, 0.0)
        while True:
            try:
                fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return self
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise LockTimeout(self._timeout_message)
                time.sleep(0.1)

    def __exit__(self, *exc) -> None:
        assert self._fd is not None
        with contextlib.suppress(OSError):
            fcntl.flock(self._fd, fcntl.LOCK_UN)
        os.close(self._fd)
        self._fd = None
