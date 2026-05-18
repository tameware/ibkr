"""Isolated ledgers directories for unit tests."""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any, Dict, Optional


class TemporaryLedgersDir:
    """Create a temp ``ledgers_dir`` and remove it (and all ledger files) on cleanup."""

    def __init__(self) -> None:
        self._tmp: Optional[tempfile.TemporaryDirectory[str]] = None

    @property
    def path(self) -> str:
        if self._tmp is None:
            raise RuntimeError("TemporaryLedgersDir.start() was not called")
        return self._tmp.name

    def start(self) -> str:
        if self._tmp is None:
            self._tmp = tempfile.TemporaryDirectory()
        return self._tmp.name

    def stop(self) -> None:
        if self._tmp is not None:
            self._tmp.cleanup()
            self._tmp = None

    def __enter__(self) -> str:
        return self.start()

    def __exit__(self, *_exc: object) -> None:
        self.stop()


def with_ledgers_dir(config: Dict[str, Any], ledgers_dir: str) -> Dict[str, Any]:
    """Return a copy of ``config`` with ``ledgers_dir`` set."""
    out = dict(config)
    out["ledgers_dir"] = ledgers_dir
    return out


def register_ledgers_dir_cleanup(test_case: Any, ledgers: TemporaryLedgersDir) -> str:
    """Start a temp ledgers dir and register unittest cleanup. Returns the path."""
    path = ledgers.start()
    test_case.addCleanup(ledgers.stop)
    return path


def cleanup_ledgers_dir(ledgers_dir: str | Path) -> None:
    """Remove all ``*.json`` ledger files under ``ledgers_dir`` (ignore errors)."""
    root = Path(ledgers_dir)
    if not root.is_dir():
        return
    for path in root.glob("*.json"):
        path.unlink(missing_ok=True)


def init_test_ledgers_dir(test_case: Any) -> str:
    """Start a temp ledgers directory and register cleanup on ``test_case``."""
    ledgers = TemporaryLedgersDir()
    path = register_ledgers_dir_cleanup(test_case, ledgers)
    test_case._test_ledgers = ledgers
    test_case.addCleanup(lambda: cleanup_ledgers_dir(path))
    return path
