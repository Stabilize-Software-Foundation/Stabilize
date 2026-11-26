"""Log records captured by a handler of our own, so a test does not depend on pytest's logging plugin."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager


class _Collect(logging.Handler):
    def __init__(self, level: int) -> None:
        super().__init__(level)
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


@contextmanager
def captured(level: int = logging.WARNING, name: str = "stabilize") -> Iterator[list[logging.LogRecord]]:
    logger = logging.getLogger(name)
    handler = _Collect(level)
    previous = logger.level
    logger.addHandler(handler)
    if logger.getEffectiveLevel() > level:
        logger.setLevel(level)
    try:
        yield handler.records
    finally:
        logger.removeHandler(handler)
        logger.setLevel(previous)
