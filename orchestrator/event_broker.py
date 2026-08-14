# -*- coding: utf-8 -*-
from __future__ import annotations

import threading
from collections import deque
from copy import deepcopy
from typing import Any


class ReplaySequenceGapError(RuntimeError):
    """Raised when bounded replay cannot provide a contiguous sequence."""


class ReplayEventBroker:
    """Multi-subscriber task event stream with bounded in-memory replay."""

    def __init__(self, max_events: int = 2000, initial_sequence: int = 0):
        if max_events < 1:
            raise ValueError("max_events must be positive")
        if initial_sequence < 0:
            raise ValueError("initial_sequence must be non-negative")
        self._condition = threading.Condition()
        self._events: deque[dict[str, Any]] = deque(maxlen=max_events)
        self._sequence = initial_sequence
        self._active_publishers = 0
        self._accepting = True
        self._closed = False

    def put(self, event: dict[str, Any]) -> None:
        self.put_many((event,))

    def put_many(self, events: list[dict[str, Any]] | tuple[dict[str, Any], ...]) -> int:
        """Publish a batch under one admission and notification boundary."""
        if not events:
            return 0
        with self._condition:
            if not self._accepting or self._closed:
                raise RuntimeError("event broker is closing")
            self._active_publishers += 1
        try:
            pending = [deepcopy(event) for event in events]
            published = 0
            with self._condition:
                for stored in pending:
                    durable_sequence = int(stored.get("sequence") or 0)
                    if durable_sequence > 0:
                        if durable_sequence <= self._sequence:
                            # Duplicate/stale durable events must not move the live
                            # cursor backwards or consume a new sequence number.
                            continue
                        if durable_sequence > self._sequence + 1:
                            stored["_gap_after"] = self._sequence
                        self._sequence = durable_sequence
                    else:
                        self._sequence += 1
                    stored["_seq"] = self._sequence
                    self._events.append(stored)
                    published += 1
                if published:
                    self._condition.notify_all()
            return published
        finally:
            with self._condition:
                self._active_publishers -= 1
                self._condition.notify_all()

    def events_after(
        self,
        sequence: int,
        *,
        require_contiguous: bool = False,
    ) -> list[dict[str, Any]]:
        with self._condition:
            available = [
                deepcopy(event)
                for event in self._events
                if int(event.get("_seq", 0)) > sequence
            ]
            if (
                require_contiguous
                and available
                and int(available[0].get("_seq", 0)) > sequence + 1
            ):
                raise ReplaySequenceGapError(
                    f"replay gap after sequence {sequence}; "
                    f"oldest available is {available[0].get('_seq')}"
                )
            return available

    def wait_after(
        self,
        sequence: int,
        timeout: float = 15.0,
        *,
        require_contiguous: bool = False,
    ) -> list[dict[str, Any]]:
        with self._condition:
            available = [
                event
                for event in self._events
                if int(event.get("_seq", 0)) > sequence
            ]
            if not available:
                self._condition.wait_for(
                    lambda: self._closed
                    or any(
                        int(event.get("_seq", 0)) > sequence
                        for event in self._events
                    ),
                    timeout=timeout,
                )
            result = [
                deepcopy(event)
                for event in self._events
                if int(event.get("_seq", 0)) > sequence
            ]
            if (
                require_contiguous
                and result
                and int(result[0].get("_seq", 0)) > sequence + 1
            ):
                raise ReplaySequenceGapError(
                    f"replay gap after sequence {sequence}; "
                    f"oldest available is {result[0].get('_seq')}"
                )
            return result

    def has_gap_after(self, sequence: int) -> bool:
        with self._condition:
            available = [
                int(event.get("_seq", 0))
                for event in self._events
                if int(event.get("_seq", 0)) > sequence
            ]
            return bool(available and available[0] > sequence + 1)

    @property
    def latest_sequence(self) -> int:
        with self._condition:
            return self._sequence

    @property
    def closed(self) -> bool:
        with self._condition:
            return self._closed

    def stop_accepting(self) -> None:
        with self._condition:
            self._accepting = False
            self._condition.notify_all()

    def drain(self, timeout: float = 10.0) -> bool:
        """Wait until publishers already admitted by ``put`` have finished."""
        if timeout < 0:
            raise ValueError("timeout must be non-negative")
        with self._condition:
            return self._condition.wait_for(
                lambda: self._active_publishers == 0,
                timeout=timeout,
            )

    def close(self, timeout: float = 10.0) -> bool:
        """Stop intake, drain active publishers, and wake waiting consumers."""
        with self._condition:
            if self._closed:
                return True
        self.stop_accepting()
        drained = self.drain(timeout)
        with self._condition:
            self._closed = True
            self._condition.notify_all()
        return drained
