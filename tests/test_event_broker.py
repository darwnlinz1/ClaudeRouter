import threading
import time

import pytest

from orchestrator.event_broker import (
    ReplayEventBroker,
    ReplaySequenceGapError,
)


def test_events_can_be_replayed_by_multiple_subscribers():
    broker = ReplayEventBroker()
    broker.put({"type": "status", "data": "planning"})
    broker.put({"type": "token", "text": "hello"})

    first_subscriber = broker.events_after(0)
    second_subscriber = broker.events_after(0)

    assert first_subscriber == second_subscriber
    assert [event["_seq"] for event in first_subscriber] == [1, 2]
    assert broker.events_after(1)[0]["text"] == "hello"


def test_wait_after_receives_event_published_while_disconnected():
    broker = ReplayEventBroker()

    def publish():
        time.sleep(0.02)
        broker.put({"type": "status", "data": "reviewing"})

    publisher = threading.Thread(target=publish)
    publisher.start()
    events = broker.wait_after(0, timeout=1)
    publisher.join()

    assert events[0]["data"] == "reviewing"
    assert events[0]["_seq"] == 1


def test_replay_history_is_bounded():
    broker = ReplayEventBroker(max_events=2)
    broker.put({"type": "status", "data": "one"})
    broker.put({"type": "status", "data": "two"})
    broker.put({"type": "status", "data": "three"})

    events = broker.events_after(0)

    assert [event["data"] for event in events] == ["two", "three"]
    assert [event["_seq"] for event in events] == [2, 3]


def test_resume_cursor_continues_from_durable_sequence():
    broker = ReplayEventBroker(initial_sequence=40)

    broker.put({"type": "status", "sequence": 41, "data": "resumed"})

    assert broker.latest_sequence == 41
    assert broker.events_after(40) == [
        {
            "type": "status",
            "sequence": 41,
            "data": "resumed",
            "_seq": 41,
        }
    ]


def test_stale_durable_event_does_not_advance_or_duplicate_cursor():
    broker = ReplayEventBroker(initial_sequence=5)

    broker.put({"type": "status", "sequence": 5, "data": "stale"})

    assert broker.latest_sequence == 5
    assert broker.events_after(0) == []


def test_batch_publish_gap_detection_and_closed_wait_behavior():
    broker = ReplayEventBroker(max_events=2)
    assert broker.put_many(
        (
            {"type": "status", "sequence": 1},
            {"type": "status", "sequence": 2},
            {"type": "done", "sequence": 3},
        )
    ) == 3

    with pytest.raises(ReplaySequenceGapError, match="oldest available is 2"):
        broker.events_after(0, require_contiguous=True)

    broker.close()
    started = time.monotonic()
    assert broker.wait_after(3, timeout=10) == []
    assert time.monotonic() - started < 0.2
    with pytest.raises(RuntimeError, match="closing"):
        broker.put_many(({"type": "status"},))
