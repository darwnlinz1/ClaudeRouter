import threading
import time

from orchestrator.event_broker import ReplayEventBroker


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
