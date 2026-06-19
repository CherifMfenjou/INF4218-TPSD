"""Tests unitaires pour les horloges de Lamport."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.coordination.lamport import LamportClock, LamportTimestamp


class TestLamportClock:
    def test_local_events_increment(self):
        clock = LamportClock(process_id=1)
        ts1 = clock.tick("e1")
        ts2 = clock.tick("e2")
        assert ts1.time < ts2.time
        assert ts1.process_id == 1

    def test_send_increments(self):
        clock = LamportClock(process_id=0)
        ts = clock.send_event("send")
        assert ts == 1
        assert clock.current == 1

    def test_receive_updates_clock(self):
        clock = LamportClock(process_id=2)
        clock.tick("local")
        ts = clock.receive_event(10, "receive")
        assert clock.current == 11
        assert ts.time == 11

    def test_happens_before_ordering(self):
        """Si a → b (a envoyé avant b reçu), C(a) < C(b)."""
        sender = LamportClock(process_id=0)
        receiver = LamportClock(process_id=1)

        send_ts = sender.send_event("msg_send")
        recv_ts = receiver.receive_event(send_ts, "msg_recv")

        assert send_ts < recv_ts.time

    def test_tie_breaker_by_process_id(self):
        ts_a = LamportTimestamp(5, 1)
        ts_b = LamportTimestamp(5, 3)
        assert ts_a < ts_b

    def test_event_log(self):
        clock = LamportClock(process_id=0)
        clock.tick("a")
        clock.tick("b")
        log = clock.get_event_log()
        assert len(log) == 2
        assert log[0][0] == "a"
        assert log[1][0] == "b"

    def test_round_consistency(self):
        """Les horloges garantissent l'ordonnancement des rounds."""
        clocks = [LamportClock(i) for i in range(3)]
        events = []

        for i, clock in enumerate(clocks):
            ts = clock.send_event(f"round_event_{i}")
            events.append((ts, i))

        for ts, sender_id in events:
            for j, clock in enumerate(clocks):
                if j != sender_id:
                    clock.receive_event(ts, f"recv_from_{sender_id}")

        for clock in clocks:
            assert clock.current >= len(clocks)
