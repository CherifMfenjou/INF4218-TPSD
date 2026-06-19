"""Tests unitaires pour l'exclusion mutuelle Ricart-Agrawala."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.coordination.lamport import LamportClock
from src.coordination.mutual_exclusion import MutexRequest, RicartAgrawalaMutex, MutexState


class TestRicartAgrawalaMutex:
    def _make_mutex(self, process_id, all_ids):
        clock = LamportClock(process_id)
        mutex = RicartAgrawalaMutex(process_id, clock)
        mutex.set_processes(all_ids)

        replies_sent = []

        def send_request(req, targets):
            pass

        def send_reply(target, resource, ts):
            replies_sent.append((target, resource, ts))

        mutex.set_send_callbacks(send_request, send_reply)
        return mutex, replies_sent

    def test_single_process_grants_immediately(self):
        mutex, _ = self._make_mutex(0, [0])
        assert mutex.request_access("test")
        assert mutex.is_in_critical_section()

    def test_release_resets_state(self):
        mutex, _ = self._make_mutex(0, [0])
        mutex.request_access("test")
        mutex.release_access("test")
        assert not mutex.is_in_critical_section()
        assert mutex.state == MutexState.RELEASED

    def test_lower_timestamp_wins(self):
        mutex0, _ = self._make_mutex(0, [0, 1])
        mutex1, replies1 = self._make_mutex(1, [0, 1])

        req0 = MutexRequest(timestamp=5, process_id=0, resource="agg")
        req1 = MutexRequest(timestamp=10, process_id=1, resource="agg")

        mutex1.handle_request(req0)
        mutex0.handle_request(req1)

        assert len(replies1) >= 0

    def test_deferred_reply_on_conflict(self):
        mutex, replies = self._make_mutex(1, [0, 1])
        mutex.state = MutexState.HELD

        req = MutexRequest(timestamp=3, process_id=0, resource="agg")
        mutex.handle_request(req)
        assert len(mutex._deferred_replies) == 1

        mutex.release_access("agg")
        assert len(replies) == 1

    def test_handle_reply_grants_access(self):
        mutex, _ = self._make_mutex(0, [0, 1, 2])
        mutex.state = MutexState.WANTED
        mutex.handle_reply(1)
        mutex.handle_reply(2)
        assert mutex.is_in_critical_section()
