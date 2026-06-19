"""Tests unitaires pour l'algorithme Bully."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.coordination.bully import BullyElection, ElectionState


class TestBullyElection:
    def _make_election(self, process_id, all_ids):
        bully = BullyElection(process_id=process_id)
        for pid in all_ids:
            bully.mark_process_alive(pid, f"host:{50050 + pid}")
        bully.set_send_election(lambda s, t: [])
        bully.set_send_coordinator(lambda c, t: None)
        return bully

    def test_highest_id_becomes_coordinator(self):
        bully = self._make_election(3, [0, 1, 2, 3])
        coord = bully.start_election()
        assert coord == 3
        assert bully.is_coordinator()

    def test_lower_id_defers_to_higher(self):
        bully = self._make_election(1, [0, 1, 2, 3])
        responses = []

        def mock_send(sender, targets):
            responses.extend(targets)
            return [2, 3]

        bully.set_send_election(mock_send)
        bully.start_election()
        assert not bully.is_coordinator()
        assert 2 in responses
        assert 3 in responses

    def test_election_on_coordinator_failure(self):
        bully = self._make_election(0, [0, 1, 2])
        bully.coordinator_id = 2
        bully.mark_coordinator_dead()
        assert bully.coordinator_id == 0
        assert bully.is_coordinator()

    def test_handle_election_from_lower(self):
        bully = self._make_election(5, [0, 1, 5])
        result = bully.handle_election_message(sender_id=1)
        assert result is True

    def test_handle_election_from_higher(self):
        bully = self._make_election(1, [0, 1, 5])
        result = bully.handle_election_message(sender_id=5)
        assert result is False

    def test_coordinator_announcement(self):
        bully = self._make_election(2, [0, 1, 2, 3])
        bully.handle_coordinator_message(3)
        assert bully.coordinator_id == 3

    def test_single_process_election(self):
        bully = self._make_election(0, [0])
        coord = bully.start_election()
        assert coord == 0
        assert bully.is_coordinator()
