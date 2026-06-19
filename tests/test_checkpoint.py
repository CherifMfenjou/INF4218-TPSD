"""Tests unitaires pour checkpoint et recovery."""

import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.fault_tolerance.checkpoint import CheckpointManager, RecoveryManager, SystemState


class TestCheckpointManager:
    def test_save_and_load(self, tmp_path):
        mgr = CheckpointManager(str(tmp_path))
        state = SystemState(
            current_round=5,
            coordinator_id=0,
            lamport_clock=42,
            model_weights=[1.0, 2.0, 3.0],
        )
        mgr.save(state)
        loaded = mgr.load()
        assert loaded is not None
        assert loaded.current_round == 5
        assert loaded.lamport_clock == 42
        assert loaded.model_weights == [1.0, 2.0, 3.0]

    def test_should_checkpoint_interval(self, tmp_path):
        mgr = CheckpointManager(str(tmp_path))
        assert not mgr.should_checkpoint(1, interval=3)
        assert mgr.should_checkpoint(3, interval=3)
        assert mgr.should_checkpoint(6, interval=3)

    def test_list_checkpoints(self, tmp_path):
        mgr = CheckpointManager(str(tmp_path))
        mgr.save(SystemState(current_round=1), "cp1")
        mgr.save(SystemState(current_round=2), "cp2")
        names = mgr.list_checkpoints()
        assert "cp1" in names
        assert "cp2" in names

    def test_load_nonexistent(self, tmp_path):
        mgr = CheckpointManager(str(tmp_path))
        assert mgr.load("missing") is None


class TestRecoveryManager:
    def test_detect_failure_by_timeout(self, tmp_path):
        mgr = CheckpointManager(str(tmp_path))
        recovery = RecoveryManager(mgr, timeout=0.1)
        old_time = time.time() - 1.0
        assert recovery.detect_failure(old_time)

    def test_no_failure_within_timeout(self, tmp_path):
        mgr = CheckpointManager(str(tmp_path))
        recovery = RecoveryManager(mgr, timeout=10.0)
        assert not recovery.detect_failure(time.time())

    def test_recover_from_checkpoint(self, tmp_path):
        mgr = CheckpointManager(str(tmp_path))
        mgr.save(SystemState(current_round=7, coordinator_id=2))
        recovery = RecoveryManager(mgr)
        state = recovery.recover()
        assert state.current_round == 7
        assert state.coordinator_id == 2
