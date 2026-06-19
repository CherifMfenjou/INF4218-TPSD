"""Tolérance aux pannes : checkpointing et reprise (chapitre 8.6.2).

Sauvegarde périodique de l'état du système pour permettre
la reprise après une panne sans tout recommencer.
"""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class SystemState:
    """État sauvegardable du système distribué."""

    current_round: int = 0
    coordinator_id: Optional[int] = None
    lamport_clock: int = 0
    registered_clients: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    model_weights: List[float] = field(default_factory=list)
    round_history: List[Dict[str, Any]] = field(default_factory=list)
    timestamp: float = field(default_factory=time.time)


class CheckpointManager:
    """Gestionnaire de checkpoints pour la tolérance aux pannes."""

    def __init__(self, checkpoint_dir: str = "checkpoints") -> None:
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self._last_checkpoint: Optional[SystemState] = None

    def _checkpoint_path(self, name: str) -> Path:
        return self.checkpoint_dir / f"{name}.json"

    def save(self, state: SystemState, name: str = "latest") -> str:
        """Sauvegarde un checkpoint sur disque."""
        state.timestamp = time.time()
        path = self._checkpoint_path(name)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(asdict(state), f, indent=2)
        self._last_checkpoint = state
        logger.info("Checkpoint saved: %s (round %d)", path, state.current_round)
        return str(path)

    def load(self, name: str = "latest") -> Optional[SystemState]:
        """Charge un checkpoint depuis le disque."""
        path = self._checkpoint_path(name)
        if not path.exists():
            logger.warning("Checkpoint not found: %s", path)
            return None

        with open(path, encoding="utf-8") as f:
            data = json.load(f)

        state = SystemState(**data)
        self._last_checkpoint = state
        logger.info("Checkpoint loaded: %s (round %d)", path, state.current_round)
        return state

    def list_checkpoints(self) -> List[str]:
        """Liste tous les checkpoints disponibles."""
        return [p.stem for p in self.checkpoint_dir.glob("*.json")]

    def should_checkpoint(self, round_number: int, interval: int = 3) -> bool:
        """Détermine si un checkpoint doit être créé."""
        return round_number > 0 and round_number % interval == 0


class RecoveryManager:
    """Gestionnaire de reprise après panne."""

    def __init__(self, checkpoint_manager: CheckpointManager, timeout: float = 5.0) -> None:
        self.checkpoint_manager = checkpoint_manager
        self.timeout = timeout
        self._failure_count = 0

    def detect_failure(self, last_heartbeat: float) -> bool:
        """Détecte une panne par timeout de heartbeat."""
        elapsed = time.time() - last_heartbeat
        if elapsed > self.timeout:
            self._failure_count += 1
            logger.warning("Failure detected: no heartbeat for %.1fs", elapsed)
            return True
        return False

    def recover(self, checkpoint_name: str = "latest") -> Optional[SystemState]:
        """Reprend depuis le dernier checkpoint."""
        state = self.checkpoint_manager.load(checkpoint_name)
        if state:
            logger.info(
                "Recovery successful from round %d, coordinator was %s",
                state.current_round,
                state.coordinator_id,
            )
        return state

    @property
    def failure_count(self) -> int:
        return self._failure_count
