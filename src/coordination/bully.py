"""Algorithme d'élection Bully (chapitre 5.4.1 - Van Steen & Tanenbaum).

Le processus avec le plus grand identifiant devient coordinateur.
1. Pk envoie ELECTION aux processus d'ID supérieur
2. Si aucune réponse → Pk gagne
3. Si réponse OK → le plus grand ID prend le relais
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class ElectionState(Enum):
    IDLE = "idle"
    ELECTION_IN_PROGRESS = "election"
    COORDINATOR = "coordinator"


@dataclass
class ProcessInfo:
    process_id: int
    address: str
    alive: bool = True


@dataclass
class BullyElection:
    """Implémentation de l'algorithme Bully pour l'élection de leader."""

    process_id: int
    processes: Dict[int, ProcessInfo] = field(default_factory=dict)
    state: ElectionState = ElectionState.IDLE
    coordinator_id: Optional[int] = None
    election_timeout: float = 2.0
    _responses_received: Set[int] = field(default_factory=set)
    _lock: threading.Lock = field(default_factory=threading.Lock)
    _on_coordinator_elected: Optional[Callable[[int], None]] = None

    def __post_init__(self) -> None:
        if self.process_id not in self.processes:
            self.processes[self.process_id] = ProcessInfo(
                process_id=self.process_id,
                address="localhost",
            )

    def set_send_election(self, callback: Callable[[int, List[int]], List[int]]) -> None:
        """Définit le callback d'envoi de messages ELECTION."""
        self._send_election = callback

    def set_send_coordinator(self, callback: Callable[[int, List[int]], None]) -> None:
        """Définit le callback d'annonce COORDINATOR."""
        self._send_coordinator = callback

    def set_on_coordinator_elected(self, callback: Callable[[int], None]) -> None:
        """Callback appelé quand un coordinateur est élu."""
        self._on_coordinator_elected = callback

    def get_higher_processes(self) -> List[int]:
        """Retourne les IDs supérieurs au processus courant."""
        return sorted(
            pid for pid, info in self.processes.items()
            if pid > self.process_id and info.alive
        )

    def get_all_alive(self) -> List[int]:
        """Retourne tous les processus vivants."""
        return sorted(pid for pid, info in self.processes.items() if info.alive)

    def start_election(self) -> Optional[int]:
        """Démarre une élection Bully."""
        with self._lock:
            self.state = ElectionState.ELECTION_IN_PROGRESS
            self._responses_received.clear()

        higher = self.get_higher_processes()
        logger.info("Process %d starting election, higher processes: %s", self.process_id, higher)

        if not higher:
            return self._become_coordinator()

        responders = self._send_election(self.process_id, higher)
        self._responses_received.update(responders)

        if not responders:
            return self._become_coordinator()

        deadline = time.time() + self.election_timeout
        while time.time() < deadline:
            with self._lock:
                if self.coordinator_id is not None:
                    return self.coordinator_id
            time.sleep(0.05)

        return self.coordinator_id

    def handle_election_message(self, sender_id: int) -> bool:
        """Reçoit un message ELECTION d'un processus inférieur."""
        if sender_id < self.process_id:
            logger.info("Process %d received ELECTION from %d, responding OK", self.process_id, sender_id)
            if self.state != ElectionState.ELECTION_IN_PROGRESS:
                threading.Thread(target=self.start_election, daemon=True).start()
            return True
        return False

    def handle_coordinator_message(self, coordinator_id: int) -> None:
        """Reçoit l'annonce d'un nouveau coordinateur."""
        with self._lock:
            self.coordinator_id = coordinator_id
            self.state = ElectionState.IDLE
        logger.info("Process %d acknowledges coordinator %d", self.process_id, coordinator_id)

    def _become_coordinator(self) -> int:
        """Ce processus devient le coordinateur."""
        with self._lock:
            self.coordinator_id = self.process_id
            self.state = ElectionState.COORDINATOR

        alive = self.get_all_alive()
        self._send_coordinator(self.process_id, alive)
        logger.info("Process %d elected as coordinator", self.process_id)

        if self._on_coordinator_elected:
            self._on_coordinator_elected(self.process_id)

        return self.process_id

    def mark_process_dead(self, process_id: int) -> None:
        """Marque un processus comme mort (simulation de panne)."""
        if process_id in self.processes:
            self.processes[process_id].alive = False
            if self.coordinator_id == process_id:
                self.coordinator_id = None
                self.state = ElectionState.IDLE

    def mark_coordinator_dead(self) -> bool:
        """Marque le coordinateur actuel comme mort et lance une élection."""
        if self.coordinator_id is None:
            return False
        dead_id = self.coordinator_id
        self.mark_process_dead(dead_id)
        logger.warning("Coordinator %d is dead, starting election", dead_id)
        self.start_election()
        return True

    def detect_coordinator_failure(self) -> bool:
        """Détecte la panne du coordinateur (timeout) et lance une élection."""
        if self.coordinator_id is None:
            self.start_election()
            return True
        coord = self.processes.get(self.coordinator_id)
        if coord is None or not coord.alive:
            return self.mark_coordinator_dead()
        return False

    def mark_process_alive(self, process_id: int, address: str = "localhost") -> None:
        """Enregistre ou réactive un processus."""
        self.processes[process_id] = ProcessInfo(
            process_id=process_id,
            address=address,
            alive=True,
        )

    def is_coordinator(self) -> bool:
        return self.coordinator_id == self.process_id
