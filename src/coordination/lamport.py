"""Horloges logiques de Lamport (chapitre 5.2.1 - Van Steen & Tanenbaum).

Chaque processus maintient un compteur local Ci :
1. Avant un événement : Ci ← Ci + 1
2. À l'envoi d'un message : ts(m) = Ci
3. À la réception : Cj ← max(Cj, ts(m)), puis Ci ← Ci + 1
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Tuple


@dataclass(order=True)
class LamportTimestamp:
    """Horodatage Lamport avec tie-breaker par identifiant de processus."""

    time: int
    process_id: int


class LamportClock:
    """Horloge logique de Lamport pour un processus."""

    def __init__(self, process_id: int = 0) -> None:
        self.process_id = process_id
        self._counter = 0
        self._event_log: List[Tuple[str, LamportTimestamp]] = []

    @property
    def current(self) -> int:
        return self._counter

    def tick(self, event_name: str = "local") -> LamportTimestamp:
        """Incrémente l'horloge avant un événement local."""
        self._counter += 1
        ts = LamportTimestamp(self._counter, self.process_id)
        self._event_log.append((event_name, ts))
        return ts

    def send_event(self, event_name: str = "send") -> int:
        """Prépare un timestamp pour l'envoi d'un message."""
        self._counter += 1
        ts = LamportTimestamp(self._counter, self.process_id)
        self._event_log.append((event_name, ts))
        return self._counter

    def receive_event(self, received_ts: int, event_name: str = "receive") -> LamportTimestamp:
        """Met à jour l'horloge à la réception d'un message."""
        self._counter = max(self._counter, received_ts) + 1
        ts = LamportTimestamp(self._counter, self.process_id)
        self._event_log.append((event_name, ts))
        return ts

    def compare(self, other_time: int, other_process: int) -> int:
        """Compare deux timestamps (-1, 0, 1)."""
        self_ts = LamportTimestamp(self._counter, self.process_id)
        other_ts = LamportTimestamp(other_time, other_process)
        if self_ts < other_ts:
            return -1
        if self_ts > other_ts:
            return 1
        return 0

    def get_event_log(self) -> List[Tuple[str, LamportTimestamp]]:
        """Retourne le journal des événements horodatés."""
        return list(self._event_log)

    def reset(self) -> None:
        """Réinitialise l'horloge."""
        self._counter = 0
        self._event_log.clear()
