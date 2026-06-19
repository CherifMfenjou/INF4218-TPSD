"""Exclusion mutuelle distribuée Ricart-Agrawala (chapitre 5.3.3).

Utilise les horloges de Lamport pour ordonner les demandes.
Le processus avec le plus petit timestamp accède à la ressource.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Dict, List, Optional, Set

from .lamport import LamportClock

logger = logging.getLogger(__name__)


class MutexState(Enum):
    RELEASED = "released"
    WANTED = "wanted"
    HELD = "held"


@dataclass(order=True)
class MutexRequest:
    """Demande d'exclusion mutuelle ordonnée par timestamp Lamport."""

    timestamp: int
    process_id: int
    resource: str = field(compare=False)


class RicartAgrawalaMutex:
    """Algorithme Ricart-Agrawala pour exclusion mutuelle distribuée."""

    def __init__(
        self,
        process_id: int,
        clock: Optional[LamportClock] = None,
        reply_timeout: float = 5.0,
    ) -> None:
        self.process_id = process_id
        self.clock = clock or LamportClock(process_id)
        self.reply_timeout = reply_timeout
        self.state = MutexState.RELEASED
        self._request_queue: List[MutexRequest] = []
        self._replies_received: Set[int] = set()
        self._deferred_replies: List[MutexRequest] = []
        self._known_processes: Set[int] = set()
        self._lock = threading.Lock()
        self._reply_event = threading.Event()
        self._send_request: Optional[Callable[[MutexRequest, List[int]], None]] = None
        self._send_reply: Optional[Callable[[int, str, int], None]] = None

    def set_processes(self, process_ids: List[int]) -> None:
        """Définit l'ensemble des processus du groupe."""
        self._known_processes = set(process_ids)

    def set_send_callbacks(
        self,
        send_request: Callable[[MutexRequest, List[int]], None],
        send_reply: Callable[[int, str, int], None],
    ) -> None:
        """Configure les callbacks de communication."""
        self._send_request = send_request
        self._send_reply = send_reply

    def request_access(self, resource: str = "aggregation") -> bool:
        """Demande l'accès à la ressource partagée (section critique)."""
        with self._lock:
            if self.state == MutexState.HELD:
                return True
            self.state = MutexState.WANTED

        ts = self.clock.send_event(f"mutex_request_{resource}")
        request = MutexRequest(timestamp=ts, process_id=self.process_id, resource=resource)

        with self._lock:
            self._request_queue.append(request)
            self._request_queue.sort()
            self._replies_received.clear()

        others = [p for p in self._known_processes if p != self.process_id]
        if not others:
            with self._lock:
                self.state = MutexState.HELD
            return True

        self._send_request(request, others)

        if self._reply_event.wait(timeout=self.reply_timeout):
            with self._lock:
                self.state = MutexState.HELD
            logger.info("Process %d entered critical section for %s", self.process_id, resource)
            return True

        logger.warning("Process %d mutex request timed out for %s", self.process_id, resource)
        with self._lock:
            self.state = MutexState.RELEASED
            self._request_queue = [r for r in self._request_queue if r.process_id != self.process_id]
        return False

    def release_access(self, resource: str = "aggregation") -> None:
        """Libère la ressource et envoie les OK différés."""
        with self._lock:
            self.state = MutexState.RELEASED
            self._request_queue = [r for r in self._request_queue if r.process_id != self.process_id]
            deferred = list(self._deferred_replies)
            self._deferred_replies.clear()

        for req in deferred:
            self._send_reply(req.process_id, req.resource, req.timestamp)

        logger.info("Process %d released critical section for %s", self.process_id, resource)

    def handle_request(self, request: MutexRequest) -> None:
        """Traite une demande d'exclusion mutuelle entrante."""
        self.clock.receive_event(request.timestamp, f"mutex_recv_{request.resource}")

        with self._lock:
            self._request_queue.append(request)
            self._request_queue.sort()

            if self.state == MutexState.HELD:
                self._deferred_replies.append(request)
                return

            if self.state == MutexState.WANTED:
                my_request = next(
                    (r for r in self._request_queue if r.process_id == self.process_id),
                    None,
                )
                if my_request and (
                    request.timestamp < my_request.timestamp
                    or (request.timestamp == my_request.timestamp and request.process_id < self.process_id)
                ):
                    self._send_reply(request.process_id, request.resource, request.timestamp)
                else:
                    self._deferred_replies.append(request)
                return

        self._send_reply(request.process_id, request.resource, request.timestamp)

    def handle_reply(self, sender_id: int) -> None:
        """Traite un message OK reçu."""
        with self._lock:
            self._replies_received.add(sender_id)
            expected = len(self._known_processes) - 1
            if len(self._replies_received) >= expected:
                self.state = MutexState.HELD
                self._reply_event.set()

    def is_in_critical_section(self) -> bool:
        return self.state == MutexState.HELD
