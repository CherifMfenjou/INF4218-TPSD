"""Serveur coordinateur d'apprentissage fédéré (architecture multi-niveaux).

Couches :
- Application : logique FL (rounds, agrégation)
- Middleware : Lamport, Bully, mutex, nommage
- Réseau : gRPC (unary + streaming)
"""

from __future__ import annotations

import logging
import sys
import threading
import time
from concurrent import futures
from pathlib import Path
from typing import Dict, Iterator, List, Optional

import grpc
import numpy as np

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.communication import federation_pb2, federation_pb2_grpc
from src.coordination.bully import BullyElection
from src.coordination.lamport import LamportClock
from src.coordination.mutual_exclusion import MutexRequest, RicartAgrawalaMutex
from src.fault_tolerance.checkpoint import CheckpointManager, RecoveryManager, SystemState
from src.naming.attribute import AttributeNamingService
from src.naming.flat import FlatNamingService
from src.naming.structured import StructuredNamingService

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

CHUNK_SIZE = 4096
STREAMING_THRESHOLD = 8192


class FederationCoordinatorServicer(federation_pb2_grpc.FederationCoordinatorServicer):
    """Implémentation du service gRPC coordinateur."""

    def __init__(self, process_id: int = 0, port: int = 50051, checkpoint_dir: str = "checkpoints"):
        self.process_id = process_id
        self.port = port
        self.address = f"localhost:{port}"

        self.clock = LamportClock(process_id)
        self.flat_naming = FlatNamingService()
        self.structured_naming = StructuredNamingService()
        self.attribute_naming = AttributeNamingService()

        self.checkpoint_mgr = CheckpointManager(checkpoint_dir)
        self.recovery_mgr = RecoveryManager(self.checkpoint_mgr)

        self.current_round = 0
        self.model_weights: List[float] = [0.0] * 10
        self.client_gradients: Dict[str, List[float]] = {}
        self.round_events: List[dict] = []
        self._lock = threading.Lock()

        self.bully = BullyElection(process_id=process_id)
        self.bully.mark_process_alive(process_id, self.address)
        self.bully.coordinator_id = process_id
        self._setup_bully_callbacks()

        self.mutex = RicartAgrawalaMutex(process_id, self.clock)
        self._client_process_map: Dict[str, int] = {}
        self._process_counter = 1

        self._recover_state()

    def _setup_bully_callbacks(self) -> None:
        def send_election(sender_id: int, targets: List[int]) -> List[int]:
            return []

        def send_coordinator(coord_id: int, targets: List[int]) -> None:
            logger.info("Announcing coordinator %d to %s", coord_id, targets)

        self.bully.set_send_election(send_election)
        self.bully.set_send_coordinator(send_coordinator)

    def _recover_state(self) -> None:
        state = self.recovery_mgr.recover()
        if state:
            self.current_round = state.current_round
            self.model_weights = state.model_weights or [0.0] * 10
            if state.coordinator_id is not None:
                self.bully.coordinator_id = state.coordinator_id
            self.clock._counter = state.lamport_clock
            logger.info("Recovered from checkpoint at round %d", self.current_round)

    def _save_checkpoint(self) -> None:
        with self._lock:
            state = SystemState(
                current_round=self.current_round,
                coordinator_id=self.bully.coordinator_id,
                lamport_clock=self.clock.current,
                model_weights=list(self.model_weights),
                round_history=list(self.round_events[-20:]),
            )
        self.checkpoint_mgr.save(state)

    def RegisterClient(self, request, context):
        self.clock.tick("register")

        flat_id = request.flat_id or self.flat_naming.generate_id()
        address = request.address or "unknown"

        self.flat_naming.register(address, flat_id=flat_id)
        if request.structured_path:
            self.structured_naming.register(request.structured_path, address)
        if request.attributes:
            self.attribute_naming.register(flat_id, address, dict(request.attributes))

        with self._lock:
            proc_id = request.process_id if request.process_id > 0 else self._process_counter
            self._process_counter = max(self._process_counter, proc_id + 1)
            self._client_process_map[flat_id] = proc_id
            self.bully.mark_process_alive(proc_id, address)
            self.mutex.set_processes(list(self.bully.get_all_alive()))

        logger.info("Client registered: %s (process %d) at %s", flat_id, proc_id, address)
        return federation_pb2.RegisterResponse(
            success=True,
            assigned_id=proc_id,
            lamport_timestamp=self.clock.current,
            current_round=self.current_round,
        )

    def Heartbeat(self, request, context):
        self.clock.receive_event(request.lamport_timestamp, "heartbeat")
        return federation_pb2.HeartbeatResponse(
            alive=True,
            coordinator_id=self.bully.coordinator_id or self.process_id,
            lamport_timestamp=self.clock.current,
            current_round=self.current_round,
        )

    def RequestMutualExclusion(self, request, context):
        self.clock.receive_event(request.lamport_timestamp, "mutex_req_recv")
        req = MutexRequest(
            timestamp=request.lamport_timestamp,
            process_id=request.process_id,
            resource=request.resource,
        )
        self.mutex.handle_request(req)

        my_req = MutexRequest(
            timestamp=self.clock.send_event("mutex_req"),
            process_id=self.process_id,
            resource=request.resource,
        )
        granted = True
        return federation_pb2.MutexResponse(granted=granted, lamport_timestamp=self.clock.current)

    def ReleaseMutualExclusion(self, request, context):
        self.clock.receive_event(request.lamport_timestamp, "mutex_release")
        self.mutex.release_access(request.resource)
        return federation_pb2.MutexAck(success=True)

    def ResolveName(self, request, context):
        if request.flat_id:
            entity = self.flat_naming.resolve(request.flat_id)
            if entity:
                return federation_pb2.NameResolveResponse(
                    found=True, address=entity.address, attributes=entity.metadata
                )
        if request.structured_path:
            node = self.structured_naming.resolve(request.structured_path)
            if node:
                return federation_pb2.NameResolveResponse(
                    found=True, address=node.address, attributes=node.metadata
                )
        return federation_pb2.NameResolveResponse(found=False)

    def QueryByAttributes(self, request, context):
        results = self.attribute_naming.query(dict(request.attributes))
        return federation_pb2.AttributeQueryResponse(
            client_ids=[e.entity_id for e in results],
            addresses=[e.address for e in results],
        )

    def UploadGradients(self, request_iterator, context):
        """Réception streaming des gradients (gros volumes)."""
        chunks = []
        client_id = ""
        round_num = 0
        last_ts = 0

        for chunk in request_iterator:
            self.clock.receive_event(chunk.lamport_timestamp, f"gradient_chunk_{chunk.chunk_index}")
            client_id = chunk.client_id
            round_num = chunk.round
            last_ts = chunk.lamport_timestamp
            chunks.append(chunk.data)

        full_data = b"".join(chunks)
        gradients = np.frombuffer(full_data, dtype=np.float64).tolist() if full_data else []

        with self._lock:
            self.client_gradients[client_id] = gradients
            self.round_events.append({
                "round": round_num,
                "client": client_id,
                "lamport_ts": self.clock.current,
                "event": "gradient_received",
            })

        self.clock.tick("gradient_processed")
        logger.info("Received gradients from %s for round %d (%d bytes)", client_id, round_num, len(full_data))

        return federation_pb2.UploadResponse(
            success=True,
            round=round_num,
            lamport_timestamp=self.clock.current,
            message=f"Gradients received ({len(gradients)} values)",
        )

    def DownloadModel(self, request, context):
        """Envoi streaming du modèle (gros volumes)."""
        self.clock.tick("model_download_start")
        weights = np.array(self.model_weights, dtype=np.float64)
        data = weights.tobytes()
        total_chunks = max(1, (len(data) + CHUNK_SIZE - 1) // CHUNK_SIZE)

        for i in range(total_chunks):
            start = i * CHUNK_SIZE
            end = min(start + CHUNK_SIZE, len(data))
            ts = self.clock.send_event(f"model_chunk_{i}")
            yield federation_pb2.ModelChunk(
                data=data[start:end],
                chunk_index=i,
                total_chunks=total_chunks,
                lamport_timestamp=ts,
            )

    def SendElection(self, request, context):
        if request.election_type == "ELECTION":
            respond = self.bully.handle_election_message(request.sender_id)
            return federation_pb2.ElectionResponse(respond_ok=respond, responder_id=self.process_id)
        return federation_pb2.ElectionResponse(respond_ok=False, responder_id=self.process_id)

    def AnnounceCoordinator(self, request, context):
        self.bully.handle_coordinator_message(request.coordinator_id)
        return federation_pb2.CoordinatorAck(acknowledged=True)

    def aggregate_round(self) -> None:
        """Agrège les gradients et avance le round (section critique)."""
        if not self.mutex.request_access("aggregation"):
            logger.error("Failed to acquire mutex for aggregation")
            return

        try:
            with self._lock:
                if self.client_gradients:
                    all_grads = list(self.client_gradients.values())
                    aggregated = np.mean(all_grads, axis=0).tolist()
                    self.model_weights = aggregated
                    self.client_gradients.clear()

                self.current_round += 1
                ts = self.clock.tick(f"round_{self.current_round}_complete")
                self.round_events.append({
                    "round": self.current_round,
                    "lamport_ts": ts.time,
                    "event": "aggregation_complete",
                })

            logger.info("Round %d aggregated (Lamport ts=%d)", self.current_round, self.clock.current)

            if self.checkpoint_mgr.should_checkpoint(self.current_round):
                self._save_checkpoint()
        finally:
            self.mutex.release_access("aggregation")


def serve(process_id: int = 0, port: int = 50051, checkpoint_dir: str = "checkpoints") -> None:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    servicer = FederationCoordinatorServicer(process_id, port, checkpoint_dir)
    federation_pb2_grpc.add_FederationCoordinatorServicer_to_server(servicer, server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    logger.info("Federation coordinator started on port %d (process_id=%d)", port, process_id)
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        servicer._save_checkpoint()
        server.stop(0)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Federation Coordinator Server")
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--process-id", type=int, default=0)
    parser.add_argument("--checkpoint-dir", default="checkpoints")
    args = parser.parse_args()
    serve(args.process_id, args.port, args.checkpoint_dir)
