"""Client d'apprentissage fédéré."""

from __future__ import annotations

import logging
import sys
import time
from pathlib import Path
from typing import List, Optional

import grpc
import numpy as np

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.communication import federation_pb2, federation_pb2_grpc
from src.coordination.lamport import LamportClock
from src.naming.flat import FlatNamingService

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

CHUNK_SIZE = 4096
STREAMING_THRESHOLD = 8192


class FederatedClient:
    """Client participant à l'apprentissage fédéré."""

    def __init__(
        self,
        server_address: str = "localhost:50051",
        process_id: int = 1,
        dataset: str = "default",
        location: str = "local",
    ):
        self.server_address = server_address
        self.process_id = process_id
        self.dataset = dataset
        self.location = location

        self.clock = LamportClock(process_id)
        self.flat_id = FlatNamingService.generate_id()
        self.structured_path = f"/federation/clients/client{process_id}/models/local"
        self.channel: Optional[grpc.Channel] = None
        self.stub: Optional[federation_pb2_grpc.FederationCoordinatorStub] = None
        self.local_weights: List[float] = [0.0] * 10
        self.current_round = 0

    def connect(self) -> bool:
        """Établit la connexion gRPC au coordinateur."""
        try:
            self.channel = grpc.insecure_channel(self.server_address)
            self.stub = federation_pb2_grpc.FederationCoordinatorStub(self.channel)
            grpc.channel_ready_future(self.channel).result(timeout=5)
            return True
        except grpc.FutureTimeoutError:
            logger.error("Cannot connect to server at %s", self.server_address)
            return False

    def register(self) -> bool:
        """Enregistre le client auprès du coordinateur (3 types de nommage)."""
        if not self.stub:
            return False

        ts = self.clock.send_event("register")
        response = self.stub.RegisterClient(
            federation_pb2.RegisterRequest(
                flat_id=self.flat_id,
                structured_path=self.structured_path,
                attributes={"dataset": self.dataset, "location": self.location},
                process_id=self.process_id,
                address=f"client-{self.process_id}",
            ),
            timeout=5,
        )
        self.clock.receive_event(response.lamport_timestamp, "register_ack")
        self.current_round = response.current_round
        logger.info(
            "Registered as %s (process %d), round %d, Lamport ts=%d",
            self.flat_id, response.assigned_id, self.current_round, self.clock.current,
        )
        return response.success

    def heartbeat(self) -> bool:
        """Envoie un heartbeat au coordinateur."""
        if not self.stub:
            return False
        ts = self.clock.send_event("heartbeat")
        response = self.stub.Heartbeat(
            federation_pb2.HeartbeatRequest(client_id=self.flat_id, lamport_timestamp=ts),
            timeout=3,
        )
        self.clock.receive_event(response.lamport_timestamp, "heartbeat_ack")
        return response.alive

    def train_local(self, num_samples: int = 100) -> List[float]:
        """Simule un entraînement local et produit des gradients."""
        self.clock.tick("local_training")
        rng = np.random.default_rng(self.process_id + self.current_round)
        gradients = rng.normal(0.01, 0.1, size=10).tolist()
        self.local_weights = [w + g for w, g in zip(self.local_weights, gradients)]
        logger.info("Local training done (process %d, round %d)", self.process_id, self.current_round)
        return gradients

    def upload_gradients(self, gradients: List[float], use_streaming: bool = True) -> bool:
        """Envoie les gradients au serveur (streaming ou unary selon volume)."""
        if not self.stub:
            return False

        data = np.array(gradients, dtype=np.float64).tobytes()
        use_stream = use_streaming and len(data) > STREAMING_THRESHOLD

        if use_stream:
            return self._upload_streaming(gradients, data)
        return self._upload_unary(gradients, data)

    def _upload_streaming(self, gradients: List[float], data: bytes) -> bool:
        """Upload par streaming gRPC (gros volumes)."""
        total_chunks = max(1, (len(data) + CHUNK_SIZE - 1) // CHUNK_SIZE)

        def chunk_generator():
            for i in range(total_chunks):
                start = i * CHUNK_SIZE
                end = min(start + CHUNK_SIZE, len(data))
                ts = self.clock.send_event(f"upload_chunk_{i}")
                yield federation_pb2.GradientChunk(
                    client_id=self.flat_id,
                    round=self.current_round + 1,
                    data=data[start:end],
                    chunk_index=i,
                    total_chunks=total_chunks,
                    lamport_timestamp=ts,
                )

        start_time = time.perf_counter()
        response = self.stub.UploadGradients(chunk_generator(), timeout=30)
        elapsed = time.perf_counter() - start_time
        self.clock.receive_event(response.lamport_timestamp, "upload_ack")
        logger.info("Streaming upload: %d bytes in %.3fs", len(data), elapsed)
        return response.success

    def _upload_unary(self, gradients: List[float], data: bytes) -> bool:
        """Upload par message unique (petits volumes)."""
        ts = self.clock.send_event("upload_unary")

        def single_chunk():
            yield federation_pb2.GradientChunk(
                client_id=self.flat_id,
                round=self.current_round + 1,
                data=data,
                chunk_index=0,
                total_chunks=1,
                lamport_timestamp=ts,
            )

        start_time = time.perf_counter()
        response = self.stub.UploadGradients(single_chunk(), timeout=10)
        elapsed = time.perf_counter() - start_time
        self.clock.receive_event(response.lamport_timestamp, "upload_ack")
        logger.info("Unary upload: %d bytes in %.3fs", len(data), elapsed)
        return response.success

    def download_model(self) -> List[float]:
        """Télécharge le modèle global par streaming."""
        if not self.stub:
            return self.local_weights

        chunks = []
        for chunk in self.stub.DownloadModel(
            federation_pb2.ModelRequest(client_id=self.flat_id, round=self.current_round),
            timeout=30,
        ):
            self.clock.receive_event(chunk.lamport_timestamp, f"download_chunk_{chunk.chunk_index}")
            chunks.append(chunk.data)

        if chunks:
            full_data = b"".join(chunks)
            weights = np.frombuffer(full_data, dtype=np.float64).tolist()
            self.local_weights = weights
            self.clock.tick("model_applied")
            logger.info("Downloaded global model (%d weights)", len(weights))
            return weights
        return self.local_weights

    def run_round(self) -> bool:
        """Exécute un round complet : train → upload → download."""
        gradients = self.train_local()
        if not self.upload_gradients(gradients):
            return False
        self.download_model()
        self.current_round += 1
        return True

    def resolve_name(self, flat_id: str = "", structured_path: str = "") -> Optional[str]:
        """Teste la résolution de noms."""
        if not self.stub:
            return None
        response = self.stub.ResolveName(
            federation_pb2.NameResolveRequest(flat_id=flat_id, structured_path=structured_path),
            timeout=5,
        )
        return response.address if response.found else None

    def query_by_attributes(self, attributes: dict) -> list:
        """Teste la recherche par attributs."""
        if not self.stub:
            return []
        response = self.stub.QueryByAttributes(
            federation_pb2.AttributeQueryRequest(attributes=attributes),
            timeout=5,
        )
        return list(response.client_ids)

    def close(self) -> None:
        if self.channel:
            self.channel.close()


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Federated Learning Client")
    parser.add_argument("--server", default="localhost:50051")
    parser.add_argument("--process-id", type=int, default=1)
    parser.add_argument("--dataset", default="iris")
    parser.add_argument("--location", default="local")
    parser.add_argument("--rounds", type=int, default=5)
    args = parser.parse_args()

    client = FederatedClient(args.server, args.process_id, args.dataset, args.location)
    if not client.connect():
        sys.exit(1)
    if not client.register():
        sys.exit(1)

    for r in range(args.rounds):
        logger.info("=== Round %d/%d ===", r + 1, args.rounds)
        client.heartbeat()
        if not client.run_round():
            logger.error("Round %d failed", r + 1)
        time.sleep(0.5)

    client.close()
    logger.info("Client %d finished. Lamport event log:", args.process_id)
    for event, ts in client.clock.get_event_log()[-10:]:
        logger.info("  %s → <%d, %d>", event, ts.time, ts.process_id)


if __name__ == "__main__":
    main()
