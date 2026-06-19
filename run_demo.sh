#!/usr/bin/env bash
# Script de démonstration du système d'apprentissage fédéré

set -e
cd "$(dirname "$0")"
source .venv/bin/activate

PORT=${1:-50051}
ROUNDS=${2:-3}

echo "=== Démarrage du coordinateur sur le port $PORT ==="
python -m src.server.coordinator --port "$PORT" --process-id 0 &
SERVER_PID=$!
sleep 2

cleanup() {
    echo "=== Arrêt du serveur ==="
    kill $SERVER_PID 2>/dev/null || true
}
trap cleanup EXIT

echo "=== Lancement de 3 clients fédérés ($ROUNDS rounds chacun) ==="
python -m src.client.federated_client --server "localhost:$PORT" --process-id 1 --dataset iris --rounds "$ROUNDS" &
python -m src.client.federated_client --server "localhost:$PORT" --process-id 2 --dataset mnist --rounds "$ROUNDS" &
python -m src.client.federated_client --server "localhost:$PORT" --process-id 3 --dataset cifar --rounds "$ROUNDS" &
wait

echo "=== Exécution des benchmarks ==="
python experiments/benchmarks.py

echo "=== Démonstration terminée ==="
