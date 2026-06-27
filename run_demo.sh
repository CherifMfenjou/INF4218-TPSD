#!/usr/bin/env bash
# Script de démonstration du système d'apprentissage fédéré
#
# Démo locale (même machine) :
#   ./run_demo.sh
#
# Démo multi-machines : sur le serveur
#   HOST=192.168.1.100 ./run_demo.sh
# Sur chaque client distant :
#   python -m src.client.federated_client --server 192.168.1.100:50051 --process-id 1

set -e
cd "$(dirname "$0")"
source .venv/bin/activate

PORT=${1:-50051}
ROUNDS=${2:-3}
HOST=${HOST:-$(python -c "from src.utils.network import get_local_ip; print(get_local_ip())")}

echo "=== Démarrage du coordinateur sur ${HOST}:${PORT} ==="
python -m src.server.coordinator --port "$PORT" --process-id 0 --host "$HOST" &
SERVER_PID=$!
sleep 2

cleanup() {
    echo "=== Arrêt du serveur ==="
    kill $SERVER_PID 2>/dev/null || true
}
trap cleanup EXIT

echo "=== Lancement de 3 clients fédérés ($ROUNDS rounds chacun) ==="
echo "    Adresses annoncées : ${HOST}:50061, ${HOST}:50062, ${HOST}:50063"
python -m src.client.federated_client --server "${HOST}:${PORT}" --process-id 1 --dataset iris --rounds "$ROUNDS" &
python -m src.client.federated_client --server "${HOST}:${PORT}" --process-id 2 --dataset mnist --rounds "$ROUNDS" &
python -m src.client.federated_client --server "${HOST}:${PORT}" --process-id 3 --dataset cifar --rounds "$ROUNDS" &
wait

echo "=== Exécution des benchmarks ==="
python experiments/benchmarks.py

echo "=== Démonstration terminée ==="
