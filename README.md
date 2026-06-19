# INF4218 — Système Distribué d'Apprentissage Fédéré

Projet de programmation distribuée implémentant les concepts fondamentaux des systèmes distribués (Van Steen & Tanenbaum, chapitres 1-8).

## Architecture

Architecture client-serveur à **3 niveaux** :

```
┌─────────────────────────────────────────┐
│  Couche Application                     │
│  (rounds FL, entraînement local,        │
│   agrégation de gradients)              │
├─────────────────────────────────────────┤
│  Couche Middleware                      │
│  (Lamport, Bully, mutex, nommage)       │
├─────────────────────────────────────────┤
│  Couche Réseau (gRPC)                   │
│  (RPC unary + streaming)                │
└─────────────────────────────────────────┘
```

## Concepts implémentés

| Concept | Chapitre | Module |
|---------|----------|--------|
| Nommage plat | 6.2 | `src/naming/flat.py` |
| Nommage structuré | 6.3 | `src/naming/structured.py` |
| Nommage par attributs | 6.4 | `src/naming/attribute.py` |
| Horloges Lamport | 5.2.1 | `src/coordination/lamport.py` |
| Élection Bully | 5.4.1 | `src/coordination/bully.py` |
| Exclusion mutuelle (Ricart-Agrawala) | 5.3.3 | `src/coordination/mutual_exclusion.py` |
| Communication gRPC | 4 | `src/communication/`, `proto/` |
| Tolérance aux pannes (checkpoint) | 8.6.2 | `src/fault_tolerance/checkpoint.py` |

## Installation

```bash
cd TPSD
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m grpc_tools.protoc -Iproto --python_out=src/communication --grpc_python_out=src/communication proto/federation.proto
```

## Lancement

### 1. Démarrer le serveur coordinateur

```bash
source .venv/bin/activate
python -m src.server.coordinator --port 50051 --process-id 0
```

### 2. Démarrer les clients (dans des terminaux séparés)

```bash
# Client 1
python -m src.client.federated_client --process-id 1 --dataset iris --rounds 5

# Client 2
python -m src.client.federated_client --process-id 2 --dataset mnist --rounds 5

# Client 3
python -m src.client.federated_client --process-id 3 --dataset cifar --rounds 5
```

### 3. Simuler une panne du coordinateur

Arrêtez le serveur (Ctrl+C), puis relancez-le. Le système reprend depuis le dernier checkpoint dans `checkpoints/`.

## Tests unitaires

```bash
source .venv/bin/activate
pytest tests/ -v
```

## Expériences de performance

```bash
source .venv/bin/activate
python experiments/benchmarks.py
```

Les résultats sont sauvegardés dans `experiments/results/benchmark_results.json`.

### Expériences incluses

1. **Cohérence Lamport** — vérifie que les événements de rounds respectent l'ordre happens-before
2. **Streaming vs Unary** — compare les temps de transfert selon le volume de données
3. **Élection Bully** — simule la panne du coordinateur et mesure le temps d'élection
4. **Checkpoint/Recovery** — teste la reprise après panne

## Structure du projet

```
TPSD/
├── proto/federation.proto       # Définition gRPC
├── src/
│   ├── naming/                # Nommage plat, structuré, attributs
│   ├── coordination/          # Lamport, Bully, mutex
│   ├── fault_tolerance/       # Checkpoint, recovery
│   ├── communication/         # Stubs gRPC générés
│   ├── server/coordinator.py  # Serveur coordinateur
│   └── client/federated_client.py  # Client fédéré
├── tests/                     # Tests unitaires
├── experiments/benchmarks.py  # Benchmarks
├── checkpoints/               # Sauvegardes d'état
└── requirements.txt
```

## Limitations et améliorations possibles

- **Scalabilité** : l'élection Bully est O(n²) en messages ; remplacer par Raft pour de grands clusters
- **Sécurité** : communication non chiffrée (gRPC insecure) ; ajouter TLS et authentification
- **Hétérogénéité** : clients homogènes en Python ; supporter d'autres langages via gRPC multi-langage
- **Byzantine fault tolerance** : non implémentée ; seule la panne-crash est gérée

## Références

- Van Steen & Tanenbaum, *Distributed Systems*, 4th edition (2024)
- Lamport, "Time, Clocks, and the Ordering of Events" (1978)
- Garcia-Molina, "Elections in a Distributed Computing System" (1982)
- Ricart & Agrawala, "An Optimal Algorithm for Mutual Exclusion" (1981)
