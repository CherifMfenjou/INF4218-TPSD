# Rapport de Projet — INF4218 Programmation Distribuée

**Système distribué d'apprentissage fédéré**

---

**Contexte :** Travail pratique INF4218 — Programmation Distribuée  
**Référence théorique :** Van Steen & Tanenbaum, *Distributed Systems*, 4e édition (chapitres 1 à 8)  
**Date :** Juin 2026

---

## Table des matières

1. [Introduction](#1-introduction)
2. [Conception](#2-conception)
3. [Détails d'implémentation](#3-détails-dimplémentation)
4. [Résultats expérimentaux](#4-résultats-expérimentaux)
5. [Conclusion](#5-conclusion)
6. [Références](#6-références)

---

## 1. Introduction

### 1.1 Contexte et objectifs

Ce projet s'inscrit dans le cadre du cours INF4218 — Programmation Distribuée. Il vise à mettre en œuvre les concepts fondamentaux des systèmes distribués étudiés en cours, en s'appuyant sur l'ouvrage de référence de Van Steen et Tanenbaum (*Distributed Systems*, chapitres 1 à 8).

L'objectif principal est de concevoir et d'implémenter un **système distribué simplifié d'apprentissage fédéré** (*Federated Learning*, FL) dans lequel un serveur central coordonne plusieurs clients participants. Chaque client entraîne un modèle localement sur ses propres données, puis envoie ses gradients au serveur qui les agrège pour produire un modèle global. Ce scénario est représentatif des systèmes distribués modernes : coordination, communication à distance, cohérence des événements et tolérance aux pannes.

### 1.2 Concepts obligatoires

Le cahier des charges impose l'implémentation des concepts suivants :

| Concept | Chapitre Tanenbaum |
|---------|-------------------|
| Nommage plat | 6.2 |
| Nommage structuré | 6.3 |
| Nommage par attributs | 6.4 |
| Horloges logiques de Lamport | 5.2.1 |
| Élection de leader (Bully) | 5.4.1 |
| Exclusion mutuelle distribuée | 5.3.3 |
| Communication RPC/gRPC | 4 |
| Tolérance aux pannes (checkpoint) | 8.6.2 |

### 1.3 Technologies choisies

- **Langage :** Python 3.14
- **Communication :** gRPC (Protocol Buffers)
- **Calcul :** NumPy (simulation des gradients)
- **Tests :** pytest (38 tests unitaires + 1 test d'intégration)
- **Architecture :** Client-serveur à trois niveaux

---

## 2. Conception

### 2.1 Architecture globale

Le système suit une **architecture client-serveur à trois niveaux**, inspirée du chapitre 2 de Tanenbaum (modèle en couches) :

```
+---------------------------------------------------------+
|  COUCHE APPLICATION                                     |
|  - Rounds d'apprentissage fedeere                       |
|  - Entrainement local (simulation)                      |
|  - Agregation des gradients (moyenne federee)           |
+---------------------------------------------------------+
|  COUCHE MIDDLEWARE                                      |
|  - Horloges de Lamport (ordonnancement des evenements)  |
|  - Election Bully (election du coordinateur)            |
|  - Exclusion mutuelle Ricart-Agrawala                   |
|  - Services de nommage (plat, structure, attributs)     |
|  - Checkpoint / Recovery                                |
+---------------------------------------------------------+
|  COUCHE RESEAU (gRPC)                                   |
|  - RPC unary : heartbeat, enregistrement, mutex         |
|  - RPC streaming : upload gradients, download modele    |
+---------------------------------------------------------+
```

### 2.2 Acteurs du système

**Serveur coordinateur (`FederationCoordinator`)**
- Point central de coordination des rounds FL
- Maintient le modèle global et agrège les gradients
- Gère les registres de nommage et l'horloge Lamport globale
- Effectue des checkpoints périodiques de l'état

**Clients fédérés (`FederatedClient`)**
- S'enregistrent auprès du coordinateur via les trois systèmes de nommage
- Entraînent localement et envoient leurs gradients
- Téléchargent le modèle global mis à jour
- Envoient des heartbeats pour la détection de pannes

### 2.3 Flux d'un round d'apprentissage fédéré

```
Client                          Coordinateur
  |                                  |
  |---- Heartbeat (Lamport ts) ----->|
  |<--- Round actuel + ts -----------|
  |                                  |
  |  [Entrainement local]            |
  |                                  |
  |---- UploadGradients (stream) --->|  [Mutex: section critique]
  |                                  |  [Agregation des gradients]
  |<--- DownloadModel (stream) ------|
  |                                  |
  |  [Checkpoint si round % 3 == 0]  |
```

### 2.4 Systèmes de nommage

Trois paradigmes coexistent pour localiser les entités du système :

**Nommage plat (UUID)**
- Identifiants opaques sans structure sémantique
- Exemple : `a3f2b1c4-5678-90ab-cdef-1234567890ab`
- Résolution par recherche directe dans un registre central

**Nommage structuré (chemins hiérarchiques)**
- Organisation en arbre, inspirée du système de fichiers Unix et du DNS
- Exemple : `/federation/clients/client1/models/local`
- Résolution par navigation dans l'espace de noms

**Nommage par attributs**
- Recherche par paires clé-valeur descriptives
- Exemple : `{dataset: "iris", location: "local"}`
- Permet de sélectionner dynamiquement les clients selon leurs caractéristiques

### 2.5 Mécanismes de coordination

**Horloges de Lamport**
- Chaque processus maintient un compteur local `Ci`
- Règles : incrément avant événement local ; `Cj ← max(Cj, ts(m)) + 1` à la réception
- Garantit : si `a → b` (happens-before), alors `C(a) < C(b)`
- Utilisées pour ordonner les événements de rounds et l'exclusion mutuelle

**Élection Bully**
- Le processus d'identifiant le plus élevé devient coordinateur
- En cas de panne du coordinateur, le premier détecteur lance une élection
- Messages `ELECTION` vers les processus supérieurs ; `COORDINATOR` en cas de victoire

**Exclusion mutuelle (Ricart-Agrawala)**
- Protège la section critique d'agrégation des gradients
- Demande avec timestamp Lamport ; réponses `OK` selon l'ordre total
- Le plus petit timestamp accède en premier ; pas de deadlock ni de famine

### 2.6 Tolérance aux pannes

- **Détection :** timeout sur les heartbeats (5 secondes par défaut)
- **Checkpointing :** sauvegarde JSON de l'état tous les 3 rounds
- **Recovery :** reprise depuis le dernier checkpoint au redémarrage du serveur
- État sauvegardé : round courant, poids du modèle, horloge Lamport, historique

---

## 3. Détails d'implémentation

### 3.1 Structure du projet

```
TPSD/
  proto/federation.proto          -- Definition du service gRPC
  src/
    naming/                       -- flat.py, structured.py, attribute.py
    coordination/                 -- lamport.py, bully.py, mutual_exclusion.py
    fault_tolerance/              -- checkpoint.py
    communication/                -- Stubs gRPC generes
    server/coordinator.py         -- Serveur coordinateur
    client/federated_client.py    -- Client federe
  tests/                          -- 38 tests unitaires
  experiments/benchmarks.py       -- Scripts de mesure
  checkpoints/                    -- Sauvegardes d'etat
```

### 3.2 Communication gRPC

Le fichier `proto/federation.proto` définit le service `FederationCoordinator` avec deux modes de communication adaptés au volume de données :

**RPC unary** (messages légers, latence faible) :
- `RegisterClient`, `Heartbeat`
- `RequestMutualExclusion`, `ReleaseMutualExclusion`
- `ResolveName`, `QueryByAttributes`
- `SendElection`, `AnnounceCoordinator`

**RPC streaming** (gros volumes) :
- `UploadGradients` : flux client → serveur par chunks de 4 Ko
- `DownloadModel` : flux serveur → client par chunks de 4 Ko

Chaque message transporte un **timestamp Lamport** pour maintenir la cohérence temporelle à travers le réseau.

### 3.3 Horloges de Lamport

Implémentation dans `src/coordination/lamport.py` :

```python
class LamportClock:
    def tick(self, event_name):           # Événement local : Ci ← Ci + 1
    def send_event(self, event_name):     # Envoi : retourne ts(m) = Ci
    def receive_event(self, received_ts): # Réception : Cj ← max(Cj, ts) + 1
```

Les timestamps utilisent des tuples `(time, process_id)` pour garantir l'unicité en cas d'égalité (tie-breaker par identifiant de processus).

### 3.4 Élection Bully

Implémentation dans `src/coordination/bully.py` :

1. `start_election()` : envoie `ELECTION` aux processus d'ID supérieur
2. Si aucune réponse → `_become_coordinator()` : annonce `COORDINATOR` à tous
3. Si réponse `OK` → attend l'élection du processus supérieur
4. `mark_coordinator_dead()` : déclenche une élection après panne détectée

### 3.5 Exclusion mutuelle Ricart-Agrawala

Implémentation dans `src/coordination/mutual_exclusion.py` :

- `request_access(resource)` : diffuse une demande horodatée, attend les `OK`
- `handle_request(req)` : compare les timestamps, accorde ou diffère
- `release_access(resource)` : envoie les `OK` différés, libère la ressource

La section critique protège l'agrégation des gradients dans `coordinator.py` :

```python
def aggregate_round(self):
    if not self.mutex.request_access("aggregation"):
        return
    try:
        # Agrégation : moyenne des gradients
        self.model_weights = np.mean(all_grads, axis=0).tolist()
        self.current_round += 1
    finally:
        self.mutex.release_access("aggregation")
```

### 3.6 Services de nommage

| Service | Classe | Opérations principales |
|---------|--------|------------------------|
| Plat | `FlatNamingService` | `register()`, `resolve()`, `broadcast_resolve()` |
| Structuré | `StructuredNamingService` | `register(path)`, `resolve(path)`, `list_children()` |
| Attributs | `AttributeNamingService` | `register(id, attrs)`, `query(attrs)`, `query_partial()` |

L'enregistrement d'un client utilise simultanément les trois systèmes lors de l'appel `RegisterClient`.

### 3.7 Checkpoint et recovery

```python
@dataclass
class SystemState:
    current_round: int
    coordinator_id: Optional[int]
    lamport_clock: int
    model_weights: List[float]
    round_history: List[Dict]
```

- Sauvegarde : `checkpoints/latest.json` (ou `round_N.json`)
- Intervalle : tous les 3 rounds
- Recovery : chargement automatique au démarrage du serveur

### 3.8 Tests unitaires

38 tests couvrent l'ensemble des modules :

| Module | Tests | Couverture |
|--------|-------|------------|
| Lamport | 7 | Incrément, send/receive, happens-before, tie-breaker |
| Bully | 7 | Élection, panne, defer, annonce coordinateur |
| Nommage | 12 | Plat, structuré, attributs |
| Mutex | 5 | Accès, conflit, release, replies |
| Checkpoint | 7 | Save/load, intervalle, recovery, timeout |

Commande : `pytest tests/ -v` — **38 passed**

---

## 4. Résultats expérimentaux

Les expériences ont été exécutées via `python experiments/benchmarks.py`. Les résultats sont stockés dans `experiments/results/benchmark_results.json`.

### 4.1 Cohérence des rounds avec horloges Lamport

**Objectif :** Vérifier que les horloges Lamport garantissent l'ordonnancement correct des événements de rounds (relation happens-before).

**Protocole :** 5 processus, 10 rounds. À chaque round, chaque processus envoie un message horodaté à tous les autres. On vérifie que pour chaque paire (envoi, réception), `C(réception) > C(envoi)`.

**Résultats :**

| Métrique | Valeur |
|----------|--------|
| Processus | 5 |
| Rounds | 10 |
| Événements totaux | 250 |
| Violations d'ordonnancement | **0** |
| Cohérence garantie | **Oui** |
| Valeurs finales des horloges | [100, 100, 100, 100, 99] |

**Analyse :** Aucune violation de la relation happens-before n'a été détectée sur 200 paires send/receive (5 processus × 4 récepteurs × 10 rounds). Les horloges Lamport assurent une cohérence totale de l'ordonnancement des événements de rounds, condition nécessaire à l'agrégation fiable des gradients dans un contexte distribué.

### 4.2 Comparaison streaming vs unary (gRPC)

**Objectif :** Mesurer l'impact de la stratégie de communication selon le volume de données transférées.

**Protocole :** Transfert de buffers de tailles croissantes (1 Ko à 256 Ko), comparaison entre envoi en un seul message (unary) et envoi par chunks de 4 Ko (streaming).

**Résultats :**

| Taille (octets) | Chunks | Unary (ms) | Streaming (ms) | Ratio overhead |
|-----------------|--------|------------|----------------|----------------|
| 1 024 | 1 | 0.001 | 0.006 | 10.8× |
| 4 096 | 1 | 0.000 | 0.003 | 15.0× |
| 16 384 | 4 | 0.000 | 0.006 | 20.9× |
| 65 536 | 16 | 0.000 | 0.011 | 33.3× |
| 262 144 | 64 | 0.000 | 0.041 | 83.9× |

**Analyse :**
- Pour les **petits messages** (< 8 Ko, seuil `STREAMING_THRESHOLD`), l'approche unary est préférable : moins de surcharge protocolaire.
- Pour les **gros volumes** (> 64 Ko), le streaming devient avantageux en termes de robustesse (reprise partielle possible) et de consommation mémoire (pas de buffer monolithique), malgré un overhead mesuré localement.
- En production réseau, l'écart serait plus marqué : le streaming permet le pipelining et réduit la latence perçue pour les gros modèles.

### 4.3 Élection Bully après panne du coordinateur

**Objectif :** Démontrer que l'élection de leader fonctionne lors d'une panne simulée.

**Protocole :** 8 processus (IDs 0–7), coordinateur initial = processus 7. Simulation de la panne de P7, élection déclenchée par P4.

**Résultats :**

| Métrique | Valeur |
|----------|--------|
| Processus | 8 |
| Coordinateur initial | P7 |
| Coordinateur élu | **P6** (ID le plus élevé vivant) |
| Temps d'élection | **0.17 ms** |
| Succès | **Oui** |

**Déroulement :**
1. P4 détecte la panne de P7
2. P4 envoie `ELECTION` à P5 et P6
3. P5 et P6 répondent `OK` (IDs supérieurs vivants)
4. P6, ayant l'ID le plus élevé, devient coordinateur
5. P6 annonce sa victoire via `COORDINATOR`

**Analyse :** L'algorithme Bully garantit qu'après une panne, le processus vivant d'identifiant maximal prend le relais. Le temps d'élection est faible en local ; en réseau réel, il dépendrait des timeouts configurés.

### 4.4 Tolérance aux pannes (checkpoint / recovery)

**Objectif :** Valider la détection de panne et la reprise depuis un checkpoint.

**Protocole :** 9 rounds simulés, checkpoints aux rounds 3, 6 et 9. Crash simulé (absence de heartbeat pendant 2 s). Recovery depuis le dernier checkpoint.

**Résultats :**

| Métrique | Valeur |
|----------|--------|
| Rounds avant crash | 9 |
| Crash détecté | **Oui** (timeout heartbeat) |
| Round récupéré | **9** |
| Poids du modèle récupérés | [0.9 × 10] |
| Checkpoints disponibles | round_3, round_6, round_9 |
| Succès | **Oui** |

**Analyse :** Le mécanisme de checkpoint permet de reprendre exactement au dernier état cohérent sans recommencer depuis le round 0. En cas de panne entre deux checkpoints (ex. round 8), la reprise se ferait au round 6, avec une perte maximale de 2 rounds de travail.

### 4.5 Synthèse des expériences

| Expérience | Objectif | Résultat |
|------------|----------|----------|
| Lamport | Cohérence des rounds | 0 violation / 200 paires |
| Streaming vs unary | Adaptation au volume | Unary < 8 Ko ; streaming > 64 Ko |
| Bully | Reprise après panne | P6 élu en 0.17 ms |
| Checkpoint | Recovery | Round 9 restauré intégralement |

---

## 5. Conclusion

### 5.1 Bilan

Ce projet a permis de concevoir et d'implémenter un système distribué d'apprentissage fédéré intégrant l'ensemble des concepts obligatoires du cours INF4218. L'architecture en trois niveaux (application, middleware, réseau) offre une séparation claire des responsabilités et facilite la maintenance et l'extension.

Les résultats expérimentaux confirment :
- La **cohérence** apportée par les horloges de Lamport pour l'ordonnancement des événements
- L'**adaptabilité** de la communication gRPC selon le volume de données
- La **résilience** face aux pannes grâce à l'élection Bully et au checkpointing

Le système est fonctionnel, testé (38 tests unitaires passent) et documenté (README, scripts de démonstration).

### 5.2 Limitations

**Scalabilité**
- L'algorithme Bully génère O(n²) messages en cas d'élection fréquente
- Le coordinateur central constitue un goulot d'étranglement pour l'agrégation
- L'exclusion mutuelle Ricart-Agrawala requiert 2(n-1) messages par acces

**Sécurité**
- Communication gRPC non chiffrée (`insecure_channel`)
- Absence d'authentification des clients
- Pas de protection contre les clients malveillants (Byzantine faults)

**Hétérogénéité**
- Implémentation monolingue (Python uniquement)
- Clients supposés homogènes (même dimension de modèle)
- Pas de support pour des datasets de tailles très différentes

### 5.3 Perspectives d'amélioration

1. **Scalabilité :** Remplacer Bully par Raft ou ZooKeeper pour les grands clusters ; introduire un coordinateur hiérarchique ou un agrégateur intermédiaire (tree-based aggregation).

2. **Sécurité :** Activer TLS/mTLS sur gRPC ; ajouter une authentification par certificats ; implémenter l'agrégation sécurisée (Secure Aggregation).

3. **Hétérogénéité :** Exploiter la portabilité de gRPC pour des clients Java, Go ou C++ ; supporter le *Federated Averaging* avec des modèles de dimensions variables.

4. **Tolérance avancée :** Réplication du coordinateur ; checkpoint distribué ; gestion des pannes Byzantine (PBFT).

5. **Performance :** Compression des gradients (quantification, sparsification) ; parallélisation de l'agrégation ; cache des résolutions de noms.

---

## 6. Références

1. Van Steen M., Tanenbaum A.S., *Distributed Systems*, 4th edition, 2024.
2. Lamport L., « Time, Clocks, and the Ordering of Events in a Distributed System », *Communications of the ACM*, 21(7), 1978.
3. Garcia-Molina H., « Elections in a Distributed Computing System », *IEEE Transactions on Computers*, 31(1), 1982.
4. Ricart G., Agrawala A.K., « An Optimal Algorithm for Mutual Exclusion in Computer Networks », *Communications of the ACM*, 24(1), 1981.
5. McMahan B. et al., « Communication-Efficient Learning of Deep Networks from Decentralized Data », *AISTATS*, 2017.
6. Documentation gRPC : https://grpc.io/docs/languages/python/

---

## Annexe A — Commandes de reproduction

```bash
# Installation
cd TPSD && python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Tests
pytest tests/ -v

# Expériences
python experiments/benchmarks.py

# Démonstration live
./run_demo.sh
```

## Annexe B — Diagramme de séquence (round FL)

```
Client A          Client B          Coordinateur
   |                 |                  |
   |-- Heartbeat ----|----------------->|
   |                 |-- Heartbeat ---->|
   |                 |                  | [Lamport: ordonnancement]
   |  [Train local]  |  [Train local]   |
   |                 |                  |
   |-- Gradients ----|----------------->| [Mutex: agregation]
   |                 |-- Gradients ---->|
   |<- Model --------|------------------|
   |                 |<- Model ----------|
   |                 |                  | [Checkpoint round N]
```

---

*Fin du rapport — INF4218 Programmation Distribuée*
