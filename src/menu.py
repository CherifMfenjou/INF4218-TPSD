"""Menu interactif — système d'apprentissage fédéré INF4218."""

from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import List, Optional

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.client.federated_client import FederatedClient
from src.fault_tolerance.checkpoint import CheckpointManager
from src.utils.network import CLIENT_BASE_PORT, coordinator_address, default_client_address, get_local_ip

DEFAULT_PORT = 50051
DEFAULT_CHECKPOINT_DIR = "checkpoints"
LOG_DIR = PROJECT_ROOT / "logs"


class FederationMenu:
    """Orchestrateur interactif : serveur, clients, benchmarks, nommage."""

    def __init__(self) -> None:
        self.port = DEFAULT_PORT
        self.checkpoint_dir = DEFAULT_CHECKPOINT_DIR
        self.host = get_local_ip()
        self.server_process: Optional[subprocess.Popen] = None
        self.server_log_path = LOG_DIR / "server.log"

    @property
    def server_address(self) -> str:
        return coordinator_address(self.host, self.port)

    def run(self) -> None:
        self._print_banner()
        while True:
            self._print_menu()
            choice = input("Choix : ").strip()
            handlers = {
                "1": self.start_server,
                "2": self.stop_server,
                "3": self.launch_client,
                "4": self.full_demo,
                "5": self.run_benchmarks,
                "6": self.run_tests,
                "7": self.view_checkpoints,
                "8": self.test_naming,
                "9": self.show_status,
                "10": self.test_network_addresses,
                "0": self.quit,
            }
            handler = handlers.get(choice)
            if handler:
                handler()
            else:
                print("Choix invalide.\n")
            input("\n[Entrée] pour revenir au menu...")

    def quit(self) -> None:
        if self.server_process and self.server_process.poll() is None:
            print("Arrêt du serveur avant de quitter...")
            self.stop_server()
        print("Au revoir.")
        sys.exit(0)

    def start_server(self) -> None:
        if self._server_alive():
            print(f"Le serveur tourne déjà sur le port {self.port} (PID {self.server_process.pid}).")
            return

        port_str = input(f"Port [{self.port}] : ").strip()
        if port_str:
            self.port = int(port_str)

        ckpt = input(f"Dossier checkpoints [{self.checkpoint_dir}] : ").strip()
        if ckpt:
            self.checkpoint_dir = ckpt

        host_str = input(f"IP annoncée du serveur [{self.host}] : ").strip()
        if host_str:
            self.host = host_str

        if self._port_in_use(self.port):
            print(f"Erreur : le port {self.port} est déjà utilisé.")
            return

        LOG_DIR.mkdir(parents=True, exist_ok=True)
        log_file = open(self.server_log_path, "a", encoding="utf-8")
        log_file.write(f"\n--- Démarrage serveur {time.strftime('%Y-%m-%d %H:%M:%S')} ---\n")
        log_file.flush()

        cmd = [
            sys.executable,
            "-m",
            "src.server.coordinator",
            "--port",
            str(self.port),
            "--process-id",
            "0",
            "--checkpoint-dir",
            self.checkpoint_dir,
            "--host",
            self.host,
        ]
        self.server_process = subprocess.Popen(
            cmd,
            cwd=PROJECT_ROOT,
            stdout=log_file,
            stderr=subprocess.STDOUT,
        )
        time.sleep(2)

        if self.server_process.poll() is not None:
            print("Échec du démarrage. Consultez logs/server.log")
            self.server_process = None
            return

        print(f"Serveur démarré — {self.server_address}, PID {self.server_process.pid}")
        print(f"Logs : {self.server_log_path}")

        latest = CheckpointManager(self.checkpoint_dir).load("latest")
        if latest:
            print(f"Checkpoint chargé : reprise au round {latest.current_round}")
        else:
            print("Aucun checkpoint existant (premier lancement).")

    def stop_server(self) -> None:
        if not self._server_alive():
            print("Aucun serveur géré par le menu n'est en cours d'exécution.")
            return

        assert self.server_process is not None
        print(f"Arrêt du serveur (PID {self.server_process.pid})...")
        self._terminate_process(self.server_process)
        self.server_process = None
        print("Serveur arrêté. Checkpoint sauvegardé si des rounds ont été exécutés.")

    def launch_client(self) -> None:
        if not self._port_in_use(self.port):
            print(f"Le serveur n'est pas accessible sur {self.server_address}.")
            print("Utilisez l'option 1 pour démarrer le coordinateur.")
            return

        process_id = int(input("Process ID [1] : ").strip() or "1")
        dataset = input("Dataset [iris] : ").strip() or "iris"
        location = input("Location [local] : ").strip() or "local"
        rounds = int(input("Nombre de rounds [3] : ").strip() or "3")
        mode = input("Mode (a=automatique, p=pas à pas) [a] : ").strip().lower() or "a"

        client = FederatedClient(self.server_address, process_id, dataset, location)
        if not client.connect():
            print("Impossible de se connecter au serveur.")
            return
        if not client.register():
            print("Échec de l'inscription (RegisterClient).")
            client.close()
            return

        print(f"Inscrit : UUID={client.flat_id}")
        print(f"  Adresse réseau : {client.advertise_address}")
        print(f"  Chemin : {client.structured_path}")
        print(f"  Attributs : dataset={dataset}, location={location}")

        for r in range(rounds):
            print(f"\n--- Round {r + 1}/{rounds} ---")
            if mode == "p":
                input("  [Entrée] pour envoyer heartbeat et exécuter le round...")
            client.heartbeat()
            if not client.run_round():
                print(f"  Échec du round {r + 1}")
                break
            print(f"  Round {r + 1} terminé — Lamport ts={client.clock.current}")
            if mode == "a" and r < rounds - 1:
                time.sleep(0.5)

        print("\nJournal Lamport (10 derniers événements) :")
        for event, ts in client.clock.get_event_log()[-10:]:
            print(f"  {event} → <{ts.time}, {ts.process_id}>")
        client.close()

    def full_demo(self) -> None:
        rounds = int(input("Rounds par client [3] : ").strip() or "3")
        run_bench = input("Lancer les benchmarks après la démo ? (o/n) [o] : ").strip().lower()
        if run_bench == "":
            run_bench = "o"

        if not self._server_alive():
            print("Démarrage du serveur pour la démo...")
            self.port = DEFAULT_PORT
            self.checkpoint_dir = DEFAULT_CHECKPOINT_DIR
            if self._port_in_use(self.port):
                print(f"Port {self.port} occupé — arrêtez l'autre processus ou changez de port.")
                return
            LOG_DIR.mkdir(parents=True, exist_ok=True)
            log_file = open(self.server_log_path, "a", encoding="utf-8")
            cmd = [
                sys.executable, "-m", "src.server.coordinator",
                "--port", str(self.port), "--process-id", "0",
                "--checkpoint-dir", self.checkpoint_dir,
                "--host", self.host,
            ]
            self.server_process = subprocess.Popen(
                cmd, cwd=PROJECT_ROOT, stdout=log_file, stderr=subprocess.STDOUT,
            )
            time.sleep(2)

        if not self._port_in_use(self.port):
            print("Serveur inaccessible. Abandon.")
            return

        datasets = [("1", "iris"), ("2", "mnist"), ("3", "cifar")]
        print(f"\nLancement de 3 clients ({rounds} rounds chacun)...")
        print(f"  Serveur : {self.server_address}")
        procs: List[subprocess.Popen] = []
        for pid, ds in datasets:
            addr = default_client_address(int(pid), self.host)
            cmd = [
                sys.executable, "-m", "src.client.federated_client",
                "--server", self.server_address,
                "--process-id", pid,
                "--dataset", ds,
                "--rounds", str(rounds),
                "--host", self.host,
            ]
            procs.append(subprocess.Popen(cmd, cwd=PROJECT_ROOT))
            print(f"  Client P{pid} ({ds}) — {addr} — PID {procs[-1].pid}")

        for p in procs:
            p.wait()
        print("\nDémonstration clients terminée.")

        if run_bench == "o":
            self.run_benchmarks()

    def run_benchmarks(self) -> None:
        print("Exécution de experiments/benchmarks.py ...\n")
        result = subprocess.run(
            [sys.executable, "experiments/benchmarks.py"],
            cwd=PROJECT_ROOT,
        )
        if result.returncode != 0:
            print("Les benchmarks ont échoué.")
            return

        results_file = PROJECT_ROOT / "experiments" / "results" / "benchmark_results.json"
        if not results_file.exists():
            return

        with open(results_file, encoding="utf-8") as f:
            data = json.load(f)

        print("\n--- Résumé benchmark_results.json ---")
        print(f"Date : {data.get('timestamp', '?')}")
        for exp in data.get("experiments", []):
            name = exp.get("experiment", "?")
            if name == "lamport_round_consistency":
                print(f"  Lamport : {exp.get('ordering_violations')} violation(s) / "
                      f"{exp.get('total_events')} événements")
            elif name == "bully_election":
                print(f"  Bully : P{exp.get('new_coordinator')} élu en "
                      f"{exp.get('election_time_ms')} ms")
            elif name == "fault_tolerance":
                print(f"  Checkpoint : round {exp.get('recovered_round')} restauré")
            elif name == "communication_strategies":
                results = exp.get("results", [])
                if results:
                    print(f"  gRPC : ratio max streaming = "
                          f"{results[-1].get('streaming_overhead_ratio')}×")
        print(f"\nFichier complet : {results_file}")

    def run_tests(self) -> None:
        print("Exécution de pytest tests/ -v ...\n")
        subprocess.run([sys.executable, "-m", "pytest", "tests/", "-v"], cwd=PROJECT_ROOT)

    def view_checkpoints(self) -> None:
        mgr = CheckpointManager(str(PROJECT_ROOT / self.checkpoint_dir))
        names = mgr.list_checkpoints()
        if not names:
            print(f"Aucun checkpoint dans {self.checkpoint_dir}/")
            return

        print(f"Checkpoints disponibles ({self.checkpoint_dir}/) :")
        for i, name in enumerate(sorted(names), 1):
            print(f"  {i}. {name}")

        choice = input("Nom ou numéro à afficher [latest] : ").strip() or "latest"
        if choice.isdigit():
            idx = int(choice) - 1
            if 0 <= idx < len(names):
                choice = sorted(names)[idx]

        state = mgr.load(choice)
        if not state:
            print("Checkpoint introuvable.")
            return

        print(f"\n--- {choice}.json ---")
        print(f"  Round           : {state.current_round}")
        print(f"  Coordinateur    : {state.coordinator_id}")
        print(f"  Horloge Lamport : {state.lamport_clock}")
        weights = state.model_weights
        if weights:
            preview = weights[:5]
            suffix = "..." if len(weights) > 5 else ""
            print(f"  Poids modèle    : {preview}{suffix} ({len(weights)} valeurs)")

        if input("\nAfficher le JSON complet ? (o/n) [n] : ").strip().lower() == "o":
            path = mgr._checkpoint_path(choice)
            print(path.read_text(encoding="utf-8"))

    def test_naming(self) -> None:
        if not self._port_in_use(self.port):
            print(f"Serveur inaccessible sur {self.server_address}.")
            return

        print("\n--- Test de nommage ---")
        print("  1. Résolution par UUID (plat)")
        print("  2. Résolution par chemin structuré")
        print("  3. Recherche par attributs (QueryByAttributes)")
        sub = input("Sous-menu [3] : ").strip() or "3"

        client = FederatedClient(self.server_address, process_id=99, dataset="probe", location="local")
        if not client.connect():
            print("Connexion impossible.")
            return

        probe_registered = client.register()
        if probe_registered:
            print(f"Client sonde inscrit : {client.flat_id}")

        try:
            if sub == "1":
                uid = input(f"UUID [{client.flat_id}] : ").strip() or client.flat_id
                addr = client.resolve_name(flat_id=uid)
                print(f"Résultat : {addr or 'non trouvé'}")
            elif sub == "2":
                path = input("Chemin [/federation/clients/client1/models/local] : ").strip()
                path = path or "/federation/clients/client1/models/local"
                addr = client.resolve_name(structured_path=path)
                print(f"Résultat : {addr or 'non trouvé'}")
            else:
                ds = input("Attribut dataset [mnist] : ").strip() or "mnist"
                loc = input("Attribut location [local] : ").strip() or "local"
                ids, addrs = client.query_by_attributes({"dataset": ds, "location": loc})
                if ids:
                    print(f"Clients trouvés ({len(ids)}) :")
                    for cid, addr in zip(ids, addrs):
                        print(f"  - {cid}  →  {addr or '?'}")
                else:
                    print("Aucun client trouvé. Lancez d'abord des clients (option 3 ou 4).")
        finally:
            client.close()

    def test_network_addresses(self) -> None:
        """Affiche et teste les adresses IP serveur/clients."""
        print("\n--- Test des adresses réseau ---")

        detected = get_local_ip()
        print(f"\n  IP auto-détectée     : {detected}")
        host_str = input(f"  IP à utiliser [{self.host}] : ").strip()
        if host_str:
            self.host = host_str

        port_str = input(f"  Port serveur [{self.port}] : ").strip()
        if port_str:
            self.port = int(port_str)

        print(f"\n  {'Rôle':<22} {'Processus':<10} {'Adresse réseau'}")
        print(f"  {'-' * 22} {'-' * 10} {'-' * 28}")
        print(f"  {'Serveur coordinateur':<22} {'P0':<10} {self.server_address}")
        for pid in range(1, 4):
            addr = default_client_address(pid, self.host)
            print(f"  {'Client fédéré':<22} {'P' + str(pid):<10} {addr}")

        print(f"\n  Ports clients : {CLIENT_BASE_PORT + 1} à {CLIENT_BASE_PORT + 3}"
              f" (base {CLIENT_BASE_PORT} + process_id)")

        print("\n  Test connectivité TCP vers le serveur :")
        reachable_hosts = []
        for test_host in dict.fromkeys(["127.0.0.1", self.host, detected]):
            ok = self._port_in_use(self.port, test_host)
            status = "accessible" if ok else "inaccessible"
            print(f"    {test_host}:{self.port} → {status}")
            if ok:
                reachable_hosts.append(test_host)

        if not reachable_hosts:
            print("\n  Le serveur ne répond pas. Démarrez-le avec l'option 1, puis relancez l'option 10.")
            print(f"\n  Commande manuelle serveur :")
            print(f"    python -m src.server.coordinator --host {self.host} --port {self.port}")
            print(f"\n  Commande manuelle client (ex. P1) :")
            print(f"    python -m src.client.federated_client --server {self.server_address}"
                  f" --process-id 1 --host {self.host}")
            return

        connect_host = reachable_hosts[0]
        server_for_clients = coordinator_address(connect_host, self.port)
        print(f"\n  Adresse gRPC recommandée pour les clients : {server_for_clients}")

        if input("\n  Lister les clients inscrits sur le serveur ? (o/n) [o] : ").strip().lower() in ("", "o"):
            self._print_registered_client_addresses(server_for_clients)

        if input("  Inscrire un client sonde et afficher son IP ? (o/n) [n] : ").strip().lower() == "o":
            probe_id = int(input("  Process ID sonde [99] : ").strip() or "99")
            client = FederatedClient(
                server_for_clients, probe_id, "probe", "local", host=self.host,
            )
            try:
                if client.connect() and client.register():
                    print("\n  Client sonde inscrit :")
                    print(f"    UUID    : {client.flat_id}")
                    print(f"    Adresse : {client.advertise_address}")
                    resolved = client.resolve_name(flat_id=client.flat_id)
                    print(f"    ResolveName (plat) : {resolved or 'non trouvé'}")
                else:
                    print("  Échec connexion ou inscription.")
            finally:
                client.close()

    def _print_registered_client_addresses(self, server_address: str) -> None:
        """Interroge le nommage pour afficher UUID → adresse IP des clients inscrits."""
        client = FederatedClient(server_address, process_id=98, dataset="probe", location="local")
        if not client.connect():
            print("  Connexion au serveur impossible.")
            return

        seen: dict[str, str] = {}
        try:
            for ds in ("iris", "mnist", "cifar", "probe", "default"):
                ids, addrs = client.query_by_attributes({"dataset": ds})
                for cid, addr in zip(ids, addrs):
                    seen.setdefault(cid, addr or "?")

            for pid in range(1, 6):
                path = f"/federation/clients/client{pid}/models/local"
                addr = client.resolve_name(structured_path=path)
                if addr:
                    seen[f"[structuré] client{pid}"] = addr

            if not seen:
                print("  Aucun client inscrit. Lancez l'option 3 ou 4 d'abord.")
                return

            print(f"\n  {'Identifiant':<40} {'Adresse IP:port'}")
            print(f"  {'-' * 40} {'-' * 20}")
            for cid, addr in seen.items():
                label = cid if len(cid) <= 38 else cid[:35] + "..."
                print(f"  {label:<40} {addr}")
        finally:
            client.close()

    def show_status(self) -> None:
        print("\n--- État du système ---")
        print(f"  IP locale détectée : {self.host}")
        print(f"  Port configuré     : {self.port}")
        print(f"  Adresse gRPC       : {self.server_address}")
        print(f"  Dossier checkpoints: {self.checkpoint_dir}")

        if self._server_alive():
            print(f"  Serveur            : ACTIF (PID {self.server_process.pid})")
        elif self._port_in_use(self.port):
            print("  Serveur            : port ouvert (processus externe ?)")
        else:
            print("  Serveur            : ARRÊTÉ")

        mgr = CheckpointManager(str(PROJECT_ROOT / self.checkpoint_dir))
        latest = mgr.load("latest")
        if latest:
            print(f"  Dernier checkpoint : round {latest.current_round}, "
                  f"Lamport {latest.lamport_clock}")
        else:
            print("  Dernier checkpoint : aucun")

        if self.server_log_path.exists():
            lines = self.server_log_path.read_text(encoding="utf-8").splitlines()
            tail = lines[-8:]
            print(f"\n  Dernières lignes ({self.server_log_path.name}) :")
            for line in tail:
                print(f"    {line}")

    @staticmethod
    def _terminate_process(proc: subprocess.Popen) -> None:
        """Arrête un sous-processus (SIGINT pour checkpoint, sinon terminate/kill)."""
        try:
            if os.name != "nt":
                proc.send_signal(signal.SIGINT)
            else:
                proc.terminate()
        except (PermissionError, ProcessLookupError, OSError):
            proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()

    def _server_alive(self) -> bool:
        return self.server_process is not None and self.server_process.poll() is None

    @staticmethod
    def _port_in_use(port: int, host: str = "127.0.0.1") -> bool:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(1)
            return sock.connect_ex((host, port)) == 0

    @staticmethod
    def _print_banner() -> None:
        print()
        print("=" * 56)
        print("  INF4218 — Système d'apprentissage fédéré")
        print("  Menu interactif")
        print("=" * 56)
        print()

    def _print_menu(self) -> None:
        status = "ACTIF" if self._server_alive() else (
            "port ouvert" if self._port_in_use(self.port) else "arrêté"
        )
        print(f"\nServeur ({self.server_address}) : {status}\n")
        print("  1. Démarrer le serveur coordinateur")
        print("  2. Arrêter le serveur")
        print("  3. Lancer un client fédéré (auto ou pas à pas)")
        print("  4. Démonstration complète (3 clients)")
        print("  5. Exécuter les benchmarks")
        print("  6. Lancer les tests unitaires (pytest)")
        print("  7. Consulter les checkpoints")
        print("  8. Tester le nommage (plat / structuré / attributs)")
        print("  9. Afficher l'état du système")
        print(" 10. Tester les adresses réseau (IP serveur / clients)")
        print("  0. Quitter")


def main() -> None:
    try:
        FederationMenu().run()
    except KeyboardInterrupt:
        print("\n\nInterruption — au revoir.")
        sys.exit(0)


if __name__ == "__main__":
    main()
