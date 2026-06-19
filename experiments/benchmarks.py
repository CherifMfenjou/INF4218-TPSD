"""Script d'expériences : comparaison des performances et démonstrations."""

from __future__ import annotations

import json
import logging
import subprocess
import sys
import time
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.coordination.lamport import LamportClock
from src.coordination.bully import BullyElection

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)
RESULTS_DIR = Path(__file__).parent.parent / "experiments" / "results"


def experiment_lamport_round_consistency():
    """Mesure l'impact des horloges Lamport sur la cohérence des rounds."""
    logger.info("=== Expérience 1 : Cohérence des rounds avec horloges Lamport ===")

    num_processes = 5
    num_rounds = 10
    clocks = [LamportClock(i) for i in range(num_processes)]
    round_events = []
    violations = 0

    for round_num in range(num_rounds):
        for i, clock in enumerate(clocks):
            send_ts = clock.send_event(f"round_{round_num}_send")
            round_events.append({
                "round": round_num,
                "process": i,
                "send_ts": send_ts,
                "type": "send",
            })

            for j, other_clock in enumerate(clocks):
                if i != j:
                    recv_ts = other_clock.receive_event(send_ts, f"round_{round_num}_recv_from_{i}")
                    if recv_ts.time <= send_ts:
                        violations += 1
                    round_events.append({
                        "round": round_num,
                        "process": j,
                        "sender": i,
                        "recv_ts": recv_ts.time,
                        "send_ts": send_ts,
                        "type": "receive",
                    })

    results = {
        "experiment": "lamport_round_consistency",
        "num_processes": num_processes,
        "num_rounds": num_rounds,
        "total_events": len(round_events),
        "ordering_violations": violations,
        "consistent": violations == 0,
        "final_clock_values": [c.current for c in clocks],
    }

    logger.info("Violations d'ordonnancement : %d (attendu: 0)", violations)
    logger.info("Cohérence garantie : %s", results["consistent"])
    return results


def experiment_communication_strategies():
    """Compare streaming vs unary pour différents volumes de données."""
    logger.info("=== Expérience 2 : Comparaison streaming vs unary ===")

    sizes = [1024, 4096, 16384, 65536, 262144]
    chunk_size = 4096
    results = []

    for size in sizes:
        data = np.random.randn(size // 8).astype(np.float64).tobytes()
        num_chunks = max(1, (len(data) + chunk_size - 1) // chunk_size)

        unary_start = time.perf_counter()
        _ = data
        unary_time = time.perf_counter() - unary_start

        streaming_start = time.perf_counter()
        chunks = []
        for i in range(num_chunks):
            start = i * chunk_size
            end = min(start + chunk_size, len(data))
            chunks.append(data[start:end])
        _ = b"".join(chunks)
        streaming_time = time.perf_counter() - streaming_start

        results.append({
            "data_size_bytes": size,
            "num_chunks": num_chunks,
            "unary_time_ms": round(unary_time * 1000, 3),
            "streaming_time_ms": round(streaming_time * 1000, 3),
            "streaming_overhead_ratio": round(streaming_time / max(unary_time, 1e-9), 2),
        })
        logger.info(
            "Size %dB: unary=%.3fms, streaming=%.3fms (ratio=%.2f)",
            size, results[-1]["unary_time_ms"], results[-1]["streaming_time_ms"],
            results[-1]["streaming_overhead_ratio"],
        )

    return {"experiment": "communication_strategies", "results": results}


def experiment_bully_election():
    """Simule une panne du coordinateur et mesure l'élection."""
    logger.info("=== Expérience 3 : Élection Bully après panne ===")

    num_processes = 8
    bully = BullyElection(process_id=4)
    for i in range(num_processes):
        bully.mark_process_alive(i, f"host:{50050 + i}")

    bully.coordinator_id = 7
    elections_started = []

    def mock_send_election(sender, targets):
        alive_higher = [p for p in targets if bully.processes[p].alive]
        if alive_higher:
            highest = max(alive_higher)
            bully.handle_coordinator_message(highest)
            return alive_higher
        return []

    bully.set_send_election(mock_send_election)
    bully.set_send_coordinator(lambda c, t: None)

    start = time.perf_counter()
    bully.mark_coordinator_dead()
    election_time = time.perf_counter() - start

    results = {
        "experiment": "bully_election",
        "num_processes": num_processes,
        "failed_coordinator": 7,
        "new_coordinator": bully.coordinator_id,
        "election_time_ms": round(election_time * 1000, 3),
        "success": bully.coordinator_id is not None and bully.coordinator_id != 7,
    }

    logger.info("Coordinateur %d tombé → nouveau coordinateur : %d (%.3fms)",
                7, bully.coordinator_id, results["election_time_ms"])
    return results


def experiment_fault_tolerance():
    """Teste checkpoint et recovery."""
    logger.info("=== Expérience 4 : Tolérance aux pannes (checkpoint/recovery) ===")

    from src.fault_tolerance.checkpoint import CheckpointManager, RecoveryManager, SystemState

    cp_dir = Path(__file__).parent.parent / "checkpoints" / "exp"
    cp_dir.mkdir(parents=True, exist_ok=True)
    mgr = CheckpointManager(str(cp_dir))
    recovery = RecoveryManager(mgr, timeout=0.5)

    rounds_before_crash = 9
    for r in range(1, rounds_before_crash + 1):
        state = SystemState(
            current_round=r,
            coordinator_id=0,
            lamport_clock=r * 3,
            model_weights=[0.1 * r] * 10,
        )
        if mgr.should_checkpoint(r, interval=3):
            mgr.save(state, f"round_{r}")

    crash_detected = recovery.detect_failure(time.time() - 2.0)
    recovered = recovery.recover("round_9") or recovery.recover("round_6") or recovery.recover("round_3")

    results = {
        "experiment": "fault_tolerance",
        "rounds_before_crash": rounds_before_crash,
        "crash_detected": crash_detected,
        "recovered_round": recovered.current_round if recovered else None,
        "recovered_weights": recovered.model_weights if recovered else None,
        "checkpoints_available": mgr.list_checkpoints(),
        "success": recovered is not None,
    }

    logger.info("Crash détecté: %s, reprise au round %s", crash_detected, results["recovered_round"])
    return results


def run_all_experiments():
    """Exécute toutes les expériences et sauvegarde les résultats."""
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    all_results = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "experiments": [],
    }

    for experiment_fn in [
        experiment_lamport_round_consistency,
        experiment_communication_strategies,
        experiment_bully_election,
        experiment_fault_tolerance,
    ]:
        result = experiment_fn()
        all_results["experiments"].append(result)

    output_file = RESULTS_DIR / "benchmark_results.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2)

    logger.info("Résultats sauvegardés dans %s", output_file)
    return all_results


if __name__ == "__main__":
    run_all_experiments()
