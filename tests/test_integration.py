"""Test d'intégration serveur + clients."""

import subprocess
import sys
import time
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).parent.parent
PYTHON = PROJECT_ROOT / ".venv" / "bin" / "python"


@pytest.fixture
def server_process():
    proc = subprocess.Popen(
        [str(PYTHON), "-m", "src.server.coordinator", "--port", "50052", "--process-id", "0"],
        cwd=str(PROJECT_ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    time.sleep(2)
    yield proc
    try:
        proc.terminate()
        proc.wait(timeout=5)
    except (PermissionError, ProcessLookupError):
        pass


@pytest.mark.timeout(30)
def test_client_server_integration(server_process):
    result = subprocess.run(
        [str(PYTHON), "-m", "src.client.federated_client",
         "--server", "localhost:50052", "--process-id", "10",
         "--dataset", "iris", "--rounds", "2"],
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
        timeout=25,
    )
    assert result.returncode == 0, f"Client failed: {result.stderr}"
    assert "Registered" in result.stderr or "Registered" in result.stdout
