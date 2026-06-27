"""Tests des utilitaires réseau."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils.network import default_client_address, get_local_ip


class TestNetworkUtils:
    def test_get_local_ip_returns_string(self):
        ip = get_local_ip()
        assert isinstance(ip, str)
        assert ip

    def test_default_client_address_uses_distinct_ports(self):
        addr1 = default_client_address(1, host="10.0.0.5")
        addr2 = default_client_address(2, host="10.0.0.5")
        assert addr1 == "10.0.0.5:50061"
        assert addr2 == "10.0.0.5:50062"
        assert addr1 != addr2
