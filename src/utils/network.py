"""Utilitaires réseau : détection d'IP et adresses client/serveur."""

from __future__ import annotations

import socket
from typing import Optional

CLIENT_BASE_PORT = 50060


def get_local_ip(fallback: str = "127.0.0.1") -> str:
    """Retourne l'IP locale routable (LAN), ou fallback si indéterminée."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.connect(("8.8.8.8", 80))
            return sock.getsockname()[0]
    except OSError:
        return fallback


def default_client_address(process_id: int, host: Optional[str] = None) -> str:
    """Adresse annoncée d'un client : IP:port distinct par process_id."""
    ip = host or get_local_ip()
    return f"{ip}:{CLIENT_BASE_PORT + process_id}"


def coordinator_address(host: Optional[str], port: int) -> str:
    """Adresse gRPC du coordinateur."""
    return f"{host or get_local_ip()}:{port}"
