"""Nommage plat (chapitre 6.2 - Van Steen & Tanenbaum).

Les identifiants plats sont des chaînes aléatoires sans structure,
utilisées pour identifier de manière unique des entités (clients, modèles).
La résolution se fait par recherche directe dans un registre.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Dict, Optional


@dataclass
class FlatEntity:
    """Entité identifiée par un nom plat."""

    flat_id: str
    address: str
    metadata: Dict[str, str] = field(default_factory=dict)


class FlatNamingService:
    """Registre de nommage plat avec résolution par identifiant."""

    def __init__(self) -> None:
        self._registry: Dict[str, FlatEntity] = {}

    @staticmethod
    def generate_id() -> str:
        """Génère un identifiant plat unique (UUID)."""
        return str(uuid.uuid4())

    def register(self, address: str, flat_id: Optional[str] = None, **metadata: str) -> str:
        """Enregistre une entité et retourne son identifiant plat."""
        entity_id = flat_id or self.generate_id()
        self._registry[entity_id] = FlatEntity(
            flat_id=entity_id,
            address=address,
            metadata=dict(metadata),
        )
        return entity_id

    def resolve(self, flat_id: str) -> Optional[FlatEntity]:
        """Résout un identifiant plat vers l'entité correspondante."""
        return self._registry.get(flat_id)

    def unregister(self, flat_id: str) -> bool:
        """Supprime une entité du registre."""
        if flat_id in self._registry:
            del self._registry[flat_id]
            return True
        return False

    def list_all(self) -> list[str]:
        """Liste tous les identifiants enregistrés."""
        return list(self._registry.keys())

    def broadcast_resolve(self, flat_id: str) -> Optional[FlatEntity]:
        """Simulation de résolution par broadcast (ARP-like).

        Dans un vrai réseau LAN, un message serait diffusé à tous les hôtes.
        Ici, on simule en interrogeant le registre local.
        """
        return self.resolve(flat_id)
