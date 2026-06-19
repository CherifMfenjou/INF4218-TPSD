"""Nommage par attributs (chapitre 6.4 - Van Steen & Tanenbaum).

Permet de rechercher des entités par leurs attributs
(ex: dataset=iris, location=local) plutôt que par nom.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class AttributedEntity:
    """Entité avec attributs descriptifs."""

    entity_id: str
    address: str
    attributes: Dict[str, str] = field(default_factory=dict)


class AttributeNamingService:
    """Service de résolution par attributs (type tuple space / LDAP)."""

    def __init__(self) -> None:
        self._entities: Dict[str, AttributedEntity] = {}

    def register(
        self,
        entity_id: str,
        address: str,
        attributes: Dict[str, str],
    ) -> None:
        """Enregistre une entité avec ses attributs."""
        self._entities[entity_id] = AttributedEntity(
            entity_id=entity_id,
            address=address,
            attributes=dict(attributes),
        )

    def query(self, required_attributes: Dict[str, str]) -> List[AttributedEntity]:
        """Recherche les entités dont les attributs correspondent.

        Tous les attributs requis doivent être présents et égaux.
        """
        results = []
        for entity in self._entities.values():
            if all(
                entity.attributes.get(key) == value
                for key, value in required_attributes.items()
            ):
                results.append(entity)
        return results

    def query_partial(self, attributes: Dict[str, str]) -> List[AttributedEntity]:
        """Recherche avec correspondance partielle (au moins un attribut)."""
        results = []
        for entity in self._entities.values():
            if any(
                entity.attributes.get(key) == value
                for key, value in attributes.items()
            ):
                results.append(entity)
        return results

    def resolve(self, entity_id: str) -> Optional[AttributedEntity]:
        """Résout un identifiant vers l'entité."""
        return self._entities.get(entity_id)

    def update_attributes(self, entity_id: str, attributes: Dict[str, str]) -> bool:
        """Met à jour les attributs d'une entité."""
        entity = self._entities.get(entity_id)
        if entity is None:
            return False
        entity.attributes.update(attributes)
        return True

    def unregister(self, entity_id: str) -> bool:
        """Supprime une entité."""
        if entity_id in self._entities:
            del self._entities[entity_id]
            return True
        return False
