"""Nommage structuré (chapitre 6.3 - Van Steen & Tanenbaum).

Organise les noms en graphe hiérarchique (espace de noms) avec
chemins absolus du type /federation/clients/client1/models/v1.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple


@dataclass
class DirectoryNode:
    """Nœud de répertoire dans l'espace de noms."""

    name: str
    children: Dict[str, "DirectoryNode | LeafNode"] = field(default_factory=dict)
    is_directory: bool = True


@dataclass
class LeafNode:
    """Nœud feuille contenant les données d'une entité."""

    name: str
    address: str
    metadata: Dict[str, str] = field(default_factory=dict)
    is_directory: bool = False


class StructuredNamingService:
    """Espace de noms hiérarchique inspiré du DNS/Unix."""

    SEPARATOR = "/"

    def __init__(self, root_name: str = "federation") -> None:
        self._root = DirectoryNode(name=root_name)

    def _parse_path(self, path: str) -> List[str]:
        """Parse un chemin absolu en composants."""
        path = path.strip(self.SEPARATOR)
        if not path:
            return []
        return path.split(self.SEPARATOR)

    def _navigate(self, components: List[str]) -> Tuple[Optional[DirectoryNode], List[str]]:
        """Navigue dans l'arbre jusqu'au dernier nœud répertoire atteint."""
        current: DirectoryNode = self._root
        for i, component in enumerate(components):
            child = current.children.get(component)
            if child is None:
                return current, components[i:]
            if isinstance(child, LeafNode):
                return None, components[i:]
            current = child
        return current, []

    def register(self, path: str, address: str, **metadata: str) -> bool:
        """Enregistre une entité à un chemin structuré."""
        components = self._parse_path(path)
        if not components:
            return False

        current = self._root
        for component in components[:-1]:
            child = current.children.get(component)
            if child is None:
                child = DirectoryNode(name=component)
                current.children[component] = child
            elif isinstance(child, LeafNode):
                return False
            current = child

        leaf_name = components[-1]
        current.children[leaf_name] = LeafNode(
            name=leaf_name,
            address=address,
            metadata=dict(metadata),
        )
        return True

    def resolve(self, path: str) -> Optional[LeafNode]:
        """Résout un chemin structuré vers l'entité correspondante."""
        components = self._parse_path(path)
        if not components:
            return None

        current: DirectoryNode | LeafNode = self._root
        for component in components:
            if isinstance(current, LeafNode):
                return None
            child = current.children.get(component)
            if child is None:
                return None
            current = child

        return current if isinstance(current, LeafNode) else None

    def list_children(self, path: str = "") -> List[str]:
        """Liste les enfants d'un répertoire."""
        components = self._parse_path(path) if path else []
        node, remaining = self._navigate(components)
        if node is None or remaining:
            return []
        return list(node.children.keys())

    def get_absolute_path(self, path: str) -> str:
        """Retourne le chemin absolu normalisé."""
        components = self._parse_path(path)
        return self.SEPARATOR + self.SEPARATOR.join(components)
