"""Tests unitaires pour les systèmes de nommage."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.naming.flat import FlatNamingService
from src.naming.structured import StructuredNamingService
from src.naming.attribute import AttributeNamingService


class TestFlatNaming:
    def test_register_and_resolve(self):
        svc = FlatNamingService()
        fid = svc.register("localhost:5001", dataset="iris")
        entity = svc.resolve(fid)
        assert entity is not None
        assert entity.address == "localhost:5001"

    def test_generate_unique_ids(self):
        svc = FlatNamingService()
        ids = {svc.generate_id() for _ in range(100)}
        assert len(ids) == 100

    def test_unregister(self):
        svc = FlatNamingService()
        fid = svc.register("addr")
        assert svc.unregister(fid)
        assert svc.resolve(fid) is None

    def test_broadcast_resolve(self):
        svc = FlatNamingService()
        fid = svc.register("10.0.0.1")
        assert svc.broadcast_resolve(fid).address == "10.0.0.1"


class TestStructuredNaming:
    def test_register_and_resolve(self):
        svc = StructuredNamingService()
        svc.register("/federation/clients/c1/models/v1", "localhost:5001")
        node = svc.resolve("/federation/clients/c1/models/v1")
        assert node is not None
        assert node.address == "localhost:5001"

    def test_nested_directories(self):
        svc = StructuredNamingService()
        svc.register("/federation/clients/c1/models/v1", "addr1")
        svc.register("/federation/clients/c2/models/v1", "addr2")
        children = svc.list_children("/federation/clients")
        assert "c1" in children
        assert "c2" in children

    def test_absolute_path(self):
        svc = StructuredNamingService()
        path = svc.get_absolute_path("federation/clients/c1")
        assert path == "/federation/clients/c1"

    def test_nonexistent_path(self):
        svc = StructuredNamingService()
        assert svc.resolve("/nonexistent/path") is None


class TestAttributeNaming:
    def test_query_exact_match(self):
        svc = AttributeNamingService()
        svc.register("c1", "addr1", {"dataset": "iris", "location": "local"})
        svc.register("c2", "addr2", {"dataset": "mnist", "location": "cloud"})
        results = svc.query({"dataset": "iris"})
        assert len(results) == 1
        assert results[0].entity_id == "c1"

    def test_query_multiple_attributes(self):
        svc = AttributeNamingService()
        svc.register("c1", "addr1", {"dataset": "iris", "location": "local"})
        svc.register("c3", "addr3", {"dataset": "iris", "location": "cloud"})
        results = svc.query({"dataset": "iris", "location": "local"})
        assert len(results) == 1

    def test_query_partial(self):
        svc = AttributeNamingService()
        svc.register("c1", "addr1", {"dataset": "iris"})
        svc.register("c2", "addr2", {"dataset": "mnist"})
        results = svc.query_partial({"dataset": "iris"})
        assert len(results) == 1

    def test_update_attributes(self):
        svc = AttributeNamingService()
        svc.register("c1", "addr1", {"dataset": "iris"})
        svc.update_attributes("c1", {"location": "remote"})
        entity = svc.resolve("c1")
        assert entity.attributes["location"] == "remote"
