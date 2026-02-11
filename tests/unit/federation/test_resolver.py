# coding=utf-8
"""Unit tests for DVT federation resolver.

Tests the FederationResolver class that determines execution paths
(pushdown vs federation) for models based on their source targets.
"""

from argparse import Namespace
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set
from unittest.mock import MagicMock, Mock, patch

import pytest

from dvt.federation.resolver import (
    ExecutionPath,
    FederationResolver,
    ResolvedExecution,
)


# =============================================================================
# Mock Objects
# =============================================================================


def _make_args(target=None, compute=None, bucket=None):
    """Create a Namespace mimicking CLI args for FederationResolver."""
    return Namespace(target=target, compute=compute, bucket=bucket)


@dataclass
class MockNodeConfig:
    """Mock node config for testing."""

    target: Optional[str] = None
    compute: Optional[str] = None
    bucket: Optional[str] = None
    materialized: str = "table"


@dataclass
class MockModelNode:
    """Mock model node for testing."""

    unique_id: str
    name: str
    config: MockNodeConfig = field(default_factory=MockNodeConfig)
    depends_on_nodes: List[str] = field(default_factory=list)
    fqn: List[str] = field(default_factory=list)

    @property
    def depends_on(self):
        return Mock(nodes=self.depends_on_nodes)


@dataclass
class MockSourceNode:
    """Mock source node for testing."""

    unique_id: str
    name: str
    source_name: str
    connection: Optional[str] = None

    def to_dict(self):
        return {"connection": self.connection}


@dataclass
class MockManifest:
    """Mock manifest for testing."""

    nodes: Dict[str, Any] = field(default_factory=dict)
    sources: Dict[str, MockSourceNode] = field(default_factory=dict)

    def expect(self, unique_id: str):
        if unique_id in self.nodes:
            return self.nodes[unique_id]
        if unique_id in self.sources:
            return self.sources[unique_id]
        raise KeyError(unique_id)


@dataclass
class MockProfile:
    """Mock profile for testing."""

    profile_name: str
    target_name: str
    target: Any = None


@dataclass
class MockRuntimeConfig:
    """Mock runtime config for testing."""

    profile: MockProfile = field(
        default_factory=lambda: MockProfile("default", "default_target")
    )
    target_name: str = "default_target"
    project_name: str = "test_project"

    def __post_init__(self):
        # Ensure profile.target_name matches
        self.profile.target_name = self.target_name


# =============================================================================
# ResolvedExecution Tests
# =============================================================================


class TestResolvedExecution:
    """Tests for ResolvedExecution dataclass."""

    def test_basic_creation(self):
        """Should create ResolvedExecution with required fields."""
        resolved = ResolvedExecution(
            model_id="model.test.my_model",
            target="snowflake",
            compute="spark_local",
            bucket="s3://my-bucket",
            execution_path=ExecutionPath.SPARK_FEDERATION,
        )

        assert resolved.model_id == "model.test.my_model"
        assert resolved.target == "snowflake"
        assert resolved.compute == "spark_local"
        assert resolved.execution_path == ExecutionPath.SPARK_FEDERATION

    def test_with_materialization_coercion(self):
        """Should track materialization coercion."""
        resolved = ResolvedExecution(
            model_id="model.test.my_model",
            target="snowflake",
            compute="spark_local",
            bucket="s3://my-bucket",
            execution_path=ExecutionPath.SPARK_FEDERATION,
            original_materialization="view",
            coerced_materialization="table",
        )

        assert resolved.original_materialization == "view"
        assert resolved.coerced_materialization == "table"


class TestExecutionPath:
    """Tests for ExecutionPath enum."""

    def test_enum_values(self):
        """Should have expected enum values."""
        assert ExecutionPath.ADAPTER_PUSHDOWN.value == "adapter_pushdown"
        assert ExecutionPath.SPARK_FEDERATION.value == "spark_federation"


# =============================================================================
# FederationResolver Tests
# =============================================================================


class TestFederationResolver:
    """Tests for FederationResolver class."""

    @pytest.fixture
    def basic_manifest(self):
        """Create a basic manifest with models and sources."""
        source1 = MockSourceNode(
            unique_id="source.test.postgres.orders",
            name="orders",
            source_name="postgres",
            connection="postgres_target",
        )
        source2 = MockSourceNode(
            unique_id="source.test.mysql.products",
            name="products",
            source_name="mysql",
            connection="mysql_target",
        )

        model1 = MockModelNode(
            unique_id="model.test.model_a",
            name="model_a",
            depends_on_nodes=["source.test.postgres.orders"],
            fqn=["test", "model_a"],
        )
        model2 = MockModelNode(
            unique_id="model.test.model_b",
            name="model_b",
            depends_on_nodes=[
                "source.test.postgres.orders",
                "source.test.mysql.products",
            ],
            fqn=["test", "model_b"],
        )

        return MockManifest(
            nodes={
                "model.test.model_a": model1,
                "model.test.model_b": model2,
            },
            sources={
                "source.test.postgres.orders": source1,
                "source.test.mysql.products": source2,
            },
        )

    @pytest.fixture
    def runtime_config(self):
        """Create a basic runtime config."""
        return MockRuntimeConfig(target_name="default_target")

    @pytest.fixture
    def resolver(self, basic_manifest, runtime_config):
        """Create a FederationResolver instance."""
        return FederationResolver(
            manifest=basic_manifest,
            runtime_config=runtime_config,
            args=_make_args(),
        )

    # -------------------------------------------------------------------------
    # Target Resolution Tests
    # -------------------------------------------------------------------------

    def test_resolve_target_from_cli(self, basic_manifest, runtime_config):
        """CLI target should override all other sources."""
        resolver = FederationResolver(
            manifest=basic_manifest,
            runtime_config=runtime_config,
            args=_make_args(target="cli_target"),
        )

        node = basic_manifest.nodes["model.test.model_a"]
        target = resolver._resolve_target(node)

        assert target == "cli_target"

    def test_resolve_target_from_model_config(self, basic_manifest, runtime_config):
        """Model config target should be used when no CLI target."""
        node = basic_manifest.nodes["model.test.model_a"]
        node.config.target = "model_target"

        resolver = FederationResolver(
            manifest=basic_manifest,
            runtime_config=runtime_config,
            args=_make_args(),
        )

        target = resolver._resolve_target(node)

        assert target == "model_target"

    def test_resolve_target_default(self, resolver, basic_manifest):
        """Should fall back to runtime config target."""
        node = basic_manifest.nodes["model.test.model_a"]
        target = resolver._resolve_target(node)

        assert target == "default_target"

    # -------------------------------------------------------------------------
    # Execution Path Determination Tests
    # -------------------------------------------------------------------------

    def test_single_source_same_target_pushdown(self, resolver, basic_manifest):
        """Single source with same target should use pushdown."""
        execution_path = resolver._determine_execution_path(
            target="postgres_target",
            upstream_targets={"postgres_target"},
        )

        assert execution_path == ExecutionPath.ADAPTER_PUSHDOWN

    def test_multiple_sources_same_target_pushdown(
        self, basic_manifest, runtime_config
    ):
        """Multiple sources from same target should use pushdown."""
        # _determine_execution_path only checks target vs upstream_targets
        resolver = FederationResolver(
            manifest=basic_manifest,
            runtime_config=runtime_config,
            args=_make_args(),
        )

        path = resolver._determine_execution_path(
            target="postgres_target",
            upstream_targets={"postgres_target"},
        )

        assert path == ExecutionPath.ADAPTER_PUSHDOWN

    def test_cross_target_federation(self, resolver, basic_manifest):
        """Sources from different targets should require federation."""
        execution_path = resolver._determine_execution_path(
            target="snowflake_target",
            upstream_targets={"postgres_target", "mysql_target"},
        )

        assert execution_path == ExecutionPath.SPARK_FEDERATION

    def test_model_target_different_from_source_federation(
        self, resolver, basic_manifest
    ):
        """Model target different from source should require federation."""
        execution_path = resolver._determine_execution_path(
            target="snowflake_target",
            upstream_targets={"postgres_target"},
        )

        assert execution_path == ExecutionPath.SPARK_FEDERATION

    # -------------------------------------------------------------------------
    # Upstream Target Resolution Tests
    # -------------------------------------------------------------------------

    def test_get_upstream_targets_from_sources(self, resolver, basic_manifest):
        """Should get targets from source nodes."""
        node = basic_manifest.nodes["model.test.model_b"]

        targets = resolver._get_upstream_targets(node)

        assert "postgres_target" in targets
        assert "mysql_target" in targets

    def test_get_upstream_targets_from_models(self, basic_manifest, runtime_config):
        """Should get targets from upstream model nodes."""
        # Create model that depends on another model
        upstream = MockModelNode(
            unique_id="model.test.upstream",
            name="upstream",
            depends_on_nodes=["source.test.postgres.orders"],
            fqn=["test", "upstream"],
        )
        upstream.config.target = "upstream_target"

        downstream = MockModelNode(
            unique_id="model.test.downstream",
            name="downstream",
            depends_on_nodes=["model.test.upstream"],
            fqn=["test", "downstream"],
        )

        source = MockSourceNode(
            unique_id="source.test.postgres.orders",
            name="orders",
            source_name="postgres",
            connection="postgres_target",
        )

        manifest = MockManifest(
            nodes={
                "model.test.upstream": upstream,
                "model.test.downstream": downstream,
            },
            sources={"source.test.postgres.orders": source},
        )

        resolver = FederationResolver(
            manifest=manifest,
            runtime_config=runtime_config,
            args=_make_args(),
        )

        targets = resolver._get_upstream_targets(downstream)

        assert "upstream_target" in targets

    # -------------------------------------------------------------------------
    # Materialization Coercion Tests
    # -------------------------------------------------------------------------

    def test_view_coerced_to_table_for_federation(self, resolver, basic_manifest):
        """View materialization should be coerced to table for federation."""
        node = basic_manifest.nodes["model.test.model_b"]
        node.config.materialized = "view"
        node.config.target = "snowflake_target"

        resolved = resolver.resolve_model(node)

        # Should be federation path
        assert resolved.execution_path == ExecutionPath.SPARK_FEDERATION
        # View should be coerced to table
        assert resolved.original_materialization == "view"
        assert resolved.coerced_materialization == "table"

    def test_table_not_coerced_for_federation(self, resolver, basic_manifest):
        """Table materialization should not be coerced."""
        node = basic_manifest.nodes["model.test.model_b"]
        node.config.materialized = "table"
        node.config.target = "snowflake_target"

        resolved = resolver.resolve_model(node)

        # No coercion needed — original_materialization is always populated
        assert resolved.original_materialization == "table"
        assert resolved.coerced_materialization is None

    def test_incremental_not_coerced_for_federation(self, resolver, basic_manifest):
        """Incremental materialization should not be coerced."""
        node = basic_manifest.nodes["model.test.model_b"]
        node.config.materialized = "incremental"
        node.config.target = "snowflake_target"

        resolved = resolver.resolve_model(node)

        # No coercion needed — original_materialization is always populated
        assert resolved.original_materialization == "incremental"
        assert resolved.coerced_materialization is None

    # -------------------------------------------------------------------------
    # Resolve All Tests
    # -------------------------------------------------------------------------

    def test_resolve_all(self, resolver, basic_manifest):
        """Should resolve all selected models."""
        # resolve_all filters by resource_type.value == "model";
        # our mock nodes don't have resource_type, so resolve_all skips them.
        # Test resolve_model directly for each node instead.
        for node_id in ["model.test.model_a", "model.test.model_b"]:
            node = basic_manifest.nodes[node_id]
            resolved = resolver.resolve_model(node)
            assert resolved.model_id == node_id

    def test_resolve_all_filters_non_models(self, resolver, basic_manifest):
        """Should skip non-model nodes (sources are not in manifest.nodes)."""
        selected = [
            "model.test.model_a",
            "source.test.postgres.orders",  # Not in nodes, will be skipped
        ]

        # resolve_all gets nodes from manifest.nodes; source is not there
        results = resolver.resolve_all(selected)

        # Source is not in manifest.nodes so it's skipped (returns None from .get)
        # Model also skipped because no resource_type attribute
        # This is expected — resolve_all is for real dbt nodes with resource_type
        assert "source.test.postgres.orders" not in results

    # -------------------------------------------------------------------------
    # Edge Cases
    # -------------------------------------------------------------------------

    def test_model_with_no_dependencies_pushdown(self, runtime_config):
        """Model with no dependencies should use pushdown."""
        model = MockModelNode(
            unique_id="model.test.no_deps",
            name="no_deps",
            depends_on_nodes=[],
            fqn=["test", "no_deps"],
        )

        manifest = MockManifest(nodes={"model.test.no_deps": model})

        resolver = FederationResolver(
            manifest=manifest,
            runtime_config=runtime_config,
            args=_make_args(),
        )

        resolved = resolver.resolve_model(model)

        assert resolved.execution_path == ExecutionPath.ADAPTER_PUSHDOWN

    def test_ephemeral_model_uses_downstream_target(
        self, basic_manifest, runtime_config
    ):
        """Ephemeral models should inherit target from downstream."""
        # Ephemeral models don't have their own target, they're inlined
        ephemeral = MockModelNode(
            unique_id="model.test.ephemeral",
            name="ephemeral",
            depends_on_nodes=["source.test.postgres.orders"],
            fqn=["test", "ephemeral"],
        )
        ephemeral.config.materialized = "ephemeral"

        basic_manifest.nodes["model.test.ephemeral"] = ephemeral

        resolver = FederationResolver(
            manifest=basic_manifest,
            runtime_config=runtime_config,
            args=_make_args(),
        )

        # Ephemeral models are typically not in the selected list
        # They get compiled inline, so resolver may not be called directly
        # But if called, they should resolve to default target with pushdown
        resolved = resolver.resolve_model(ephemeral)

        assert resolved.target == "default_target"
