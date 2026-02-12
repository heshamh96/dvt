"""DVT-specific Compiler with source-dialect Jinja rendering and SQL transpilation.

Extends the standard dbt Compiler to support cross-dialect compilation:

1. **Source-dialect Jinja rendering** — ``{{ ref() }}`` and ``{{ source() }}``
   are rendered using the **source** adapter (the dialect the user wrote their
   SQL in), producing compiled SQL that is entirely in the source dialect.
   This ensures consistency: user-written SQL and adapter-generated relation
   names are in the same dialect.

2. **SQL dialect transpilation** — after Jinja rendering, the compiled SQL
   (now uniformly in the source dialect) is transpiled to the target dialect
   via SQLGlot.  This handles *all* dialect differences in one pass:
   identifier quoting (``"double quotes"`` → `` `backticks` ``),
   cast syntax (``::int`` → ``CAST(... AS INT)``), function names, etc.

   Source dialect = what the user wrote their SQL in:
   - If the model has ``config(target='X')``, source = adapter type of target X
   - Otherwise, source = adapter type of the profile's YAML default target

   Target dialect = what the SQL will execute on:
   - Determined by ``--target`` CLI flag or ``config.credentials.type``

   When source == target dialect, no transpilation occurs (zero overhead),
   and Jinja renders with the default adapter (standard dbt behavior).

When ``adapter`` is None and source == target, behaviour is identical to
the standard dbt Compiler.
"""

import logging
from typing import Any, Dict, List, Optional, Tuple

from dvt.compilation import Compiler
from dvt.contracts.graph.manifest import Manifest
from dvt.contracts.graph.nodes import (
    InjectedCTE,
    ManifestSQLNode,
    SeedNode,
    UnitTestNode,
)
from dvt.exceptions import DbtInternalError, DbtRuntimeError

logger = logging.getLogger(__name__)


class DvtCompiler(Compiler):
    """Compiler with source-dialect Jinja rendering and cross-dialect SQL transpilation.

    Compilation flow:
    1. Resolve the *source adapter* (dialect the user wrote SQL in)
    2. Jinja-render with the source adapter → consistent source-dialect SQL
    3. Inject CTEs (also with source adapter)
    4. SQLGlot transpile source → target dialect (if they differ)
    5. Write compiled output
    """

    # ------------------------------------------------------------------
    # compile_node — main entry point
    # ------------------------------------------------------------------

    def compile_node(
        self,
        node: ManifestSQLNode,
        manifest: Manifest,
        extra_context: Optional[Dict[str, Any]] = None,
        write: bool = True,
        split_suffix: Optional[str] = None,
        adapter: Optional[Any] = None,
    ) -> ManifestSQLNode:
        """Compile a single node with cross-dialect support.

        Jinja rendering uses the **source** adapter so that ``{{ ref() }}``
        and ``{{ source() }}`` produce relation names in the same dialect as
        the user's hand-written SQL.  After rendering, SQLGlot transpiles
        the uniformly source-dialect SQL to the target dialect.

        Parameters
        ----------
        adapter : optional
            Explicit target adapter (e.g., from NonDefaultPushdownRunner).
            Used for transpile target resolution.  When None, the global
            default adapter is used as the target.
        """
        from dvt.contracts.graph.nodes import UnitTestDefinition

        if isinstance(node, UnitTestDefinition):
            return node

        # Make sure Lexer for sqlparse 0.4.4 is initialized
        from sqlparse.lexer import Lexer  # type: ignore

        if hasattr(Lexer, "get_default_instance"):
            Lexer.get_default_instance()

        # DVT: Resolve source adapter for Jinja rendering.
        # When source == target dialect, source_adapter is None (no override).
        source_adapter = self._resolve_source_adapter(node)
        jinja_adapter = source_adapter if source_adapter is not None else adapter

        node = self._compile_code(node, manifest, extra_context, adapter=jinja_adapter)

        node, _ = self._recursively_prepend_ctes(
            node, manifest, extra_context, adapter=jinja_adapter
        )

        # DVT: Transpile compiled SQL from source dialect to target dialect
        node = self._transpile_if_needed(node, adapter=adapter)

        if write:
            self._write_node(node, split_suffix=split_suffix)
        return node

    # ------------------------------------------------------------------
    # _transpile_if_needed — cross-dialect SQL transpilation
    # ------------------------------------------------------------------

    def _transpile_if_needed(
        self,
        node: ManifestSQLNode,
        adapter: Optional[Any] = None,
    ) -> ManifestSQLNode:
        """Transpile compiled SQL from source to target dialect if they differ.

        Source dialect: determined by the model's config.target (if set) or
        the profile's YAML default target.

        Target dialect: the adapter type of the current compilation target
        (i.e., what the SQL will actually execute on).

        If source == target, this is a no-op.
        """
        from dvt.node_types import NodeType

        # Only transpile SQL models
        if node.resource_type != NodeType.Model:
            return node
        if not node.compiled_code:
            return node

        # Skip Python models
        from dvt.node_types import ModelLanguage

        if getattr(node, "language", None) == ModelLanguage.python:
            return node

        source_adapter_type = self._resolve_source_adapter_type(node)
        target_adapter_type = self._resolve_target_adapter_type(adapter)

        if not source_adapter_type or not target_adapter_type:
            return node

        from dvt.federation.dialect_fallbacks import get_dialect_for_adapter

        source_dialect = get_dialect_for_adapter(source_adapter_type)
        target_dialect = get_dialect_for_adapter(target_adapter_type)

        # No-op when dialects match
        if source_dialect == target_dialect:
            return node

        try:
            import sqlglot

            transpiled_statements = sqlglot.transpile(
                node.compiled_code,
                read=source_dialect,
                write=target_dialect,
                pretty=True,
            )
            transpiled_sql = ";\n".join(transpiled_statements)

            logger.debug(
                "Transpiled %s from %s to %s (%d chars → %d chars)",
                node.unique_id,
                source_dialect,
                target_dialect,
                len(node.compiled_code),
                len(transpiled_sql),
            )
            node.compiled_code = transpiled_sql

        except Exception as e:
            # If transpilation fails, log a warning and keep the original SQL.
            # The SQL may still work if the dialect differences are minor,
            # or the user may need to adjust their SQL.
            logger.warning(
                "SQLGlot transpilation failed for %s (%s → %s): %s. "
                "Using original compiled SQL.",
                node.unique_id,
                source_dialect,
                target_dialect,
                str(e),
            )

        return node

    # ------------------------------------------------------------------
    # _resolve_source_adapter_type — what dialect the SQL was written in
    # ------------------------------------------------------------------

    def _resolve_source_adapter_type(self, node: ManifestSQLNode) -> Optional[str]:
        """Determine the adapter type that the model's SQL was written for.

        Priority:
        1. model.config.target → look up adapter type for that target
        2. YAML default target (read from profiles.yml) → look up adapter type
        3. Fall back to config.credentials.type
        """
        # Check model-level target config
        model_target = getattr(getattr(node, "config", None), "target", None)
        if model_target:
            return self._get_adapter_type_for_target(model_target)

        # Read the YAML default target directly from profiles.yml
        yaml_default_target = self._get_yaml_default_target()
        if yaml_default_target:
            return self._get_adapter_type_for_target(yaml_default_target)

        # Last resort: use the current config's adapter type
        return getattr(getattr(self.config, "credentials", None), "type", None)

    # ------------------------------------------------------------------
    # _resolve_target_adapter_type — what dialect we're compiling toward
    # ------------------------------------------------------------------

    def _resolve_target_adapter_type(
        self, adapter: Optional[Any] = None
    ) -> Optional[str]:
        """Determine the adapter type of the compilation target."""
        if adapter is not None:
            # Explicit adapter passed (e.g., NonDefaultPushdownRunner)
            adapter_type = getattr(adapter, "type", None)
            if callable(adapter_type):
                return adapter_type()
            return adapter_type

        # Default: use the config's current adapter type
        return getattr(getattr(self.config, "credentials", None), "type", None)

    # ------------------------------------------------------------------
    # _get_profiles_dir — resolve profiles directory
    # ------------------------------------------------------------------

    def _get_profiles_dir(self) -> str:
        """Resolve the profiles directory from args or default ~/.dvt."""
        profiles_dir = getattr(self.config.args, "PROFILES_DIR", None)
        if not profiles_dir:
            profiles_dir = getattr(self.config.args, "profiles_dir", None)
        if not profiles_dir:
            from pathlib import Path

            profiles_dir = str(Path.home() / ".dvt")
        return profiles_dir

    # ------------------------------------------------------------------
    # _read_raw_profile — read and cache the raw profile dict
    # ------------------------------------------------------------------

    def _read_raw_profile(self) -> Optional[Dict[str, Any]]:
        """Read the raw profile dict from profiles.yml (cached per compile session)."""
        if hasattr(self, "_cached_raw_profile"):
            return self._cached_raw_profile

        try:
            from dvt.config.profile import read_profile

            raw_profiles = read_profile(self._get_profiles_dir())
            profile_name = self.config.profile_name
            self._cached_raw_profile = raw_profiles.get(profile_name)
        except Exception as e:
            logger.debug("Could not read profiles.yml: %s", str(e))
            self._cached_raw_profile = None

        return self._cached_raw_profile

    # ------------------------------------------------------------------
    # _get_yaml_default_target — read the YAML default target name
    # ------------------------------------------------------------------

    def _get_yaml_default_target(self) -> Optional[str]:
        """Read the 'target' key from the profile in profiles.yml.

        This is the YAML default target *before* any --target CLI override.
        """
        raw_profile = self._read_raw_profile()
        if raw_profile and "target" in raw_profile:
            return raw_profile["target"]
        return None

    # ------------------------------------------------------------------
    # _get_adapter_type_for_target — look up adapter type from profiles
    # ------------------------------------------------------------------

    def _get_adapter_type_for_target(self, target_name: str) -> Optional[str]:
        """Look up the adapter type (e.g., 'postgres', 'databricks') for a
        given target name.

        Fast path: if target matches the current config's target, returns
        config.credentials.type (no YAML read needed).
        Slow path: reads outputs from the cached raw profile.
        """
        # Fast path: if target matches current config, use credentials
        if target_name == self.config.target_name:
            return getattr(getattr(self.config, "credentials", None), "type", None)

        # Slow path: look up from raw profile outputs
        try:
            raw_profile = self._read_raw_profile()
            if raw_profile:
                outputs = raw_profile.get("outputs", {})
                if target_name in outputs:
                    return outputs[target_name].get("type")
        except Exception as e:
            logger.warning(
                "Could not look up adapter type for target '%s': %s",
                target_name,
                str(e),
            )

        return None

    # ------------------------------------------------------------------
    # _get_source_target_name — which target the user wrote SQL for
    # ------------------------------------------------------------------

    def _get_source_target_name(self, node: ManifestSQLNode) -> Optional[str]:
        """Return the target name whose dialect the user wrote their SQL in.

        Priority:
        1. model.config.target (explicit model-level target)
        2. YAML default target from profiles.yml
        """
        model_target = getattr(getattr(node, "config", None), "target", None)
        if model_target:
            return model_target
        return self._get_yaml_default_target()

    # ------------------------------------------------------------------
    # _resolve_source_adapter — get adapter for source dialect
    # ------------------------------------------------------------------

    def _resolve_source_adapter(self, node: ManifestSQLNode) -> Optional[Any]:
        """Get the adapter instance for the dialect the user wrote their SQL in.

        Returns None if:
        - source == current target (no override needed, same dialect)
        - the source adapter cannot be created (graceful fallback)
        - the node is not a SQL Model

        When non-None, this adapter should be used for Jinja rendering so
        that ``{{ ref() }}`` and ``{{ source() }}`` produce relation names
        in the same dialect as the user's hand-written SQL.
        """
        from dvt.node_types import ModelLanguage, NodeType

        # Only override for SQL models
        if node.resource_type != NodeType.Model:
            return None
        if getattr(node, "language", None) == ModelLanguage.python:
            return None

        source_target = self._get_source_target_name(node)
        if not source_target:
            return None

        # If source target matches the current compilation target, no override
        if source_target == self.config.target_name:
            return None

        # Source and target differ — create the source adapter via AdapterManager
        try:
            from dvt.federation.adapter_manager import AdapterManager

            source_adapter = AdapterManager.get_adapter(
                profile_name=self.config.profile_name,
                target_name=source_target,
                profiles_dir=self._get_profiles_dir(),
            )
            logger.debug(
                "Using source adapter '%s' for Jinja rendering of %s "
                "(target adapter: '%s')",
                source_target,
                node.unique_id,
                self.config.target_name,
            )
            return source_adapter
        except Exception as e:
            logger.warning(
                "Could not create source adapter for target '%s': %s. "
                "Falling back to default adapter for Jinja rendering.",
                source_target,
                str(e),
            )
            return None

    # ------------------------------------------------------------------
    # _create_node_context — context creation with optional adapter
    # ------------------------------------------------------------------

    def _create_node_context(
        self,
        node: ManifestSQLNode,
        manifest: Manifest,
        extra_context: Dict[str, Any],
        adapter: Optional[Any] = None,
    ) -> Dict[str, Any]:
        from dvt.context.providers import (
            generate_runtime_model_context,
            generate_runtime_unit_test_context,
        )
        from dvt.clients import jinja
        from dvt.contracts.graph.nodes import GenericTestNode

        if isinstance(node, UnitTestNode):
            context = generate_runtime_unit_test_context(node, self.config, manifest)
        else:
            # DVT: Pass explicit adapter for target-aware compilation.
            # When adapter is None, the default adapter is used (standard dbt behavior).
            context = generate_runtime_model_context(
                node, self.config, manifest, adapter=adapter
            )
        context.update(extra_context)

        if isinstance(node, GenericTestNode):
            jinja.add_rendered_test_kwargs(context, node)

        return context

    # ------------------------------------------------------------------
    # _compile_code — Jinja rendering with optional adapter
    # ------------------------------------------------------------------

    def _compile_code(
        self,
        node: ManifestSQLNode,
        manifest: Manifest,
        extra_context: Optional[Dict[str, Any]] = None,
        adapter: Optional[Any] = None,
    ) -> ManifestSQLNode:
        from dvt.adapters.factory import get_adapter
        from dvt.clients import jinja
        from dvt.contracts.graph.nodes import ModelNode
        from dvt.node_types import ModelLanguage, NodeType
        from dbt_common.contracts.constraints import ConstraintType

        if extra_context is None:
            extra_context = {}

        if (
            node.language == ModelLanguage.python
            and node.resource_type == NodeType.Model
        ):
            context = self._create_node_context(
                node, manifest, extra_context, adapter=adapter
            )

            postfix = jinja.get_rendered(
                "{{ py_script_postfix(model) }}",
                context,
                node,
            )
            node.compiled_code = f"{node.raw_code}\n\n{postfix}"

        else:
            context = self._create_node_context(
                node, manifest, extra_context, adapter=adapter
            )
            node.compiled_code = jinja.get_rendered(
                node.raw_code,
                context,
                node,
            )

        node.compiled = True

        # relation_name is set at parse time, except for tests without store_failures,
        # but cli param can turn on store_failures, so we set here.
        if (
            node.resource_type == NodeType.Test
            and node.relation_name is None
            and node.is_relational
        ):
            adapter_local = get_adapter(self.config)
            relation_cls = adapter_local.Relation
            relation_name = str(relation_cls.create_from(self.config, node))
            node.relation_name = relation_name

        # Compile 'ref' and 'source' expressions in foreign key constraints
        if isinstance(node, ModelNode):
            for constraint in node.all_constraints:
                if constraint.type == ConstraintType.foreign_key and constraint.to:
                    constraint.to = (
                        self._compile_relation_for_foreign_key_constraint_to(
                            manifest, node, constraint.to
                        )
                    )

        return node

    # ------------------------------------------------------------------
    # _recursively_prepend_ctes — CTE handling with optional adapter
    # ------------------------------------------------------------------

    def _recursively_prepend_ctes(
        self,
        model: ManifestSQLNode,
        manifest: Manifest,
        extra_context: Optional[Dict[str, Any]],
        adapter: Optional[Any] = None,
    ) -> Tuple[ManifestSQLNode, List[InjectedCTE]]:
        from dvt.compilation import (
            _add_prepended_cte,
            _extend_prepended_ctes,
            inject_ctes_into_sql,
        )
        from dvt.contracts.graph.nodes import UnitTestSourceDefinition
        from dvt.flags import get_flags

        if model.compiled_code is None:
            raise DbtRuntimeError("Cannot inject ctes into an uncompiled node", model)

        if not getattr(self.config.args, "inject_ephemeral_ctes", True):
            return (model, [])

        if model.extra_ctes_injected:
            return (model, model.extra_ctes)

        if len(model.extra_ctes) == 0:
            if not isinstance(model, SeedNode):
                model.extra_ctes_injected = True
            return (model, [])

        prepended_ctes: List[InjectedCTE] = []

        for cte in model.extra_ctes:
            if cte.id not in manifest.nodes:
                raise DbtInternalError(
                    f"During compilation, found a cte reference that "
                    f"could not be resolved: {cte.id}"
                )
            cte_model = manifest.nodes[cte.id]
            assert not isinstance(cte_model, SeedNode)

            if not cte_model.is_ephemeral_model:
                raise DbtInternalError(f"{cte.id} is not ephemeral")

            if cte_model.compiled is True and cte_model.extra_ctes_injected is True:
                new_prepended_ctes = cte_model.extra_ctes
            else:
                cte_model = self._compile_code(
                    cte_model, manifest, extra_context, adapter=adapter
                )
                cte_model, new_prepended_ctes = self._recursively_prepend_ctes(
                    cte_model, manifest, extra_context, adapter=adapter
                )
                self._write_node(cte_model)

            _extend_prepended_ctes(prepended_ctes, new_prepended_ctes)

            cte_name = (
                cte_model.cte_name
                if isinstance(cte_model, UnitTestSourceDefinition)
                else cte_model.identifier
            )
            new_cte_name = self.add_ephemeral_prefix(cte_name)
            rendered_sql = cte_model._pre_injected_sql or cte_model.compiled_code
            sql = f" {new_cte_name} as (\n{rendered_sql}\n)"

            _add_prepended_cte(prepended_ctes, InjectedCTE(id=cte.id, sql=sql))

        if not model.extra_ctes_injected:
            injected_sql = inject_ctes_into_sql(
                model.compiled_code,
                prepended_ctes,
            )
            model.extra_ctes_injected = True
            model._pre_injected_sql = model.compiled_code
            model.compiled_code = injected_sql
            model.extra_ctes = prepended_ctes

        return model, model.extra_ctes
