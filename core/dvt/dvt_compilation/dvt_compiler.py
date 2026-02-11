"""DVT-specific Compiler with target-aware compilation.

Extends the standard dbt Compiler to support compiling models with
a non-default adapter.  When a model targets a different database
(e.g., Databricks when the default is Postgres), the adapter parameter
ensures Jinja resolves {{ ref() }} and {{ source() }} with the correct
dialect — quoting, relation format, etc.

When ``adapter`` is None (the default), behaviour is identical to the
standard dbt Compiler.
"""

from typing import Any, Dict, List, Optional, Tuple

from dvt.compilation import Compiler
from dvt.context.providers import generate_runtime_model_context
from dvt.contracts.graph.manifest import Manifest
from dvt.contracts.graph.nodes import (
    InjectedCTE,
    ManifestSQLNode,
    SeedNode,
    UnitTestNode,
)
from dvt.exceptions import DbtInternalError, DbtRuntimeError


class DvtCompiler(Compiler):
    """Compiler that supports target-aware compilation via an ``adapter``
    parameter threaded through the compilation chain.

    All public methods mirror the base ``Compiler`` API with an additional
    optional ``adapter`` keyword argument.  When ``adapter`` is ``None``,
    the behaviour is identical to the standard dbt ``Compiler``.
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
        """Compile a single node, optionally with a non-default adapter.

        The ``adapter`` parameter enables target-aware compilation.  When
        provided, Jinja rendering uses this adapter's Relation class and
        dialect instead of the default adapter.
        """
        from dvt.contracts.graph.nodes import UnitTestDefinition

        if isinstance(node, UnitTestDefinition):
            return node

        # Make sure Lexer for sqlparse 0.4.4 is initialized
        from sqlparse.lexer import Lexer  # type: ignore

        if hasattr(Lexer, "get_default_instance"):
            Lexer.get_default_instance()

        node = self._compile_code(node, manifest, extra_context, adapter=adapter)

        node, _ = self._recursively_prepend_ctes(
            node, manifest, extra_context, adapter=adapter
        )
        if write:
            self._write_node(node, split_suffix=split_suffix)
        return node

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
