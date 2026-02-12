"""DVT CompileTask — extends dbt CompileTask with cross-dialect SQL transpilation.

Wires DvtCompiler into the compile path so that ``dvt compile`` transpiles
compiled SQL from the source dialect to the target dialect (e.g., Postgres
double-quotes → Databricks backticks) when they differ.

The base CompileTask/CompileRunner use the standard dbt Compiler which has
no transpilation support.  DvtCompileRunner overrides the runner-level
compiler with DvtCompiler, and DvtCompileTask returns DvtCompileRunner.
"""

from __future__ import annotations

from typing import Optional, Type

from dvt.dvt_compilation.dvt_compiler import DvtCompiler
from dvt.task.base import BaseRunner
from dvt.task.compile import CompileRunner, CompileTask


class DvtCompileRunner(CompileRunner):
    """CompileRunner that uses DvtCompiler for cross-dialect transpilation."""

    def __init__(self, config, adapter, node, node_index, num_nodes) -> None:
        super().__init__(config, adapter, node, node_index, num_nodes)
        self.compiler = DvtCompiler(config)


class DvtCompileTask(CompileTask):
    """CompileTask that uses DvtCompileRunner for cross-dialect transpilation."""

    def get_runner_type(self, _) -> Optional[Type[BaseRunner]]:
        return DvtCompileRunner
