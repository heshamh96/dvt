"""
dvt.adapters — namespace package.

Adapter infrastructure (factory, base, sql, contracts, events, etc.) is
provided by the ``dvt-adapters`` package.  Third-party dbt adapter plugins
(dbt-postgres, dbt-snowflake, dbt-mysql, etc.) install into the ``dbt.adapters``
namespace on pip.  The dbt_shim routes ``dbt.adapters.*`` to ``dvt.adapters.*``.

For adapter plugin subpackages that don't exist locally (postgres, snowflake,
mysql, oracle, databricks, sqlserver, etc.), the fallback finder in
``dvt.dbt_shim`` resolves them from the pip-installed ``dbt.adapters.<plugin>``
by temporarily bypassing the shim.

NOTE: With ``extend_path``, only the first ``__init__.py`` on the namespace path
actually executes.  Since dvt-adapters is first, this file (from dvt-core) may
NOT run.  All import-hook registration therefore lives in ``dvt.dbt_shim``
which is imported early from ``dvt/__init__.py``.
"""

from pkgutil import extend_path

__path__ = extend_path(__path__, __name__)
