# N.B.
# This will add to the package's __path__ all subdirectories of directories on sys.path named after the package which effectively combines both modules into a single namespace (dvt.adapters)
# The matching statement is in plugins/postgres/dvt/__init__.py

from pkgutil import extend_path

__path__ = extend_path(__path__, __name__)

# Register the dbt.* → dvt.* reverse shim so third-party adapters
# (dbt-mysql, dbt-sqlserver, dbt-oracle, etc.) can resolve imports like
# ``dbt.exceptions``, ``dbt.version``, ``dbt.contracts.*`` without the
# upstream ``dbt-core`` package being installed.
import dvt.dbt_shim  # noqa: F401, E402

# Extend dvt.adapters.__path__ to include site-packages/dbt/adapters/
# where third-party dbt adapter plugins install their code.  This MUST
# happen after the shim is registered (above) and before any adapter
# import.  Python's PathFinder resolves subpackage imports via __path__
# directly without consulting sys.meta_path finders.
dvt.dbt_shim._DvtAdaptersFallbackFinder._extend_path()  # noqa: E402
