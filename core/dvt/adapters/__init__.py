"""
dvt.adapters: re-export dbt.adapters so DVT can use the existing dbt-adapters package.
Install dbt-adapters (e.g. dbt-postgres) alongside dvt-core; this module exposes it as dvt.adapters.
"""
import sys

import dbt.adapters as _dbt_adapters

sys.modules["dvt.adapters"] = _dbt_adapters


def _register_submodules(prefix: str, mod):
    """Register mod and all its submodules under dvt.adapters.*."""
    sys.modules[prefix] = mod
    for name in dir(mod):
        if name.startswith("_"):
            continue
        obj = getattr(mod, name)
        if hasattr(obj, "__path__"):
            subprefix = f"{prefix}.{name}"
            if subprefix not in sys.modules:
                _register_submodules(subprefix, obj)


for _name in dir(_dbt_adapters):
    if _name.startswith("_"):
        continue
    _obj = getattr(_dbt_adapters, _name)
    if hasattr(_obj, "__path__"):
        _register_submodules(f"dvt.adapters.{_name}", _obj)
