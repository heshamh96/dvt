# coding=utf-8
"""
Reverse shim: make ``dbt.*`` imports resolve to ``dvt.*`` (or ``dbt_common.*``)
so that third-party adapters (dbt-mysql, dbt-sqlserver, dbt-oracle, etc.) that
still import from ``dbt.exceptions``, ``dbt.version``, ``dbt.contracts.*``, etc.
can work without the upstream ``dbt-core`` package being installed.

``dbt.adapters.*`` is routed to ``dvt.adapters.*`` via the catch-all rule.
The ``dvt-adapters`` package provides the full adapter interface.
``dbt.include.*`` works as a native namespace package and needs no intervention.

Usage
-----
Import this module early (from ``dvt/__init__.py``) so the ``sys.meta_path``
finder is registered before any adapter code runs::

    import dvt.dbt_shim  # noqa: F401  – registers the finder

Mapping rules (in priority order)
---------------------------------
1. ``dbt.include.*``  → skip (namespace package, already on disk)
2. ``dbt_common.*``   → skip (already installed)
3. ``dbt.clients.*``  → ``dbt_common.clients.*``
4. ``dbt.events``     → synthetic module exposing ``AdapterLogger``
5. ``dbt.utils``      → synthetic module merging ``dvt.utils`` + ``dbt_common.utils``
6. ``dbt.contracts.connection`` → ``dvt.adapters.contracts.connection``
7. ``dbt.*``          → ``dvt.*``  (catch-all: adapters, exceptions, version, contracts.*, context.*, etc.)
"""

import importlib
import importlib.abc
import importlib.machinery
import sys
import types
from typing import Optional


# ---------------------------------------------------------------------------
# Explicit mapping overrides.  Keys are *full* module names that should be
# redirected to a target that isn't simply s/dbt/dvt/.
# ---------------------------------------------------------------------------
_EXPLICIT_REDIRECTS = {
    # dbt.clients.* was moved to dbt_common.clients.*
    # (handled by prefix rule below, but listed for documentation)
}


class _DbtShimFinder(importlib.abc.MetaPathFinder):
    """Intercept ``dbt.*`` imports and redirect to ``dvt.*`` or ``dbt_common.*``."""

    # Prefixes we must NOT touch (they have their own resolution mechanism)
    _SKIP_PREFIXES = (
        "dbt.include.",
        "dbt_common.",
        "dbt_adapters.",
    )

    # Exact top-level names we must NOT touch
    _SKIP_EXACT = frozenset(
        {
            "dbt.include",
        }
    )

    def find_spec(self, fullname: str, path, target=None):
        # Only handle dbt.* (but not dbt_common, dbt_adapters, dbt_extractor, …)
        if not fullname.startswith("dbt."):
            return None

        # Skip namespaces handled elsewhere
        if fullname in self._SKIP_EXACT:
            return None
        for prefix in self._SKIP_PREFIXES:
            if fullname.startswith(prefix):
                return None

        # Already resolved — nothing to do
        if fullname in sys.modules:
            return None

        target_name = self._resolve(fullname)
        if target_name is None:
            return None

        return importlib.machinery.ModuleSpec(
            fullname,
            _DbtShimLoader(target_name),
        )

    @staticmethod
    def _resolve(fullname: str) -> Optional[str]:
        """Return the real module name to import, or None to skip."""

        # --- Rule 4: dbt.clients.* → dbt_common.clients.* ---
        if fullname.startswith("dbt.clients."):
            return "dbt_common.clients." + fullname[len("dbt.clients.") :]
        if fullname == "dbt.clients":
            return "dbt_common.clients"

        # --- Rule 6: dbt.contracts.connection → dvt.adapters.contracts.connection ---
        if fullname == "dbt.contracts.connection":
            return "dvt.adapters.contracts.connection"

        # --- Rule 5: dbt.events → synthetic (handled specially in loader) ---
        if fullname == "dbt.events":
            return "__synthetic__.dbt.events"

        # --- Rule 6: dbt.utils → synthetic (handled specially in loader) ---
        if fullname == "dbt.utils":
            return "__synthetic__.dbt.utils"

        # --- Rule 8: catch-all dbt.* → dvt.* ---
        return "dvt" + fullname[3:]


class _DbtShimLoader(importlib.abc.Loader):
    """Load a ``dbt.*`` name by importing its resolved target."""

    def __init__(self, target_name: str):
        self.target_name = target_name

    def create_module(self, spec):
        if self.target_name == "__synthetic__.dbt.events":
            return self._create_events_module(spec.name)
        if self.target_name == "__synthetic__.dbt.utils":
            return self._create_utils_module(spec.name)

        mod = importlib.import_module(self.target_name)
        sys.modules[spec.name] = mod

        # When dvt.adapters is first loaded, eagerly extend its __path__
        # to include site-packages/dbt/adapters/ where third-party adapter
        # plugins are installed.  This must happen before any relative
        # import like ``import_module('.postgres', 'dvt.adapters')``
        # because Python's PathFinder resolves subpackages via __path__
        # without consulting sys.meta_path finders.
        if self.target_name == "dvt.adapters":
            _DvtAdaptersFallbackFinder._extend_path()

        return mod

    def exec_module(self, module):
        # Module is already fully initialised.
        pass

    # -- Synthetic module builders -----------------------------------------

    @staticmethod
    def _create_events_module(name: str) -> types.ModuleType:
        """Build a synthetic ``dbt.events`` that exposes ``AdapterLogger``.

        Third-party adapters do ``from dbt.events import AdapterLogger``.
        In the new architecture, ``AdapterLogger`` lives in
        ``dbt.adapters.events.logging``.
        """
        mod = types.ModuleType(
            name, "Shim: dbt.events -> AdapterLogger from dbt-adapters"
        )
        try:
            from dvt.adapters.events.logging import AdapterLogger  # type: ignore[attr-defined]

            mod.AdapterLogger = AdapterLogger  # type: ignore[attr-defined]
        except ImportError:
            pass
        sys.modules[name] = mod
        return mod

    @staticmethod
    def _create_utils_module(name: str) -> types.ModuleType:
        """Build a synthetic ``dbt.utils`` that merges ``dvt.utils`` + ``dbt_common.utils``.

        Third-party adapters do ``from dbt.utils import executor``.
        ``executor`` is in ``dbt_common.utils``.  Other utils may be in ``dvt.utils``.
        """
        mod = types.ModuleType(name, "Shim: dbt.utils -> dvt.utils + dbt_common.utils")

        # Layer 1: everything from dbt_common.utils
        try:
            import dbt_common.utils as _dcu

            for attr in dir(_dcu):
                if not attr.startswith("_"):
                    setattr(mod, attr, getattr(_dcu, attr))
        except ImportError:
            pass

        # Layer 2: overlay with dvt.utils (dvt takes precedence)
        try:
            import dvt.utils as _dvtu

            for attr in dir(_dvtu):
                if not attr.startswith("_"):
                    setattr(mod, attr, getattr(_dvtu, attr))
        except ImportError:
            pass

        sys.modules[name] = mod
        return mod


# ---------------------------------------------------------------------------
# Fallback finder: dvt.adapters.<plugin> → dbt.adapters.<plugin> (from pip)
# ---------------------------------------------------------------------------
# The shim maps ``dbt.adapters.*`` → ``dvt.adapters.*``.  Core modules
# (factory, base, sql, etc.) resolve to dvt-adapters.  But adapter plugins
# (postgres, snowflake, mysql, etc.) have no local ``dvt.adapters.<plugin>``
# — they're installed as ``dbt.adapters.<plugin>`` by pip.  This finder
# catches those misses and loads from pip by temporarily disabling the shim.
#
# NOTE: This used to live in ``core/dvt/adapters/__init__.py`` but
# ``extend_path`` namespace packages only execute the *first* ``__init__.py``
# on the path (from dvt-adapters), so this code never ran.  Moving it here
# ensures it's always registered.
# ---------------------------------------------------------------------------


class _DvtAdaptersFallbackFinder(importlib.abc.MetaPathFinder):
    """Extend ``dvt.adapters.__path__`` to include pip's ``dbt/adapters/`` dirs.

    Third-party dbt adapter plugins (dbt-postgres, dbt-snowflake, etc.) install
    their code into ``site-packages/dbt/adapters/<plugin>/``.  The shim maps
    ``dbt.adapters.*`` → ``dvt.adapters.*``, but ``dvt.adapters.__path__`` only
    covers dvt-adapters and dvt-core paths.

    This finder runs once: on the first miss for ``dvt.adapters.<X>``, it
    discovers all ``dbt/adapters/`` directories in ``sys.path`` / site-packages
    and appends them to ``dvt.adapters.__path__``.  After that, normal Python
    import machinery finds the plugins under the extended path — no further
    interception needed.
    """

    _extended = False

    def find_spec(self, fullname, path, target=None):
        if not fullname.startswith("dvt.adapters."):
            return None
        if fullname in sys.modules:
            return None

        if not self._extended:
            self._extend_path()

        # After extending the path, use importlib to find the spec.
        # We must temporarily remove ourselves from sys.meta_path to
        # avoid infinite recursion, then use the standard finders.
        try:
            sys.meta_path.remove(self)
        except ValueError:
            pass

        try:
            import importlib.util

            spec = importlib.util.find_spec(fullname)
            return spec
        except (ImportError, ModuleNotFoundError, ValueError):
            return None
        finally:
            if self not in sys.meta_path:
                sys.meta_path.append(self)

    @classmethod
    def _extend_path(cls):
        """Add ``dbt/adapters`` directories from sys.path to ``dvt.adapters.__path__``.

        Uses ``sys.modules`` to get the already-imported ``dvt.adapters`` module
        rather than ``import dvt.adapters`` to avoid circular import issues when
        called during ``dvt/__init__.py`` execution.
        """
        cls._extended = True

        import os
        import site

        _dva = sys.modules.get("dvt.adapters")
        if _dva is None:
            # Not imported yet — will be retried on next find_spec call.
            cls._extended = False
            return

        existing = set(getattr(_dva, "__path__", []))

        # Gather candidate dbt/adapters directories from site-packages and sys.path
        candidates = []
        try:
            candidates.extend(
                os.path.join(sp, "dbt", "adapters") for sp in site.getsitepackages()
            )
        except AttributeError:
            pass
        try:
            candidates.append(
                os.path.join(site.getusersitepackages(), "dbt", "adapters")
            )
        except AttributeError:
            pass
        for sp_dir in sys.path:
            if sp_dir:
                candidates.append(os.path.join(sp_dir, "dbt", "adapters"))

        for dbt_adapters_dir in candidates:
            if dbt_adapters_dir not in existing and os.path.isdir(dbt_adapters_dir):
                _dva.__path__.append(dbt_adapters_dir)
                existing.add(dbt_adapters_dir)


# Register both finders.
# The shim finder goes early (position 1) so third-party ``dbt.*`` imports
# get redirected before anything else.
# The fallback finder should be LAST so normal resolution is tried first.
sys.meta_path.insert(1, _DbtShimFinder())
sys.meta_path.append(_DvtAdaptersFallbackFinder())
