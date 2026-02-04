# Using local dvt-core in your project with uv

Use your local dvt-core repo (with the latest fixes) in another project via `uv sync`, **without** an editable install.

## 1. Build dvt-core (optional)

From the repo root:

```bash
cd core
uv build
```

This produces `core/dist/dvt_core-<version>-py3-none-any.whl`. You can skip this step if you use the path-to-source method below; uv will build when you sync.

## 2. Point your project at local dvt-core

In your project’s `pyproject.toml` (e.g. Fanta_DB or any trial project):

1. Add `dvt-core` as a dependency if it isn’t already:

   ```toml
   [project]
   dependencies = [
     "dvt-core",
     # ... your other deps
   ]
   ```

2. Add a **path source** so uv installs from your local repo (non-editable). **You must include `editable = false`**—without it, the install and `dvt sync --help` (minimal flags) may not behave correctly:

   ```toml
   [tool.uv.sources]
   dvt-core = { path = "/full/path/to/dvt-core/core", editable = false }
   ```

   Use the **absolute path** to the `core` directory inside the dvt-core repo (e.g. `/Users/hex/Documents/My_Projects/DVT/dvt-core/core`). Always run **`uv run dvt ...`** from the project root (never bare `dvt`).

## 3. Sync in your project

From your project directory:

```bash
uv sync
```

uv will build and install dvt-core from that path (non-editable). Then:

```bash
uv run dvt sync --help   # minimal options + --python-env
uv run dvt sync
```

## Using a pre-built wheel instead

If you prefer to install from a wheel you already built:

1. Build once: `cd core && uv build`
2. In your project’s `pyproject.toml`:

   ```toml
   [project]
   dependencies = [
     "dvt-core",
     # ...
   ]

   [tool.uv.sources]
   dvt-core = { path = "/full/path/to/dvt-core/core/dist/dvt_core-1.12.0a1-py3-none-any.whl" }
   ```

   (Update the version in the filename to match `core/dist/` after each build.)

3. Run `uv sync` in your project.

After you bump the version in dvt-core, rebuild and update the path if you use the wheel file.
