# Running DVT (test team and local dev)

dvt-core is a **Python package**. Test team and local dev must **build and install the package in development mode** (using **uv**), then run the installed `dvt` CLI. Do not rely on standalone scripts that bypass the package.

Use an environment where **Python and dependencies match your CPU architecture** (arm64 on Apple Silicon, x86_64 on Intel); otherwise native extensions (e.g. `rpds`) can raise "incompatible architecture" errors.

## Quick setup (uv, recommended)

1. **Install dvt-core in dev mode** with uv:

   ```bash
   cd /path/to/dvt-core/core
   uv sync
   ```

2. **Run dvt** from the directory where you want the project (e.g. Testing_Playground):

   ```bash
   cd /Users/hex/Documents/My_Projects/DVT/Testing_Playground
   uv run --project /path/to/dvt-core/core dvt init Coke_DB --skip-profile-setup
   ```

   Or activate the uv env and run `dvt` directly:

   ```bash
   source /path/to/dvt-core/core/.venv/bin/activate
   cd /path/to/Testing_Playground
   dvt init Coke_DB --skip-profile-setup
   ```

## Test team: each trial is a uv-contained project

Test-team agents run in **Testing_Playground** (`/Users/hex/Documents/My_Projects/DVT/Testing_Playground`). See that folder’s `README.md` and `.cursor/rules/test-team-*.mdc` for roles and scenarios.

## Troubleshooting

- **"mach-o file, but is an incompatible architecture"**  
  Your Python or a dependency is the wrong arch. Use a venv created with a native `python3` and run `pip install -e .` (or `uv sync`) inside it so extensions are built for that arch.
- **"No module named 'dvt.adapters'"**  
  Install an adapter package (e.g. `dbt-postgres`) alongside dvt-core. DVT exposes it as `dvt.adapters` via the shim in `core/dvt/adapters/`.
