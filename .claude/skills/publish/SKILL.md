# Skill: publish

Build and publish DVT releases to PyPI from the `master` branch.

## Usage

- `/publish <version>` — Build and publish the specified version (e.g., `/publish 1.12.104`)

## Packages

DVT ships as two packages:
- **dvt-ce** — Core engine (from `dvt-ce/core/`)
- **dvt-adapters** — All database adapters in one package (from `dvt-adapters/`)

## Prerequisites

- Must be on `master` branch
- Version in `core/setup.py` must match the argument
- All changes committed (clean working tree)
- `python3 -m build` and `python3 -m twine` must be available
- PyPI credentials configured (token will be prompted by twine)

## Steps

### 1. Pre-flight Checks

```bash
# Verify on master branch
git branch --show-current   # must be "master"

# Verify clean working tree
git status --porcelain       # must be empty

# Verify version matches
grep "version=" core/setup.py  # must show version = "<argument>"

# Verify LICENSE and NOTICE exist
ls core/LICENSE core/NOTICE
```

If any check fails, STOP and report the issue. Do NOT proceed.

### 2. Run Unit Tests

```bash
cd core && .venv/bin/python -m pytest ../tests/unit/federation/ --tb=short -q
```

All tests must pass. If any fail, STOP.

### 3. Build dvt-ce Package

```bash
cd core
rm -rf dist/                          # clean previous builds
python3 -m build                       # produces sdist + wheel in dist/
```

Expected output:
- `dist/dvt_ce-<version>.tar.gz` (sdist)
- `dist/dvt_ce-<version>-py3-none-any.whl` (wheel)

### 4. Verify Package Contents

```bash
# Check LICENSE and NOTICE are included in sdist
tar tf dist/dvt_ce-<version>.tar.gz | grep -E "LICENSE|NOTICE"

# Check wheel metadata
unzip -l dist/dvt_ce-<version>-py3-none-any.whl | grep -E "LICENSE|NOTICE"
```

Both LICENSE and NOTICE must appear in both archives.

### 5. Upload dvt-ce to PyPI

```bash
cd core
python3 -m twine upload dist/dvt_ce-<version>*
```

Twine will prompt for credentials if not configured in `~/.pypirc`.
- Username: `__token__`
- Password: your PyPI API token (starts with `pypi-`)

### 6. Build and Upload dvt-adapters

```bash
cd /Users/hex/Documents/My_Projects/DVT/dvt-adapters
rm -rf dist/
python3 -m build
python3 -m twine upload dist/dvt_adapters-*
```

### 7. Tag the Release

```bash
git tag v<version> master
git push origin v<version>
```

### 8. Verify on PyPI

```bash
# Wait ~30 seconds for PyPI to index
python3 -m pip index versions dvt-ce
python3 -m pip index versions dvt-adapters
# Should show <version> as latest

# Test installation in clean venv
python3 -m venv /tmp/dvt-verify && source /tmp/dvt-verify/bin/activate
pip install dvt-ce==<version> dvt-adapters[postgres]==<version>
dvt --version
deactivate && rm -rf /tmp/dvt-verify
```

### 9. Post-Release Version Bump (on new_dev)

```bash
git checkout new_dev
# Edit core/setup.py to next patch version
git add core/setup.py
git commit -m "chore: bump version to <next_version> for development"
git push origin new_dev
```

## Error Recovery

- **Build fails**: Fix the issue on `new_dev`, re-merge to `master`, try again
- **Twine upload fails (auth)**: Check PyPI token, retry
- **Twine upload fails (version exists)**: Cannot re-upload same version. Bump patch, rebuild, re-upload
- **Wrong version published**: Yank it on PyPI (`pip install twine && twine yank dvt-ce <version>`), bump, republish
