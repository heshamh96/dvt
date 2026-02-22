# Skill: publish

Build and publish a DVT release to PyPI from the `master` branch.

## Usage

- `/publish <version>` — Build and publish the specified version (e.g., `/publish 1.12.104`)

## Prerequisites

- Must be on `master` branch
- Version in `core/dvt/__version__.py` must match the argument
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
cat core/dvt/__version__.py  # must show version = "<argument>"

# Verify LICENSE and NOTICE exist
ls core/LICENSE core/NOTICE
```

If any check fails, STOP and report the issue. Do NOT proceed.

### 2. Run Unit Tests

```bash
cd core && .venv/bin/python -m pytest ../tests/unit/federation/ --tb=short -q
```

All tests must pass. If any fail, STOP.

### 3. Build Package

```bash
cd core
rm -rf dist/                          # clean previous builds
python3 -m build                       # produces sdist + wheel in dist/
```

Expected output:
- `dist/dvt_core-<version>.tar.gz` (sdist)
- `dist/dvt_core-<version>-py3-none-any.whl` (wheel)

### 4. Verify Package Contents

```bash
# Check LICENSE and NOTICE are included in sdist
tar tf dist/dvt_core-<version>.tar.gz | grep -E "LICENSE|NOTICE"

# Check wheel metadata
unzip -l dist/dvt_core-<version>-py3-none-any.whl | grep -E "LICENSE|NOTICE"
```

Both LICENSE and NOTICE must appear in both archives.

### 5. Upload to PyPI

```bash
cd core
python3 -m twine upload dist/dvt_core-<version>*
```

Twine will prompt for credentials if not configured in `~/.pypirc`.
- Username: `__token__`
- Password: your PyPI API token (starts with `pypi-`)

### 6. Tag the Release

```bash
git tag v<version> master
git push origin v<version>
```

### 7. Verify on PyPI

```bash
# Wait ~30 seconds for PyPI to index
python3 -m pip index versions dvt-core
# Should show <version> as latest

# Test installation in clean venv
python3 -m venv /tmp/dvt-verify && source /tmp/dvt-verify/bin/activate
pip install dvt-core==<version>
dvt --version
deactivate && rm -rf /tmp/dvt-verify
```

### 8. Post-Release Version Bump (on dev)

```bash
git checkout dev
# Edit core/dvt/__version__.py to next patch: <major>.<minor>.<patch+1>
git add core/dvt/__version__.py
git commit -m "chore: bump version to <next_version> for development"
git push origin dev
```

## Error Recovery

- **Build fails**: Fix the issue on `dev`, re-merge to `master`, try again
- **Twine upload fails (auth)**: Check PyPI token, retry
- **Twine upload fails (version exists)**: Cannot re-upload same version. Bump patch, rebuild, re-upload
- **Wrong version published**: Yank it on PyPI (`pip install twine && twine yank dvt-core <version>`), bump, republish
