# Skill: major-release

Full DVT release workflow: new_dev -> uat (E2E testing) -> master (production) -> PyPI.

## Usage

- `/major-release <version>` — Execute the full release pipeline (e.g., `/major-release 1.12.104`)

## Overview

This skill orchestrates a complete release by:
1. Validating readiness on `new_dev`
2. Merging to `uat` and running UAT E2E tests
3. Merging to `master` after UAT passes
4. Building and publishing dvt-ce + dvt-adapters to PyPI
5. Tagging the release
6. Bumping the dev version for continued development

## Prerequisites

- On `new_dev` branch with clean working tree
- All unit tests passing
- Trial project ready for UAT E2E (see `/uat-e2e` skill)
- PostgreSQL, Databricks, Snowflake targets accessible
- `python3 -m build` and `python3 -m twine` available
- PyPI credentials ready

## Branch Flow

```
new_dev ──(merge)──> uat ──(UAT E2E pass)──> master ──(tag + publish)──> PyPI
```

| Branch | Purpose |
|--------|---------|
| `new_dev` | Active development — all work happens here |
| `uat` | Testing gate — receives new_dev code, runs E2E before release |
| `master` | Production — only receives code that passed UAT |
| `main` | Upstream dbt-core tracking (local only, for rebasing) |

## Steps

### Phase 1: Pre-Release Validation (on new_dev)

```bash
# 1a. Verify on new_dev branch with clean tree
git branch --show-current   # must be "new_dev"
git status --porcelain       # must be empty

# 1b. Verify version
grep "version=" core/setup.py  # must show target version

# 1c. Run unit tests
cd core && .venv/bin/python -m pytest ../tests/unit/federation/ --tb=short -q
# ALL must pass. If any fail, STOP and fix on new_dev.
```

### Phase 2: Merge to UAT

```bash
# 2a. Update uat branch to match new_dev
git checkout uat
git merge --ff-only new_dev
# If ff-only fails (uat diverged), use: git reset --hard new_dev
git checkout new_dev
```

### Phase 3: UAT E2E Testing

Run the full UAT E2E test suite using the `/uat-e2e` skill:

```
/uat-e2e
```

This runs all 10+ phases:
- Phase 0: Debug (4 target connections)
- Phase 1: Clean
- Phase 2: Deps
- Phase 3: Seeds (PG, DBX, SF)
- Phase 5: Run 1 --full-refresh (all models)
- Phase 6: Run 2 incremental
- Phase 7: Run 3 idempotency
- Phase 8: --target override tests
- Phase 9: Data integrity verification
- Phase 10: Extraction & DuckDB cache verification

**GATE: ALL phases must PASS.**

If any phase fails:
1. STOP the release
2. Switch back to `new_dev`
3. Fix the issue
4. Go back to Phase 1

### Phase 4: Merge to Master

```bash
# 4a. Update master to match uat (which matches new_dev)
git checkout master
git merge --ff-only uat
git checkout new_dev
```

### Phase 5: Build & Publish

Run the `/publish` skill:

```
/publish <version>
```

This will:
1. Switch to `master`
2. Build dvt-ce and dvt-adapters packages
3. Verify LICENSE + NOTICE included
4. Upload both to PyPI
5. Tag `v<version>`

### Phase 6: Post-Release

```bash
# 6a. Push all branches
git push origin new_dev
git push origin uat
git push origin master
git push origin v<version>

# 6b. Bump dev version for next release
git checkout new_dev
# Edit core/setup.py to next patch version
git add core/setup.py
git commit -m "chore: bump version to <next_version> for development"
git push origin new_dev

# 6c. Verify on PyPI
python3 -m pip index versions dvt-ce
python3 -m pip index versions dvt-adapters
# Should show <version> as latest
```

### Phase 7: Generate UAT Report

Write the UAT E2E results to the trial's findings directory:

```
<trial_dir>/findings/uat_e2e_results.md
```

Use the report template from the `/uat-e2e` skill.

## Quick Checklist

- [ ] On `new_dev`, clean tree, tests pass
- [ ] Version set to target release version
- [ ] Merged new_dev -> uat
- [ ] UAT E2E all phases PASS
- [ ] Merged uat -> master
- [ ] dvt-ce package built (sdist + wheel)
- [ ] dvt-adapters package built (sdist + wheel)
- [ ] LICENSE + NOTICE in packages
- [ ] Both uploaded to PyPI
- [ ] Tagged v<version>
- [ ] All branches pushed to origin
- [ ] Dev version bumped to next patch
- [ ] PyPI installation verified

## Error Recovery

| Problem | Solution |
|---------|----------|
| UAT fails | Fix on new_dev, re-merge to uat, re-run UAT |
| Build fails | Fix on new_dev, re-merge through uat -> master |
| PyPI upload fails (auth) | Check token, retry |
| PyPI upload fails (version exists) | Bump patch, rebuild, re-upload |
| Wrong version published | Yank on PyPI, bump, republish |
| master diverged | `git checkout master && git reset --hard uat` |
