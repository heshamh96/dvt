# Skill: major-release

Full DVT release workflow: dev -> uat (E2E testing) -> master (production) -> PyPI.

## Usage

- `/major-release <version>` — Execute the full release pipeline (e.g., `/major-release 1.12.104`)

## Overview

This skill orchestrates a complete release by:
1. Validating readiness on `dev`
2. Merging to `uat` and running UAT E2E tests
3. Merging to `master` after UAT passes
4. Building and publishing to PyPI
5. Tagging the release
6. Bumping the dev version for continued development

## Prerequisites

- On `dev` branch with clean working tree
- All unit tests passing
- Trial project ready for UAT E2E (see `/uat-e2e` skill)
- PostgreSQL, Databricks, Snowflake targets accessible
- `python3 -m build` and `python3 -m twine` available
- PyPI credentials ready

## Branch Flow

```
dev ──(merge)──> uat ──(UAT E2E pass)──> master ──(tag + publish)──> PyPI
```

| Branch | Purpose |
|--------|---------|
| `dev` | Active development — all work happens here |
| `uat` | Testing gate — receives dev code, runs E2E before release |
| `master` | Production — only receives code that passed UAT |
| `main` | Upstream dbt-core tracking (local only, for rebasing) |

## Steps

### Phase 1: Pre-Release Validation (on dev)

```bash
# 1a. Verify on dev branch with clean tree
git branch --show-current   # must be "dev"
git status --porcelain       # must be empty

# 1b. Verify version
cat core/dvt/__version__.py  # must show version = "<argument>"
# If version doesn't match, update it:
# Edit core/dvt/__version__.py to: version = "<argument>"
# git add core/dvt/__version__.py && git commit -m "chore: bump version to <argument>"

# 1c. Run unit tests
cd core && .venv/bin/python -m pytest ../tests/unit/federation/ --tb=short -q
# ALL must pass. If any fail, STOP and fix on dev.
```

### Phase 2: Merge to UAT

```bash
# 2a. Update uat branch to match dev
git checkout uat
git merge --ff-only dev
# If ff-only fails (uat diverged), use: git reset --hard dev
git checkout dev
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
- Phase 10: Staging & optimizer verification
- Phase 10.5: Incremental staging trace test

**GATE: ALL phases must PASS.**

If any phase fails:
1. STOP the release
2. Switch back to `dev`
3. Fix the issue
4. Go back to Phase 1

### Phase 4: Merge to Master

```bash
# 4a. Update master to match uat (which matches dev)
git checkout master
git merge --ff-only uat
git checkout dev
```

### Phase 5: Build & Publish

Run the `/publish` skill:

```
/publish <version>
```

This will:
1. Switch to `master`
2. Build the package
3. Verify LICENSE + NOTICE included
4. Upload to PyPI
5. Tag `v<version>`

### Phase 6: Post-Release

```bash
# 6a. Push all branches
git push origin dev
git push origin uat
git push origin master
git push origin v<version>

# 6b. Bump dev version for next release
git checkout dev
# Edit core/dvt/__version__.py to next patch
git add core/dvt/__version__.py
git commit -m "chore: bump version to <next_version> for development"
git push origin dev

# 6c. Verify on PyPI
python3 -m pip index versions dvt-core
# Should show <version> as latest
```

### Phase 7: Generate UAT Report

Write the UAT E2E results to the trial's findings directory:

```
<trial_dir>/findings/uat_e2e_results.md
```

Use the report template from the `/uat-e2e` skill.

## Quick Checklist

- [ ] On `dev`, clean tree, tests pass
- [ ] Version set to target release version
- [ ] Merged dev -> uat
- [ ] UAT E2E all phases PASS
- [ ] Merged uat -> master
- [ ] Package built (sdist + wheel)
- [ ] LICENSE + NOTICE in package
- [ ] Uploaded to PyPI
- [ ] Tagged v<version>
- [ ] All branches pushed to origin
- [ ] Dev version bumped to next patch
- [ ] PyPI installation verified

## Error Recovery

| Problem | Solution |
|---------|----------|
| UAT fails | Fix on dev, re-merge to uat, re-run UAT |
| Build fails | Fix on dev, re-merge through uat -> master |
| PyPI upload fails (auth) | Check token, retry |
| PyPI upload fails (version exists) | Bump patch, rebuild, re-upload |
| Wrong version published | Yank on PyPI, bump, republish |
| master diverged | `git checkout master && git reset --hard uat` |
