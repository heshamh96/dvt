# DVT Release Plan — v1.12.104

**Date**: 2026-02-22
**Branch**: `dev` -> `uat` -> `master`
**PyPI**: `dvt-core` (existing project, versions 1.12.0–1.12.103 published, 0.1.0 yanked)

## Decisions

| Decision | Choice |
|----------|--------|
| License | Apache 2.0 (open source, compliant with dbt-core upstream) |
| GitHub repo | Public at github.com/heshamh96/dvt |
| Version | 1.12.104 |
| PyPI package name | dvt-core |
| Platform support | macOS + Linux (ARM + x86), no Windows |
| Build tooling | `python3 -m build` + `twine upload` |

## Branch Strategy

```
dev ──(develop)──> uat ──(UAT E2E pass)──> master ──(tag + publish)──> PyPI
```

| Branch | Role | Remote |
|--------|------|--------|
| `dev` | Active development, all work happens here | origin/dev |
| `uat` | Testing gate — merged from dev before each release | origin/uat |
| `master` | Production releases only, tagged versions | origin/master (default) |
| `main` | Tracks dbt-core upstream for rebasing | local only (syncs from upstream remote) |

### Upstream Rebase Workflow

```bash
git fetch upstream
git checkout dev
git rebase upstream/main   # or upstream/1.11.latest
# resolve conflicts
git push origin dev --force-with-lease
```

`main` local branch stays pointed at the dbt-core upstream merge base. It is NOT pushed to origin. The `upstream` remote (`https://github.com/dbt-labs/dbt-core.git`) stays connected for fetching.

## License Situation

- **Upstream (dbt-core)**: Apache License 2.0, Copyright 2021 dbt Labs, Inc.
- **DVT**: Apache License 2.0, Copyright 2025-2026 Hesham H.
- **Why Apache 2.0**: DVT is a derivative work of dbt-core. Apache 2.0 Section 4 requires derivative works to retain the license. MIT (previously on origin/master) was non-compliant.
- **NOTICE file**: Required by Apache 2.0 Section 4(d) — attributes both DVT and dbt Labs.
- **Future plans**: Apache 2.0 allows commercial use, freemium/premium, and building proprietary services on top. No limitations on business model.

## Part A: What Hesham Does

### A1. GitHub Repo Settings (if needed)
- Ensure `master` branch is not protected / allows push during release
- After release, can set `master` as default branch in GitHub settings

### A2. PyPI Token
- Already has PyPI account with `dvt-core` project
- Token configured (or will be prompted during `twine upload`)

### A3. Post-Publish Verification
```bash
python3 -m venv /tmp/test-dvt && source /tmp/test-dvt/bin/activate
pip install dvt-core
dvt --version    # should show 1.12.104
dvt init test_project
deactivate && rm -rf /tmp/test-dvt
```

### A4. Announce
- Share on community channels (dbt Slack, Twitter/X, LinkedIn)

## Part B: What Claude Does

### B1. Fix LICENSE File
- Replace symlink at repo root `LICENSE` with actual Apache 2.0 text
- Both `LICENSE` (repo root) and `core/LICENSE` contain full Apache 2.0 text
- Update copyright appendix to include DVT attribution

### B2. Create NOTICE Files
- `NOTICE` at repo root
- `core/NOTICE` (included in built package)
- Content:
  ```
  DVT (Data Virtualization Tool)
  Copyright 2025-2026 Hesham H.

  This product includes software developed by dbt Labs, Inc.
  (https://github.com/dbt-labs/dbt-core)
  Original software Copyright 2021 dbt Labs, Inc.
  Licensed under the Apache License, Version 2.0
  ```

### B3. Update pyproject.toml + Version
- `core/dvt/__version__.py`: `1.12.103` -> `1.12.104`
- Remove `"Operating System :: Microsoft :: Windows"` classifier
- Ensure `license = "Apache-2.0"`
- Add NOTICE to `license-files`
- Verify `project.urls` point to github.com/heshamh96/dvt

### B4. Clean Up Old Branches
- Delete local: `extraction_enhancement`, `prod`
- Keep: `dev`, `uat`, `master`, `main`

### B5. Fast-Forward UAT
```bash
git checkout uat && git merge --ff-only dev && git checkout dev
```

### B6. Merge to Master + Tag
```bash
git checkout master && git merge --ff dev && git checkout dev
git tag v1.12.104 master
```

### B7. Build Package
```bash
cd core
python3 -m build
# Verify contents:
tar tf dist/dvt_core-1.12.104.tar.gz | grep -E "LICENSE|NOTICE"
unzip -l dist/dvt_core-1.12.104-py3-none-any.whl | grep -E "LICENSE|NOTICE"
```

### B8. Create Publish Skill
`.claude/skills/publish/SKILL.md` — for publishing any version to PyPI

### B9. Create Major Release Skill
`.claude/skills/major-release/SKILL.md` — full workflow: dev -> uat (E2E) -> master -> PyPI

### B10. Push All
```bash
git push origin dev
git push origin uat
git push origin master
git push origin v1.12.104
```

### B11. Publish to PyPI
```bash
cd core
python3 -m twine upload dist/dvt_core-1.12.104*
```

## PyPI Yanked 0.1.0 — Not Problematic

- Yanked version still exists but `pip install dvt-core` ignores it
- Only `pip install dvt-core==0.1.0` (explicit pin) would install it
- Publishing 1.12.104 is independent, no conflict
- Users see 1.12.104 as latest; 0.1.0 shows as yanked in history

## Existing PyPI Versions

```
1.12.103 (current latest)
1.12.102
1.12.101
1.12.1
1.12.0
0.1.0 (yanked)
```

## Skills Created

### `/publish <version>`
Quick publish of an already-tested version from master to PyPI.

### `/major-release <version>`
Full release workflow:
1. Pre-checks (on dev, tests pass, clean tree)
2. Merge dev -> uat
3. Run UAT E2E (invoke /uat-e2e)
4. Gate: UAT must pass
5. Merge uat -> master
6. Build + publish (invoke /publish)
7. Tag vX.Y.Z
8. Push everything
9. Bump dev to next patch version
