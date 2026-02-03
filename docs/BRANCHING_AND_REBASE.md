# Branching and Rebase Strategy

This document describes the Git workflow for DVT development, including branch strategy, remotes, and how to rebase on dbt-core.

## Repository Setup

### Remotes

| Remote | URL | Purpose |
|--------|-----|---------|
| **origin** | `git@github.com:heshamh96/dvt.git` | Our DVT repository (default push/fetch) |
| **upstream** | `https://github.com/dbt-labs/dbt-core.git` | dbt-core source (for rebasing) |

### Current Remote Configuration

```bash
# View remotes
git remote -v

# Expected output:
# origin	git@github.com:heshamh96/dvt.git (fetch)
# origin	git@github.com:heshamh96/dvt.git (push)
# upstream	https://github.com/dbt-labs/dbt-core.git (fetch)
# upstream	https://github.com/dbt-labs/dbt-core.git (push)
```

## Branch Strategy

### Branch Overview

```
┌─────────────────────────────────────────────────────────┐
│                    Branch Flow                            │
├─────────────────────────────────────────────────────────┤
│                                                          │
│  upstream/main (dbt-core)                               │
│         ↓                                                │
│    [rebase]                                             │
│         ↓                                                │
│  origin/dev (DVT development)                           │
│         ↓                                                │
│    [PR merge]                                           │
│         ↓                                                │
│  origin/uat (acceptance/staging)                        │
│         ↓                                                │
│    [PR merge]                                           │
│         ↓                                                │
│  origin/prod (production)                               │
│                                                          │
└─────────────────────────────────────────────────────────┘
```

### Branch Descriptions

- **main**: Optional. Can mirror `upstream/main` for rebase base, or kept as protected "dbt-core sync" branch.
- **dev**: Primary development branch. All feature work happens here. This is the default branch for development.
- **uat**: Acceptance/staging branch. Merges from `dev` via Pull Request for testing before production.
- **prod**: Production-ready branch. Merges from `uat` via Pull Request. Only production-ready code lives here.

## Development Workflow

### Starting New Work

1. **Ensure you're on dev branch**:
   ```bash
   git checkout dev
   git pull origin dev
   ```

2. **Create a feature branch** (optional, or work directly on dev):
   ```bash
   git checkout -b feature/my-feature
   ```

3. **Make your changes** and commit:
   ```bash
   git add .
   git commit -m "feat: description of changes"
   ```

4. **Push to origin**:
   ```bash
   git push origin dev  # or your feature branch
   ```

### Rebase Workflow (Stay on Top of dbt-core)

To keep DVT synchronized with the latest dbt-core changes:

1. **Fetch latest from upstream**:
   ```bash
   git fetch upstream
   ```

2. **Checkout dev branch**:
   ```bash
   git checkout dev
   ```

3. **Rebase dev onto upstream/main**:
   ```bash
   git rebase upstream/main
   ```

4. **Resolve any conflicts** if they occur:
   - Git will pause and show conflicted files
   - Resolve conflicts manually
   - Stage resolved files: `git add <file>`
   - Continue rebase: `git rebase --continue`
   - Or abort if needed: `git rebase --abort`

5. **Push rebased dev branch**:
   ```bash
   # If dev was already pushed, use force-with-lease for safety
   git push origin dev --force-with-lease
   ```

### Using the Rebase Script

We provide a helper script for rebasing:

```bash
./scripts/rebase-on-dbt-core.sh
```

This script:
- Fetches latest from upstream
- Checks current branch (should be `dev`)
- Rebases onto `upstream/main`
- Provides push instructions

## Branch Promotion (dev → uat → prod)

### Promoting to UAT

1. **Create a Pull Request** from `dev` to `uat`:
   - On GitHub, create PR: `dev` → `uat`
   - Add description of changes
   - Wait for CI checks to pass
   - Get approval if required
   - Merge PR

2. **After merge, update local uat**:
   ```bash
   git checkout uat
   git pull origin uat
   ```

### Promoting to Prod

1. **Create a Pull Request** from `uat` to `prod`:
   - On GitHub, create PR: `uat` → `prod`
   - Add description of changes
   - Wait for CI checks to pass
   - Get approval if required
   - Merge PR

2. **After merge, update local prod**:
   ```bash
   git checkout prod
   git pull origin prod
   ```

## Branch Protection Rules

On GitHub, configure branch protection:

- **dev**: 
  - Require pull request reviews before merging (optional)
  - Require status checks to pass (CI tests)
  
- **uat**:
  - Require pull request reviews before merging
  - Require status checks to pass
  - Require branches to be up to date before merging
  
- **prod**:
  - Require pull request reviews before merging (2 approvals recommended)
  - Require status checks to pass
  - Require branches to be up to date before merging
  - No force pushes allowed

## Common Scenarios

### Scenario 1: Starting Fresh Development

```bash
# Clone the repository
git clone git@github.com:heshamh96/dvt.git
cd dvt-core

# Add upstream remote
git remote add upstream https://github.com/dbt-labs/dbt-core.git

# Checkout dev branch
git checkout dev

# Start working
```

### Scenario 2: Syncing with dbt-core

```bash
# Fetch latest from dbt-core
git fetch upstream

# Rebase dev onto upstream/main
git checkout dev
git rebase upstream/main

# Resolve conflicts if any, then push
git push origin dev --force-with-lease
```

### Scenario 3: Working on a Feature

```bash
# Start from latest dev
git checkout dev
git pull origin dev

# Create feature branch
git checkout -b feature/my-feature

# Make changes, commit, push
git add .
git commit -m "feat: my feature"
git push origin feature/my-feature

# Create PR: feature/my-feature → dev
```

### Scenario 4: Hotfix to Production

```bash
# Create hotfix branch from prod
git checkout prod
git pull origin prod
git checkout -b hotfix/critical-fix

# Make fix, commit, push
git add .
git commit -m "fix: critical production fix"
git push origin hotfix/critical-fix

# Create PR: hotfix/critical-fix → prod
# After merge, backport to dev and uat
```

## Troubleshooting

### Rebase Conflicts

If you encounter conflicts during rebase:

1. **Identify conflicted files**:
   ```bash
   git status
   ```

2. **Resolve conflicts** in each file (look for `<<<<<<<`, `=======`, `>>>>>>>` markers)

3. **Stage resolved files**:
   ```bash
   git add <resolved-file>
   ```

4. **Continue rebase**:
   ```bash
   git rebase --continue
   ```

5. **If you need to abort**:
   ```bash
   git rebase --abort
   ```

### Force Push Safety

Always use `--force-with-lease` instead of `--force`:

```bash
# Safe (checks remote hasn't changed)
git push origin dev --force-with-lease

# Unsafe (can overwrite others' work)
git push origin dev --force  # DON'T USE
```

### Checking Branch Status

```bash
# See all branches
git branch -a

# See current branch
git branch --show-current

# See remote tracking
git branch -vv
```

## Best Practices

1. **Always rebase on dev** - Keep dev synchronized with dbt-core regularly
2. **Use feature branches** - For larger features, create branches from dev
3. **Test before promoting** - Ensure tests pass before promoting to uat/prod
4. **Write clear commit messages** - Follow conventional commits format
5. **Keep branches clean** - Delete merged branches after PR merge
6. **Communicate conflicts** - If rebase conflicts are complex, discuss with team

## Related Documentation

- [Team Agents](./TEAM_AGENTS.md) - Sub-agent documentation
- `dvt_implementation_plan.md` - Implementation plan and DVT RULES
- `scripts/rebase-on-dbt-core.sh` - Rebase helper script
