#!/bin/bash

# Rebase DVT dev branch onto dbt-core upstream/main
# This script helps keep DVT synchronized with the latest dbt-core changes

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored messages
info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if we're in a git repository
if ! git rev-parse --git-dir > /dev/null 2>&1; then
    error "Not in a git repository"
    exit 1
fi

# Get current branch
CURRENT_BRANCH=$(git branch --show-current)

# Check if upstream remote exists
if ! git remote | grep -q "^upstream$"; then
    error "Upstream remote not found. Adding it now..."
    git remote add upstream https://github.com/dbt-labs/dbt-core.git
    info "Added upstream remote"
fi

# Check if we're on dev branch (recommended)
if [ "$CURRENT_BRANCH" != "dev" ]; then
    warn "Current branch is '$CURRENT_BRANCH', not 'dev'"
    read -p "Continue anyway? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        info "Aborted. Switch to dev branch first: git checkout dev"
        exit 1
    fi
fi

info "Current branch: $CURRENT_BRANCH"

# Check for uncommitted changes
if ! git diff-index --quiet HEAD --; then
    error "You have uncommitted changes. Please commit or stash them first."
    exit 1
fi

# Fetch latest from upstream
info "Fetching latest from upstream (dbt-core)..."
git fetch upstream

# Get the commit hash we're rebasing onto
UPSTREAM_MAIN=$(git rev-parse upstream/main)
CURRENT_HEAD=$(git rev-parse HEAD)

info "Rebasing onto upstream/main ($(git log -1 --format='%h %s' upstream/main))"

# Check if we're already up to date
if [ "$CURRENT_HEAD" = "$UPSTREAM_MAIN" ]; then
    info "Already up to date with upstream/main"
    exit 0
fi

# Show what commits will be rebased
info "Commits to rebase:"
git log --oneline HEAD..upstream/main | head -5
if [ $(git rev-list --count HEAD..upstream/main) -gt 5 ]; then
    echo "... and $(($(git rev-list --count HEAD..upstream/main) - 5)) more"
fi

# Ask for confirmation
read -p "Proceed with rebase? (y/N) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    info "Aborted"
    exit 1
fi

# Perform rebase
info "Starting rebase..."
if git rebase upstream/main; then
    info "Rebase completed successfully!"
    
    # Show status
    echo
    info "Rebase summary:"
    git log --oneline HEAD~5..HEAD
    
    echo
    warn "Next steps:"
    echo "  1. Review the changes: git log HEAD~5..HEAD"
    echo "  2. Run tests to ensure everything works"
    echo "  3. Push to origin: git push origin $CURRENT_BRANCH --force-with-lease"
    echo
    info "To push now, run:"
    echo "  git push origin $CURRENT_BRANCH --force-with-lease"
else
    error "Rebase failed. Resolve conflicts and run: git rebase --continue"
    error "Or abort with: git rebase --abort"
    exit 1
fi
