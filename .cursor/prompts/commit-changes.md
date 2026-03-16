# Commit Changes

Use this prompt when committing changes to the DVT repository.

## Pre-commit Checklist

1. **Run code quality checks**:
   ```bash
   cd core
   hatch run code-quality
   ```

2. **Run tests**:
   ```bash
   hatch run unit-tests
   ```

3. **Check git status**:
   ```bash
   git status
   git diff
   ```

## Commit Message Format

Follow conventional commits:

```
<type>(<scope>): <description>

[optional body]

[optional footer]
```

### Types
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation only
- `style`: Formatting, no code change
- `refactor`: Code change that neither fixes nor adds
- `test`: Adding/updating tests
- `chore`: Maintenance tasks

### Scopes
- `cli`: CLI commands
- `config`: Configuration
- `parser`: Parsing logic
- `sync`: dvt sync command
- `extraction`: Sling extraction / DvtCache
- `adapters`: Database adapters
- `tests`: Test infrastructure

### Examples

```bash
git commit -m "feat(cli): add dvt sync command for environment setup"
git commit -m "fix(config): handle missing profiles.yml gracefully"
git commit -m "docs: update CLAUDE.md with sync command"
git commit -m "test(sync): add unit tests for adapter installation"
```

## Branch Strategy

- Work on `new_dev` branch
- Create feature branches for larger changes: `feature/<name>`
- PRs merge to `new_dev`

## Changelog

Use [changie](https://changie.dev/) for changelog entries:

```bash
changie new
```

This creates a file in `.changes/unreleased/` - commit it with your changes.

## Before Pushing

```bash
# Ensure you're on new_dev or feature branch
git branch

# Pull latest
git pull --rebase origin new_dev

# Push
git push origin <branch>
```
