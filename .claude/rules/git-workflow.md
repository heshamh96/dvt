# Git Workflow

## Branch Strategy

- **dev**: Main development branch (work here)
- **master**: Production releases

Always work on `dev` branch unless creating a feature branch.

## Before Committing

```bash
cd core
hatch run code-quality  # Run all checks
```

## Commit Format

Use conventional commits:

```
<type>(<scope>): <description>

[optional body]

[optional footer]
```

### Types
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation only
- `test`: Adding/updating tests
- `chore`: Maintenance tasks
- `refactor`: Code change that neither fixes bug nor adds feature

### Examples
```
feat(cli): add --compute flag for compute engine selection
fix(federation): correct predicate pushdown for JDBC sources
docs(readme): update installation instructions
test(config): add unit tests for target resolution
```

## Changelog

Use changie for changelog entries:

```bash
changie new
```

## Rebase Compatibility

DVT rebases onto upstream dbt-core. To preserve compatibility:

- Keep DVT-specific changes clearly marked
- Avoid modifying dbt-core files when possible
- Use extension points rather than patching

## Pull Requests

- Target `dev` branch for development
- Target `master` for releases only
- Include test coverage for new features
- Run full test suite before opening PR
