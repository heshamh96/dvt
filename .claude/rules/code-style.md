# Code Style Guidelines

## Python Formatting

- **Line length**: 99 characters (black default)
- **Formatter**: black
- **Linter**: flake8
- **Type checker**: mypy

## Running Checks

```bash
cd core
hatch run code-quality  # All checks (black, flake8, mypy, pre-commit)
hatch run lint          # flake8 + mypy only
hatch run black         # Format check only
```

## Type Hints

- Required for all new code
- Use `# type: ignore` sparingly with justification
- Prefer specific types over `Any`

## Import Order

1. Standard library
2. Third-party packages
3. Local imports

Use `isort` (included in pre-commit) to sort automatically.

## Linting Suppressions

```python
# OK - specific code with comment
result = thing()  # noqa: E501 - URL too long

# Avoid - blanket suppression
result = thing()  # noqa
```

## Docstrings

- Use Google style for public APIs
- Don't add docstrings to code you didn't modify
- Keep docstrings concise

## Comments

- Only where logic isn't self-evident
- Don't add comments to explain obvious code
- Don't add comments to code you didn't change
