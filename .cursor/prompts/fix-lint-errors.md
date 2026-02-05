# Fix Lint Errors

Use this prompt when fixing linting/type checking errors.

## Context

I need to fix lint errors in the codebase.

## Commands

```bash
cd core

# Run all checks
hatch run code-quality

# Individual checks
hatch run flake8        # Style linting
hatch run mypy          # Type checking
hatch run black         # Format checking

# Auto-fix formatting
black .                 # Format all files
isort .                 # Sort imports
```

## Common Fixes

### flake8

```python
# E501: Line too long
# Break long lines or use parentheses for implicit continuation

# F401: Module imported but unused
# Remove unused imports

# E302: Expected 2 blank lines
# Add blank lines between top-level definitions

# noqa comment to ignore:
some_long_line = "..."  # noqa: E501
from module import unused  # noqa: F401
```

### mypy

```python
# Missing type hints
def func(arg: str) -> int:
    return len(arg)

# Ignore specific line
result = some_untyped_call()  # type: ignore

# Ignore specific error
result = call()  # type: ignore[arg-type]

# Optional types
from typing import Optional
def func(arg: Optional[str] = None) -> None:
    pass
```

### black

```bash
# Check what would change
black --check --diff .

# Auto-format
black .
```

## Pre-commit Hooks

Pre-commit runs automatically on `git commit` after `hatch run setup`.

```bash
# Run manually
pre-commit run --all-files

# Skip hooks (not recommended)
git commit --no-verify
```

## Configuration

- flake8: `core/.flake8` or `setup.cfg`
- mypy: `pyproject.toml` `[tool.mypy]`
- black: `pyproject.toml` `[tool.black]`
- isort: `pyproject.toml` `[tool.isort]`
