# Add Unit Test

Use this prompt when adding unit tests for DVT code.

## Context

I need to add unit tests for `<module/function>`.

## Requirements

1. **Test location**: `tests/unit/` (mirror source structure)

2. **Test patterns**:
   - Use pytest fixtures for setup
   - Use Hypothesis for property-based testing where applicable
   - Test concrete classes, not abstract ones
   - Test object serialization round-trips (dict → object → dict)

3. **Reference files**:
   - Test examples: `tests/unit/`
   - Test README: `tests/unit/README.md`
   - Hatch config: `core/hatch.toml`

4. **Running tests**:
   ```bash
   cd core
   hatch run unit-tests                              # All unit tests
   python -m pytest tests/unit/test_file.py         # Specific file
   python -m pytest tests/unit/test_file.py::test_name  # Specific test
   python -m pytest tests/unit/test_file.py -v --pdb    # With debugger
   ```

5. **Test structure**:
   ```python
   import pytest
   from dvt.module import Thing

   class TestThing:
       def test_basic_functionality(self):
           thing = Thing()
           assert thing.do_something() == expected

       def test_edge_case(self):
           with pytest.raises(ExpectedError):
               Thing(invalid_input)
   ```

6. **Mocking**:
   - Use `pytest-mock` for mocking
   - Prefer dependency injection over patching when possible

## Agent

Use `dev-team-qa` rules when writing tests.
