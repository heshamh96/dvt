# Debug Issue

Use this prompt when debugging a DVT issue.

## Context

I'm seeing the following issue: `<describe issue>`

## Debugging Steps

1. **Check logs**:
   - Look in `logs/dbt.log` in your project directory
   - Stack traces and detailed errors are logged there

2. **Single-threaded debugging**:
   ```bash
   dvt --single-threaded run    # Easier to trace execution
   ```

3. **Use debugger**:
   ```bash
   # With pytest
   python -m pytest tests/unit/test_file.py --pdb --pdbcls=IPython.terminal.debugger:pdb

   # With ipdb in code
   import ipdb; ipdb.set_trace()
   ```

4. **Jinja debugging**:
   ```sql
   -- In SQL models
   {{ log(msg, info=true) }}    -- Print to console
   {{ debug() }}                 -- Drop into debugger
   ```

5. **Profiling**:
   ```bash
   dvt -r dbt.cprof run          # Collect profile data
   dvt -r dbt.cprof parse        # Profile parsing only
   snakeviz dbt.cprof            # View in browser (pip install snakeviz)
   ```

6. **Inspect artifacts**:
   ```bash
   # Format JSON for readability
   python -m json.tool target/run_results.json > run_results_formatted.json
   python -m json.tool target/manifest.json > manifest_formatted.json
   ```

7. **Common issues**:
   - **Missing adapter**: Install with `pip install dbt-<adapter>`
   - **Architecture mismatch**: Ensure Python matches CPU arch (arm64 vs x86_64)
   - **Profile not found**: Check `~/.dvt/profiles.yml` has correct profile name
   - **Compute not found**: Check `~/.dvt/computes.yml` has compute defined

## Key Files to Check

- `core/dvt/exceptions.py` - Exception definitions
- `core/dvt/task/` - Task implementations
- `core/dvt/cli/` - CLI handling
- `logs/dbt.log` - Runtime logs
