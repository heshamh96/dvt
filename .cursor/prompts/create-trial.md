# Create Trial

Use this prompt when creating a new test trial for feature validation.

## Context

I need to test `<feature>` with trial number `<N>`.

## Trial Setup

1. **Create trial folder**:
   ```bash
   cd /Users/hex/Documents/My_Projects/DVT/Testing_Playground
   mkdir trial_<feature>_<N>
   cd trial_<feature>_<N>
   ```

2. **Initialize as uv project**:
   ```bash
   uv init
   ```

3. **Configure pyproject.toml**:
   ```toml
   [project]
   name = "trial-<feature>-<N>"
   version = "0.1.0"
   requires-python = ">=3.10"
   dependencies = [
       "dvt-core",
       "dbt-postgres",  # or other adapter
   ]

   [tool.uv.sources]
   dvt-core = { path = "/Users/hex/Documents/My_Projects/DVT/dvt-core/core", editable = false }
   ```

4. **Install dependencies**:
   ```bash
   uv sync
   ```

5. **Run DVT commands**:
   ```bash
   uv run dvt init Coke_DB --skip-profile-setup
   uv run dvt parse --project-dir Coke_DB
   uv run dvt debug --project-dir Coke_DB
   ```

6. **Create findings directory**:
   ```bash
   mkdir findings
   ```

7. **Document findings** in `findings/`:
   - `data_engineer_findings.md` - dbt compatibility observations
   - `technical_qa_findings.md` - path/file verification
   - `negative_tester_findings.md` - edge case results

## Trial Checklist

- [ ] Trial folder created: `trial_<feature>_<N>/`
- [ ] pyproject.toml with dvt-core path dependency
- [ ] uv sync completed successfully
- [ ] DVT commands tested
- [ ] Findings documented

## Reference

- [USING_LOCAL_DVT_WITH_UV.md](../docs/USING_LOCAL_DVT_WITH_UV.md)
- [RUNNING_DVT.md](../docs/RUNNING_DVT.md)
- [test-team-data-engineer.mdc](.cursor/rules/test-team-data-engineer.mdc)
