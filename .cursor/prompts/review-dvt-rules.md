# Review DVT Rules Compliance

Use this prompt when reviewing code for DVT RULES compliance.

## Context

I need to verify that `<code/feature>` complies with DVT RULES.

## DVT Rules Checklist

### Rule 2: Target Resolution
- [ ] CLI `--target` overrides model config
- [ ] Model config overrides `profiles.yml` default
- [ ] Global target forces all models to that target

### Rule 3: Execution Path Resolution (3 paths)
- [ ] Default pushdown: model target = default target, all refs same target -> adapter pushdown
- [ ] Non-default pushdown: model target differs, but all refs share same target -> adapter pushdown on non-default
- [ ] Extraction: refs span multiple targets -> Sling extraction to DvtCache (DuckDB), then materialize

### Rule 4: Materialization Rules
- [ ] Same-target: table/view/incremental/ephemeral via adapter
- [ ] Extraction path: table via DvtCache -> target adapter
- [ ] Extraction path: view coerced to table with warning
- [ ] Extraction path: ephemeral resolved in memory

### Rule 7: Commands
- [ ] Command behavior matches specification
- [ ] Proper error handling and messaging

### Rule 10: File Locations
- [ ] User config in `~/.dvt/` (or PROFILES_DIR)
- [ ] Project config in project root
- [ ] profiles.yml, mdm.duckdb in correct locations

## Key Reference Files

- `docs/dvt_implementation_plan.md` - Canonical DVT RULES
- `hesham_mind/dvt_rules.md` - Detailed rule explanations
- `.cursor/rules/dev-team-architecture.mdc` - Architecture agent rules

## Key Code Paths

- `core/dvt/config/target_resolver.py` - Target resolution logic
- `core/dvt/federation/dvt_cache.py` - DvtCache (DuckDB cache engine)
- `core/dvt/federation/optimizer.py` - Query optimization
- `core/dvt/extraction/sling_client.py` - Sling streaming extraction
- `core/dvt/config/source_connections.py` - Source connection configs

## Agent

Use `dev-team-architecture` when reviewing DVT RULES compliance.
