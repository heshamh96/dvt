# Review DVT Rules Compliance

Use this prompt when reviewing code for DVT RULES compliance.

## Context

I need to verify that `<code/feature>` complies with DVT RULES.

## DVT Rules Checklist

### Rule 1: Compute Resolution
- [ ] CLI `--compute` overrides model config
- [ ] Model config overrides `computes.yml` default
- [ ] Invalid compute raises compilation error (no fallback)

### Rule 2: Target Resolution
- [ ] CLI `--target` overrides model config
- [ ] Model config overrides `profiles.yml` default
- [ ] Global target forces all models to that target

### Rule 3: Execution Path Resolution
- [ ] Same-target models use adapter pushdown
- [ ] Cross-target models use Spark federation
- [ ] Predicate pushdown applied on federation path

### Rule 4: Materialization Rules
- [ ] Same-target: table/view/incremental/ephemeral via adapter
- [ ] Cross-target: table via Spark JDBC
- [ ] Cross-target: view coerced to table with warning
- [ ] Cross-target: ephemeral resolved in memory

### Rule 7: Commands
- [ ] Command behavior matches specification
- [ ] Proper error handling and messaging

### Rule 10: File Locations
- [ ] User config in `~/.dvt/` (or PROFILES_DIR)
- [ ] Project config in project root
- [ ] profiles.yml, computes.yml, mdm.duckdb in correct locations

## Key Reference Files

- `docs/dvt_implementation_plan.md` - Canonical DVT RULES
- `hesham_mind/dvt_rules.md` - Detailed rule explanations
- `.cursor/rules/dev-team-architecture.mdc` - Architecture agent rules

## Agent

Use `dev-team-architecture` when reviewing DVT RULES compliance.
