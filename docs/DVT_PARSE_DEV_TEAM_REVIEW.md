# dvt parse – Dev team review (Feature 04)

Review of the current `dvt parse` implementation against **dvt-core-features/04-dvt-parse** and the dev-team agents (backend, architecture, QA).

---

## Current behavior (summary)

- **CLI**: `dvt parse` is a full command with `@global_flags`; it runs preflight → profile → project → catalogs → runtime_config → **manifest(write_perf_info=True)**.
- **Parsing**: Done in `requires.setup_manifest` → `parser.manifest.parse_manifest()`, which builds the full manifest (macros, nodes, sources, etc.) and optionally writes `target/manifest.json` and `target/perf_info.json`.
- **Output**: After this change, the parse command prints a short summary: Models, Tests, Seeds, Snapshots, Sources, Macros counts, then returns `(manifest, True)`.
- **Dependencies**: Parse **requires** profile, project, and runtime_config (so `profiles.yml` and a valid profile are required). It does **not** open a DB connection; the adapter is used for macro resolution and manifest building only.

---

## Feature spec alignment (04-dvt-parse)

| Spec item | Status | Notes |
|-----------|--------|--------|
| Parse all models without DB connection | **OK** | No connection; profile is loaded for config/macro context only. |
| SQL syntax errors with file/line | **OK** | Handled by parser; errors surface via exceptions. |
| Jinja syntax errors reported | **OK** | Parser validates Jinja. |
| Python model syntax validated | **OK** | Python models parsed like other nodes. |
| YAML schema files validated | **OK** | Schema parser handles YAML. |
| `ref()` and `source()` extracted | **OK** | Part of manifest nodes. |
| Config blocks extracted | **OK** | In node config. |
| Model selection `-s` | **Partial** | `-s` is on parse via `@global_flags`, but manifest is built for the **whole** project; selection does not limit which files are parsed. Spec may imply “parse only selected” (syntax-check subset). |
| Warnings vs errors distinguished | **OK** | Parser/events distinguish. |
| Parse result shows counts | **Done** | Summary (Models, Tests, Seeds, Snapshots, Sources, Macros) is now printed. |
| `--verbose` | **Missing** | Feature spec mentions `dvt parse --verbose`; not a dedicated parse flag. Could use log level. |

---

## Dev-team-backend

- **Rule 7**: Parse is listed; implementation matches “parse project and write manifest.”
- **Rule 10**: Uses project root and `dbt_project.yml`; target path from config.
- **Key files**: `core/dvt/cli/main.py` (parse command), `core/dvt/cli/requires.py` (manifest decorator), `core/dvt/parser/manifest.py` (parse_manifest, ManifestLoader).
- **Gaps**:
  - Parse could document in docstring that it requires profile/project (for macro/config) but does not connect to the database.
  - If “parse only selected” is desired, backend would need a path where parse runs with a selection and only loads/validates those resources (or a lighter “syntax-only” mode).

---

## Dev-team-architecture

- **Rule 7 / feature order**: Parse (04) after sync (03); compile (05) depends on parse. Matches.
- **Contracts**: Manifest and node types are the single source of truth; parse populates them. No extra contract needed for parse.
- **Gaps**:
  - None critical. If a future “parse without profile” mode is added (syntax-only, no macro resolution), that would be an architectural choice (e.g. a separate code path or a flag).

---

## Dev-team-qa

- **Tests**: Functional parse tests exist (e.g. partial parsing, parser behavior). No new tests added in this pass.
- **Recommendation**: Add or extend a test that runs `dvt parse` and asserts:
  - Exit code 0 and summary line containing “Parsed successfully” and expected resource counts for a known project.
- **Coverage**: Parser code is well covered; CLI parse path and summary output are good candidates for a quick smoke test.

---

## What was changed

1. **Parse summary output**  
   In `core/dvt/cli/main.py`, after `setup_manifest`, the parse command now prints:
   - “Parsed successfully:”
   - Counts for Models, Tests, Seeds, Snapshots, Sources, Macros (from the manifest).
   This aligns the command with the feature spec (“Parse result shows counts of each resource type”) and gives clear feedback when parse succeeds.

---

## Optional next steps

1. **`-s` / selection semantics**: Clarify whether “parse specific model” means (a) full parse then report only selected, or (b) only parse/validate selected files. If (b), implement a parse path that restricts to selection.
2. **`--verbose`**: Add a parse-specific `--verbose` or document that `--log-level debug` gives more detail.
3. **QA**: Add a functional test that checks parse exit code and summary output (and optionally perf_info path).

---

## How to run parse

From a project directory with `dbt_project.yml` and a profile in `~/.dvt/profiles.yml` (or `--profiles-dir`):

```bash
dvt parse
```

With project/profile flags:

```bash
dvt parse --project-dir /path/to/project --profiles-dir ~/.dvt
```

See **docs/USING_LOCAL_DVT_WITH_UV.md** for running with a local dvt-core build.
