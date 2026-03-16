"""
DVT CLI entry point.

This module is only imported when dbt dependencies are available.
The resilient entry point in dvt.cli.__init__ handles the broken-env case
and falls back to a sync-only CLI.
"""

import os
from pathlib import Path

import click

# Set DBT_PROFILES_DIR to ~/.dvt if it exists and isn't already set.
# This makes dbt's profile loader aware of ~/.dvt as a profiles location.
# Fallback: ~/.dbt (dbt's default, compatible with VS Code dbt extensions).
if "DBT_PROFILES_DIR" not in os.environ:
    dvt_profiles = Path.home() / ".dvt" / "profiles.yml"
    if dvt_profiles.exists():
        os.environ["DBT_PROFILES_DIR"] = str(Path.home() / ".dvt")

from dbt.cli import params as p
from dbt.cli import requires
from dbt.cli.main import global_flags


# ---------------------------------------------------------------------------
# dvt (root group)
# ---------------------------------------------------------------------------
@click.group(
    context_settings={"help_option_names": ["-h", "--help"]},
    invoke_without_command=True,
    no_args_is_help=True,
    epilog="Specify one of these sub-commands and you can find more help from there.",
)
@click.pass_context
@global_flags
@p.show_resource_report
def cli(ctx, **kwargs):
    """DVT — cross-engine data transformation tool with DuckDB federation."""


# ---------------------------------------------------------------------------
# dvt build
# ---------------------------------------------------------------------------
@cli.command("build")
@click.pass_context
@global_flags
@p.empty
@p.event_time_start
@p.event_time_end
@p.exclude
@p.export_saved_queries
@p.full_refresh
@p.deprecated_include_saved_query
@p.profiles_dir
@p.project_dir
@p.resource_type
@p.exclude_resource_type
@p.select
@p.selector
@p.show
@p.store_failures
@p.target_path
@p.threads
@p.vars
@requires.postflight
@requires.preflight
@requires.profile
@requires.project
@requires.runtime_config
@requires.manifest
def build(ctx, **kwargs):
    """Run all seeds, models, snapshots, and tests in DAG order."""
    from dvt.tasks.build import DvtBuildTask

    task = DvtBuildTask(
        ctx.obj["flags"],
        ctx.obj["runtime_config"],
        ctx.obj["manifest"],
    )
    results = task.run()
    success = task.interpret_results(results)
    return results, success


# ---------------------------------------------------------------------------
# dvt run
# ---------------------------------------------------------------------------
@cli.command("run")
@click.pass_context
@global_flags
@p.exclude
@p.full_refresh
@p.profiles_dir
@p.project_dir
@p.empty
@p.event_time_start
@p.event_time_end
@p.select
@p.selector
@p.target_path
@p.threads
@p.vars
@requires.postflight
@requires.preflight
@requires.profile
@requires.project
@requires.runtime_config
@requires.manifest
def run(ctx, **kwargs):
    """Compile SQL and execute against the current target database."""
    from dvt.tasks.run import DvtRunTask

    task = DvtRunTask(
        ctx.obj["flags"],
        ctx.obj["runtime_config"],
        ctx.obj["manifest"],
    )
    results = task.run()
    success = task.interpret_results(results)
    return results, success


# ---------------------------------------------------------------------------
# dvt compile
# ---------------------------------------------------------------------------
@cli.command("compile")
@click.pass_context
@global_flags
@p.exclude
@p.full_refresh
@p.show_output_format
@p.introspect
@p.profiles_dir
@p.project_dir
@p.empty
@p.select
@p.selector
@p.inline
@p.compile_inject_ephemeral_ctes
@p.target_path
@p.threads
@p.vars
@requires.postflight
@requires.preflight
@requires.profile
@requires.project
@requires.runtime_config
@requires.manifest
def compile(ctx, **kwargs):
    """Generates executable SQL from source, model, test, and analysis files."""
    from dbt.task.compile import CompileTask

    task = CompileTask(
        ctx.obj["flags"],
        ctx.obj["runtime_config"],
        ctx.obj["manifest"],
    )
    results = task.run()
    success = task.interpret_results(results)
    return results, success


# ---------------------------------------------------------------------------
# dvt test
# ---------------------------------------------------------------------------
@cli.command("test")
@click.pass_context
@global_flags
@p.exclude
@p.profiles_dir
@p.project_dir
@p.select
@p.selector
@p.store_failures
@p.target_path
@p.threads
@p.vars
@requires.postflight
@requires.preflight
@requires.profile
@requires.project
@requires.runtime_config
@requires.manifest
def test(ctx, **kwargs):
    """Runs tests on data in deployed models."""
    from dbt.task.test import TestTask

    task = TestTask(
        ctx.obj["flags"],
        ctx.obj["runtime_config"],
        ctx.obj["manifest"],
    )
    results = task.run()
    success = task.interpret_results(results)
    return results, success


# ---------------------------------------------------------------------------
# dvt seed
# ---------------------------------------------------------------------------
@cli.command("seed")
@click.pass_context
@global_flags
@p.exclude
@p.full_refresh
@p.profiles_dir
@p.project_dir
@p.select
@p.selector
@p.show
@p.target_path
@p.threads
@p.vars
@requires.postflight
@requires.preflight
@requires.profile
@requires.project
@requires.runtime_config
@requires.manifest
def seed(ctx, **kwargs):
    """Load data from csv files into your data warehouse via Sling."""
    from dvt.tasks.seed import DvtSeedTask

    task = DvtSeedTask(
        ctx.obj["flags"],
        ctx.obj["runtime_config"],
        ctx.obj["manifest"],
    )
    results = task.run()
    success = task.interpret_results(results)
    return results, success


# ---------------------------------------------------------------------------
# dvt snapshot
# ---------------------------------------------------------------------------
@cli.command("snapshot")
@click.pass_context
@global_flags
@p.exclude
@p.profiles_dir
@p.project_dir
@p.select
@p.selector
@p.target_path
@p.threads
@p.vars
@requires.postflight
@requires.preflight
@requires.profile
@requires.project
@requires.runtime_config
@requires.manifest
def snapshot(ctx, **kwargs):
    """Execute snapshots defined in your project."""
    from dbt.task.snapshot import SnapshotTask

    task = SnapshotTask(
        ctx.obj["flags"],
        ctx.obj["runtime_config"],
        ctx.obj["manifest"],
    )
    results = task.run()
    success = task.interpret_results(results)
    return results, success


# ---------------------------------------------------------------------------
# dvt show
# ---------------------------------------------------------------------------
@cli.command("show")
@click.pass_context
@global_flags
@p.exclude
@p.full_refresh
@p.show_output_format
@p.show_limit
@p.introspect
@p.profiles_dir
@p.project_dir
@p.select
@p.selector
@p.inline
@p.inline_direct
@p.target_path
@p.threads
@p.vars
@requires.postflight
@requires.preflight
@requires.profile
@requires.project
@requires.runtime_config
def show(ctx, **kwargs):
    """Run a query locally via DuckDB without hitting the warehouse."""
    from dvt.tasks.show import DvtShowTask

    # Load manifest if --select is used (needed to find model SQL)
    manifest = None
    select = getattr(ctx.obj.get("flags"), "SELECT", None)
    if select:
        from dbt.cli.requires import setup_manifest

        setup_manifest(ctx)
        manifest = ctx.obj.get("manifest")

    task = DvtShowTask(
        ctx.obj["flags"],
        ctx.obj["runtime_config"],
        manifest,
    )
    results = task.run()
    success = task.interpret_results(results)
    return results, success


# ---------------------------------------------------------------------------
# dvt clean
# ---------------------------------------------------------------------------
@cli.command("clean")
@click.pass_context
@global_flags
@p.clean_project_files_only
@p.profiles_dir
@p.project_dir
@p.target_path
@p.vars
@requires.postflight
@requires.preflight
@requires.unset_profile
@requires.project
def clean(ctx, **kwargs):
    """Delete all folders in the clean-targets list and the DVT cache."""
    import os
    import shutil

    from dbt.task.clean import CleanTask

    # Standard dbt clean
    with CleanTask(ctx.obj["flags"], ctx.obj["project"]) as task:
        results = task.run()
        success = task.interpret_results(results)

    # Also delete .dvt/ cache directory
    project_dir = getattr(ctx.obj["flags"], "PROJECT_DIR", None) or "."
    dvt_cache_dir = os.path.join(os.path.abspath(project_dir), ".dvt")
    if os.path.isdir(dvt_cache_dir):
        shutil.rmtree(dvt_cache_dir, ignore_errors=True)
        click.echo(f"  Deleted DVT cache: {dvt_cache_dir}")

    return results, success


# ---------------------------------------------------------------------------
# dvt deps
# ---------------------------------------------------------------------------
@cli.command("deps")
@click.pass_context
@global_flags
@p.profiles_dir_exists_false
@p.project_dir
@p.vars
@p.source
@p.lock
@p.upgrade
@p.add_package
@requires.postflight
@requires.preflight
@requires.unset_profile
@requires.project
def deps(ctx, **kwargs):
    """Pull the most recent version of the dependencies listed in packages.yml."""
    from dbt.task.deps import DepsTask

    flags = ctx.obj["flags"]
    if flags.ADD_PACKAGE:
        if not flags.ADD_PACKAGE["version"] and flags.SOURCE != "local":
            from click.exceptions import BadOptionUsage

            raise BadOptionUsage(
                message=f"Version is required in --add-package when source is {flags.SOURCE}",
                option_name="--add-package",
            )
    with DepsTask(flags, ctx.obj["project"]) as task:
        results = task.run()
        success = task.interpret_results(results)
    return results, success


# ---------------------------------------------------------------------------
# dvt debug
# ---------------------------------------------------------------------------
@cli.command("debug")
@click.pass_context
@global_flags
@p.config_dir
@p.profiles_dir_exists_false
@p.project_dir
@p.vars
@requires.postflight
@requires.preflight
def debug(ctx, **kwargs):
    """Check connections to all targets in profiles.yml, or a specific --target."""
    from dvt.tasks.debug import DvtDebugTask

    task = DvtDebugTask(ctx.obj["flags"])
    results = task.run()
    success = task.interpret_results(results)
    return results, success


# ---------------------------------------------------------------------------
# dvt init
# ---------------------------------------------------------------------------
@cli.command("init")
@click.pass_context
@global_flags
@click.argument("project_name", required=False)
@p.profiles_dir_exists_false
@p.project_dir
@p.skip_profile_setup
@p.vars
@requires.postflight
@requires.preflight
def init(ctx, **kwargs):
    """Initialize a new DVT project."""
    from dbt.task.init import InitTask

    with InitTask(ctx.obj["flags"]) as task:
        results = task.run()
        success = task.interpret_results(results)
    return results, success


# ---------------------------------------------------------------------------
# dvt list (ls)
# ---------------------------------------------------------------------------
@cli.command("list")
@click.pass_context
@global_flags
@p.exclude
@p.indirect_selection
@p.models
@p.output
@p.output_keys
@p.profiles_dir
@p.project_dir
@p.resource_type
@p.exclude_resource_type
@p.select
@p.selector
@p.target_path
@p.vars
@requires.postflight
@requires.preflight
@requires.profile
@requires.project
@requires.runtime_config
@requires.manifest
def list(ctx, **kwargs):
    """List the resources in your project."""
    from dbt.task.list import ListTask

    task = ListTask(
        ctx.obj["flags"],
        ctx.obj["runtime_config"],
        ctx.obj["manifest"],
    )
    results = task.run()
    success = task.interpret_results(results)
    return results, success


# ---------------------------------------------------------------------------
# dvt parse
# ---------------------------------------------------------------------------
@cli.command("parse")
@click.pass_context
@global_flags
@p.profiles_dir
@p.project_dir
@p.target_path
@p.threads
@p.vars
@requires.postflight
@requires.preflight
@requires.profile
@requires.project
@requires.runtime_config
@requires.manifest
def parse(ctx, **kwargs):
    """Parses the project and provides information on performance."""
    # manifest generation and writing happens in @requires.manifest
    return ctx.obj["manifest"], True


# ---------------------------------------------------------------------------
# dvt retry
# ---------------------------------------------------------------------------
@cli.command("retry")
@click.pass_context
@global_flags
@p.project_dir
@p.profiles_dir
@p.vars
@p.target_path
@p.threads
@p.full_refresh
@requires.postflight
@requires.preflight
@requires.profile
@requires.project
@requires.runtime_config
@requires.manifest
def retry(ctx, **kwargs):
    """Retry the nodes that failed in the previous run."""
    from dbt.task.retry import RetryTask

    task = RetryTask(
        ctx.obj["flags"],
        ctx.obj["runtime_config"],
        ctx.obj["manifest"],
    )
    results = task.run()
    success = task.interpret_results(results)
    return results, success


# ---------------------------------------------------------------------------
# dvt clone
# ---------------------------------------------------------------------------
@cli.command("clone")
@click.pass_context
@global_flags
@p.exclude
@p.full_refresh
@p.profiles_dir
@p.project_dir
@p.select
@p.selector
@p.target_path
@p.threads
@p.vars
@requires.postflight
@requires.preflight
@requires.profile
@requires.project
@requires.runtime_config
@requires.manifest
def clone(ctx, **kwargs):
    """Clone selected nodes from the specified state."""
    from dbt.task.clone import CloneTask

    task = CloneTask(
        ctx.obj["flags"],
        ctx.obj["runtime_config"],
        ctx.obj["manifest"],
    )
    results = task.run()
    success = task.interpret_results(results)
    return results, success


# ---------------------------------------------------------------------------
# dvt run-operation
# ---------------------------------------------------------------------------
@cli.command("run-operation")
@click.pass_context
@global_flags
@p.args
@p.profiles_dir
@p.project_dir
@p.target_path
@p.threads
@p.vars
@requires.postflight
@requires.preflight
@requires.profile
@requires.project
@requires.runtime_config
@requires.manifest
def run_operation(ctx, **kwargs):
    """Run the named macro with any supplied arguments."""
    from dbt.task.run_operation import RunOperationTask

    task = RunOperationTask(
        ctx.obj["flags"],
        ctx.obj["runtime_config"],
        ctx.obj["manifest"],
    )
    results = task.run()
    success = task.interpret_results(results)
    return results, success


# ---------------------------------------------------------------------------
# dvt docs (group)
# ---------------------------------------------------------------------------
@cli.group()
@click.pass_context
@global_flags
def docs(ctx, **kwargs):
    """Generate or serve the documentation website for your project."""


# ---------------------------------------------------------------------------
# dvt docs generate
# ---------------------------------------------------------------------------
@docs.command("generate")
@click.pass_context
@global_flags
@p.compile_docs
@p.exclude
@p.profiles_dir
@p.project_dir
@p.select
@p.selector
@p.target_path
@p.threads
@p.vars
@requires.postflight
@requires.preflight
@requires.profile
@requires.project
@requires.runtime_config
@requires.manifest
def docs_generate(ctx, **kwargs):
    """Generate the documentation website for your project."""
    from dbt.task.docs.generate import GenerateTask

    task = GenerateTask(
        ctx.obj["flags"],
        ctx.obj["runtime_config"],
        ctx.obj["manifest"],
    )
    results = task.run()
    success = task.interpret_results(results)
    return results, success


# ---------------------------------------------------------------------------
# dvt docs serve
# ---------------------------------------------------------------------------
@docs.command("serve")
@click.pass_context
@global_flags
@p.browser
@p.port
@p.profiles_dir
@p.project_dir
@p.target_path
@p.vars
@requires.postflight
@requires.preflight
@requires.profile
@requires.project
@requires.runtime_config
def docs_serve(ctx, **kwargs):
    """Serve the documentation website for your project."""
    from dbt.task.docs.serve import ServeTask

    task = ServeTask(
        ctx.obj["flags"],
        ctx.obj["runtime_config"],
    )
    results = task.run()
    success = task.interpret_results(results)
    return results, success


# ---------------------------------------------------------------------------
# dvt source (group)
# ---------------------------------------------------------------------------
@cli.group()
@click.pass_context
@global_flags
def source(ctx, **kwargs):
    """Manage your project's sources."""


# ---------------------------------------------------------------------------
# dvt source freshness
# ---------------------------------------------------------------------------
@source.command("freshness")
@click.pass_context
@global_flags
@p.exclude
@p.output_path
@p.profiles_dir
@p.project_dir
@p.select
@p.selector
@p.target_path
@p.threads
@p.vars
@requires.postflight
@requires.preflight
@requires.profile
@requires.project
@requires.runtime_config
@requires.manifest
def freshness(ctx, **kwargs):
    """Check the freshness of your project's sources."""
    from dbt.task.freshness import FreshnessTask

    task = FreshnessTask(
        ctx.obj["flags"],
        ctx.obj["runtime_config"],
        ctx.obj["manifest"],
    )
    results = task.run()
    success = task.interpret_results(results)
    return results, success


# ---------------------------------------------------------------------------
# dvt sync (DVT-SPECIFIC — works even when dbt imports are broken)
# ---------------------------------------------------------------------------
@cli.command("sync")
@click.pass_context
@click.option(
    "--profiles-dir",
    default=None,
    type=click.Path(exists=True),
    help="Directory containing profiles.yml.",
)
@click.option(
    "--skip-test",
    is_flag=True,
    default=False,
    help="Skip connection testing after sync.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Show what would be installed without installing.",
)
def sync(ctx, **kwargs):
    """Sync DVT environment: install adapters, DuckDB extensions, verify Sling, and fix conflicts."""
    from dvt.tasks.sync import DvtSyncTask

    # Build a minimal flags-like object from kwargs (no dbt dependency)
    class _SyncFlags:
        pass

    flags = _SyncFlags()
    flags.PROFILES_DIR = kwargs.get("profiles_dir")

    task = DvtSyncTask(flags, kwargs)
    results = task.run()
    success = task.interpret_results(results)
    if not success:
        ctx.exit(1)
