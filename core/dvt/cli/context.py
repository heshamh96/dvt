from typing import Optional

import click

from dvt.cli.main import cli as dvt


def make_context(args, command=dvt) -> Optional[click.Context]:
    try:
        ctx = command.make_context(command.name, args)
    except click.exceptions.Exit:
        return None

    ctx.invoked_subcommand = ctx.protected_args[0] if ctx.protected_args else None
    ctx.obj = {}

    return ctx
