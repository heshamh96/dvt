import sys
import traceback
from typing import IO, List, Optional, Union

from click.exceptions import ClickException

from dvt.artifacts.schemas.catalog import CatalogArtifact
from dvt.contracts.graph.manifest import Manifest
from dvt.contracts.results import RunExecutionResult
from dvt.utils import ExitCodes


class DvtUsageException(Exception):
    pass


class DvtInternalException(Exception):
    pass


class CliException(ClickException):
    """The base exception class for our implementation of the click CLI.
    The exit_code attribute is used by click to determine which exit code to produce
    after an invocation."""

    def __init__(self, exit_code: ExitCodes) -> None:
        self.exit_code = exit_code.value

    # the typing of _file is to satisfy the signature of ClickException.show
    # overriding this method prevents click from printing any exceptions to stdout
    def show(self, _file: Optional[IO] = None) -> None:  # type: ignore[type-arg]
        pass


class ResultExit(CliException):
    """This class wraps any exception that contains results while invoking dvt, or the
    results of an invocation that did not succeed but did not throw any exceptions."""

    def __init__(
        self,
        result: Union[
            bool,  # debug
            CatalogArtifact,  # docs generate
            List[str],  # list/ls
            Manifest,  # parse
            None,  # clean, deps, init, source
            RunExecutionResult,  # build, compile, run, seed, snapshot, test, run-operation
        ] = None,
    ) -> None:
        super().__init__(ExitCodes.ModelError)
        self.result = result


class ExceptionExit(CliException):
    """This class wraps any exception that does not contain results thrown while invoking dvt."""

    def __init__(self, exception: Exception) -> None:
        super().__init__(ExitCodes.UnhandledError)
        self.exception = exception

    def show(self, _file: Optional[IO] = None) -> None:  # type: ignore[type-arg]
        """Print the wrapped exception to stderr so exit code 2 is never silent."""
        if self.exception is not None:
            sys.stderr.write(
                "".join(
                    traceback.format_exception(
                        type(self.exception),
                        self.exception,
                        getattr(self.exception, "__traceback__", None),
                    )
                )
            )
            sys.stderr.flush()
