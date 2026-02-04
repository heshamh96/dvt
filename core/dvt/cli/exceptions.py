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

    def show(self, _file: Optional[IO] = None) -> None:  # type: ignore[type-arg]
        """Print error message when command fails with no result."""
        if self.result is None:
            # Command failed but didn't return a result - likely a sync or init failure
            # Error should have been logged by the task, but ensure user sees something
            sys.stderr.write("Command failed. Check the output above for details.\n")
            sys.stderr.flush()


class ExceptionExit(CliException):
    """This class wraps any exception that does not contain results thrown while invoking dvt."""

    def __init__(self, exception: Exception) -> None:
        super().__init__(ExitCodes.UnhandledError)
        self.exception = exception
        # For UninstalledPackagesFoundError, don't set message to prevent Click from printing it
        # (it's already printed once in the exception handler to avoid duplicates)
        from dvt.exceptions import UninstalledPackagesFoundError
        if isinstance(exception, UninstalledPackagesFoundError):
            # Set empty message so Click doesn't print it again
            self.message = ""
        else:
            # Set message so Click will display it even if show() is not called
            try:
                self.message = str(exception)
            except Exception:
                self.message = f"{type(exception).__name__}: <exception str() failed>"
            # Immediately print the exception to stderr so it's never silent
            # Use format_exception to get the full traceback
            exc_str = "".join(
                traceback.format_exception(
                    type(exception),
                    exception,
                    getattr(exception, "__traceback__", None),
                )
            )
            sys.stderr.write(exc_str)
            sys.stderr.flush()

    def show(self, _file: Optional[IO] = None) -> None:  # type: ignore[type-arg]
        """Print the wrapped exception to stderr so exit code 2 is never silent."""
        if self.exception is not None:
            # For UninstalledPackagesFoundError, don't print here - it's already printed once
            # in the exception handler to avoid duplicates as exception propagates through decorators
            from dvt.exceptions import UninstalledPackagesFoundError
            if isinstance(self.exception, UninstalledPackagesFoundError):
                # Don't print - already printed in exception handler
                pass
            else:
                exc_str = "".join(
                    traceback.format_exception(
                        type(self.exception),
                        self.exception,
                        getattr(self.exception, "__traceback__", None),
                    )
                )
                sys.stderr.write(exc_str)
                sys.stderr.flush()
        elif self.message:
            sys.stderr.write(f"{self.message}\n")
            sys.stderr.flush()
