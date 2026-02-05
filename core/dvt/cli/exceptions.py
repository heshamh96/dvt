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
        # For UninstalledPackagesFoundError, PySparkNotInstalledError, adapter-missing, and
        # project-not-found errors, don't print - they're already handled with clean messages in requires.py
        from dvt.exceptions import UninstalledPackagesFoundError, PySparkNotInstalledError, DvtProfileError, DvtProjectError
        skip_print = False
        if isinstance(exception, UninstalledPackagesFoundError):
            skip_print = True
        elif isinstance(exception, PySparkNotInstalledError):
            skip_print = True
        elif isinstance(exception, DvtProfileError):
            exc_str_check = str(exception)
            if "Could not find adapter" in exc_str_check or "Run 'dvt sync'" in exc_str_check:
                skip_print = True
        elif isinstance(exception, DvtProjectError):
            exc_str_check = str(exception)
            if "No dbt_project.yml" in exc_str_check or "No dvt_project.yml" in exc_str_check or "not found at expected path" in exc_str_check:
                skip_print = True

        if skip_print:
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
            # For UninstalledPackagesFoundError, PySparkNotInstalledError, adapter-missing, and
            # project-not-found errors, don't print - they're already handled with clean messages in requires.py
            from dvt.exceptions import UninstalledPackagesFoundError, PySparkNotInstalledError, DvtProfileError, DvtProjectError
            skip_print = False
            if isinstance(self.exception, UninstalledPackagesFoundError):
                skip_print = True
            elif isinstance(self.exception, PySparkNotInstalledError):
                skip_print = True
            elif isinstance(self.exception, DvtProfileError):
                exc_str_check = str(self.exception)
                if "Could not find adapter" in exc_str_check or "Run 'dvt sync'" in exc_str_check:
                    skip_print = True
            elif isinstance(self.exception, DvtProjectError):
                exc_str_check = str(self.exception)
                if "No dbt_project.yml" in exc_str_check or "No dvt_project.yml" in exc_str_check or "not found at expected path" in exc_str_check:
                    skip_print = True

            if not skip_print:
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
