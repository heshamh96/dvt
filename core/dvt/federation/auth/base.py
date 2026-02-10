# coding=utf-8
"""Base authentication handler for DVT federation.

All adapter-specific auth handlers inherit from this base class,
ensuring a consistent interface for:
- JDBC property building
- Native connector kwargs
- Auth method detection
- Interactive auth detection (for error handling)
- Temporary credentials file management
"""

import atexit
import os
import tempfile
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple


class BaseAuthHandler(ABC):
    """Base class for adapter authentication handlers.

    Each adapter implements its own handler that knows how to:
    1. Detect which auth method is configured
    2. Build JDBC connection properties for Spark
    3. Build native Python connector kwargs
    4. Identify interactive auth methods (SSO, browser-based)
    5. Manage temporary credentials files
    """

    # Override in subclasses: list of supported auth methods
    SUPPORTED_AUTH_METHODS: List[str] = ["password"]

    # Override in subclasses: auth methods requiring user interaction
    INTERACTIVE_AUTH_METHODS: List[str] = []

    # Class-level registry of temp files to clean up
    _temp_files: List[str] = []

    @classmethod
    def register_temp_file(cls, filepath: str) -> None:
        """Register a temp file for cleanup on exit."""
        cls._temp_files.append(filepath)

    @classmethod
    def cleanup_temp_files(cls) -> None:
        """Remove all registered temp files."""
        for filepath in cls._temp_files:
            try:
                if os.path.exists(filepath):
                    os.remove(filepath)
            except Exception:
                pass  # Best effort cleanup
        cls._temp_files.clear()

    @classmethod
    def create_temp_credentials_file(
        cls,
        content: str,
        suffix: str = ".json",
        prefix: str = "dvt_creds_",
    ) -> str:
        """Create a temp credentials file and register for cleanup.

        Args:
            content: File content (e.g., JSON string)
            suffix: File suffix
            prefix: File prefix

        Returns:
            Path to the temp file
        """
        fd, filepath = tempfile.mkstemp(suffix=suffix, prefix=prefix)
        try:
            with os.fdopen(fd, "w") as f:
                f.write(content)
            cls.register_temp_file(filepath)
            return filepath
        except Exception:
            os.close(fd)
            raise

    @abstractmethod
    def detect_auth_method(self, creds: Dict[str, Any]) -> str:
        """Detect which auth method is configured from credentials.

        Args:
            creds: Credential dict from extract_all_credentials()

        Returns:
            Auth method name (e.g., 'password', 'keypair', 'oauth', 'iam')
        """
        pass

    @abstractmethod
    def get_jdbc_properties(self, creds: Dict[str, Any]) -> Dict[str, str]:
        """Build JDBC connection properties for Spark.

        Args:
            creds: Credential dict

        Returns:
            Dict of JDBC properties (user, password, and auth-specific props)
        """
        pass

    def get_jdbc_url_params(self, creds: Dict[str, Any]) -> str:
        """Get additional JDBC URL parameters for authentication.

        Some auth methods require URL parameters rather than properties.
        Override in subclasses as needed.

        Args:
            creds: Credential dict

        Returns:
            URL parameter string (e.g., "authenticator=externalbrowser")
        """
        return ""

    @abstractmethod
    def get_native_connection_kwargs(self, creds: Dict[str, Any]) -> Dict[str, Any]:
        """Get kwargs for native Python driver connection.

        Args:
            creds: Credential dict

        Returns:
            Dict of kwargs for the native connector
        """
        pass

    def is_interactive(self, creds: Dict[str, Any]) -> bool:
        """Check if the configured auth method requires user interaction.

        Interactive methods (SSO, browser OAuth) don't work in headless
        Spark/JDBC contexts. We error early with a helpful message.

        Args:
            creds: Credential dict

        Returns:
            True if auth method requires browser/user interaction
        """
        auth_method = self.detect_auth_method(creds)
        return auth_method in self.INTERACTIVE_AUTH_METHODS

    def get_interactive_error_message(self, creds: Dict[str, Any]) -> str:
        """Get error message for interactive auth methods.

        Args:
            creds: Credential dict

        Returns:
            User-friendly error message with alternatives
        """
        auth_method = self.detect_auth_method(creds)
        adapter_type = creds.get("type", "unknown")
        non_interactive = [
            m
            for m in self.SUPPORTED_AUTH_METHODS
            if m not in self.INTERACTIVE_AUTH_METHODS
        ]
        return (
            f"Authentication method '{auth_method}' for {adapter_type} requires "
            f"browser interaction and cannot be used with Spark/JDBC federation.\n"
            f"Please use a non-interactive auth method such as: {', '.join(non_interactive)}"
        )

    def validate(self, creds: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """Validate credentials for federation use.

        Args:
            creds: Credential dict

        Returns:
            Tuple of (is_valid, error_message)
        """
        if self.is_interactive(creds):
            return False, self.get_interactive_error_message(creds)
        return True, None


# Register cleanup on interpreter exit
atexit.register(BaseAuthHandler.cleanup_temp_files)
