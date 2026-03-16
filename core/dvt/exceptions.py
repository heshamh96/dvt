"""
DVT error codes and exception classes.

Each error has a unique code (DVT100-DVT111) for easy identification
in logs and error messages.
"""


class DvtError(Exception):
    """Base exception for all DVT errors."""

    code: str = "DVT000"

    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(f"{self.code}: {message}")


# ---------------------------------------------------------------------------
# Configuration errors (100-109)
# ---------------------------------------------------------------------------


class DvtSourceConnectionRequired(DvtError):
    """Source is missing a connection property in sources.yml."""

    code = "DVT100"


class DvtConnectionNotFound(DvtError):
    """Source or model references a connection not in profiles.yml."""

    code = "DVT101"


class DvtConflictingConnections(DvtError):
    """Same source defined with different connection values."""

    code = "DVT102"


class DvtCrossTargetTest(DvtError):
    """Test references nodes in different targets."""

    code = "DVT103"


class DvtSelfReference(DvtError):
    """Non-incremental model references itself."""

    code = "DVT104"


class DvtSchemaChange(DvtError):
    """Column types changed without --full-refresh."""

    code = "DVT105"


class DvtSlingNotFound(DvtError):
    """Sling binary not found when extraction is needed."""

    code = "DVT106"


class DvtUnknownMaterialization(DvtError):
    """Invalid materialization type."""

    code = "DVT108"


class DvtWatermarkFormatError(DvtError):
    """Cannot format watermark value for source dialect."""

    code = "DVT109"


# ---------------------------------------------------------------------------
# Warnings (001-010)
# ---------------------------------------------------------------------------


class DvtMaterializationCoerced(DvtError):
    """View/ephemeral coerced to table for extraction model."""

    code = "DVT001"


class DvtTypePrecisionLoss(DvtError):
    """Exotic type mapped to text/json during extraction."""

    code = "DVT002"
