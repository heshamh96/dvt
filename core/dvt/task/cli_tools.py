"""
CLI tool detection for pipe-optimized data transfer.

Detects whether database CLI tools (psql, mysql, bcp, clickhouse-client, etc.)
are available on the system PATH. When present, DVT can use pipe-based
extraction/loading for constant-memory data transfer.

Used by `dvt sync` (Step 9) to inform users about available optimizations.
"""

import platform
import shutil
import subprocess
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple


@dataclass
class CLIToolInfo:
    """Information about a database CLI tool."""

    tool_name: str  # Binary name (e.g., "psql")
    adapter_type: str  # Primary adapter type (e.g., "postgres")
    compatible_adapters: List[str]  # All adapter types this tool covers
    version_command: List[str]  # Command to check version
    install_instructions: Dict[str, str]  # OS -> install command
    docs_url: str  # Documentation link


# Registry of CLI tools for pipe-optimized transfer
CLI_TOOL_REGISTRY: List[CLIToolInfo] = [
    CLIToolInfo(
        tool_name="psql",
        adapter_type="postgres",
        compatible_adapters=[
            "postgres",
            "greenplum",
            "cockroachdb",
            "alloydb",
            "materialize",
            "citus",
            "timescaledb",
            "neon",
            "risingwave",
            "cratedb",
        ],
        version_command=["psql", "--version"],
        install_instructions={
            "macOS": "brew install libpq",
            "Debian/Ubuntu": "sudo apt install postgresql-client",
            "RHEL/CentOS": "sudo yum install postgresql",
            "Alpine": "apk add postgresql-client",
        },
        docs_url="https://www.postgresql.org/docs/current/app-psql.html",
    ),
    CLIToolInfo(
        tool_name="mysql",
        adapter_type="mysql",
        compatible_adapters=[
            "mysql",
            "mariadb",
            "tidb",
            "singlestore",
            "planetscale",
            "vitess",
            "aurora_mysql",
        ],
        version_command=["mysql", "--version"],
        install_instructions={
            "macOS": "brew install mysql-client",
            "Debian/Ubuntu": "sudo apt install mysql-client",
            "RHEL/CentOS": "sudo yum install mysql",
            "Alpine": "apk add mysql-client",
        },
        docs_url="https://dev.mysql.com/doc/refman/en/mysql.html",
    ),
    CLIToolInfo(
        tool_name="bcp",
        adapter_type="sqlserver",
        compatible_adapters=["sqlserver", "synapse", "fabric"],
        version_command=["bcp", "-v"],
        install_instructions={
            "macOS": "brew install mssql-tools18",
            "Debian/Ubuntu": "See https://learn.microsoft.com/en-us/sql/tools/bcp-utility",
            "RHEL/CentOS": "See https://learn.microsoft.com/en-us/sql/tools/bcp-utility",
        },
        docs_url="https://learn.microsoft.com/en-us/sql/tools/bcp-utility",
    ),
    CLIToolInfo(
        tool_name="clickhouse-client",
        adapter_type="clickhouse",
        compatible_adapters=["clickhouse"],
        version_command=["clickhouse-client", "--version"],
        install_instructions={
            "macOS": "brew install clickhouse",
            "Debian/Ubuntu": "See https://clickhouse.com/docs/en/install",
            "RHEL/CentOS": "See https://clickhouse.com/docs/en/install",
        },
        docs_url="https://clickhouse.com/docs/en/interfaces/cli",
    ),
    CLIToolInfo(
        tool_name="sqlplus",
        adapter_type="oracle",
        compatible_adapters=["oracle"],
        version_command=["sqlplus", "-v"],
        install_instructions={
            "macOS": "Download Oracle Instant Client from https://www.oracle.com/database/technologies/instant-client.html",
            "Linux": "Download Oracle Instant Client from https://www.oracle.com/database/technologies/instant-client.html",
        },
        docs_url="https://docs.oracle.com/en/database/oracle/oracle-database/19/sqpug/",
    ),
]


@dataclass
class CLIToolStatus:
    """Detection result for a single CLI tool."""

    tool: CLIToolInfo
    found: bool
    path: Optional[str] = None  # Full path if found
    version: Optional[str] = None  # Version string if detected


def _get_tool_version(version_command: List[str]) -> Optional[str]:
    """Run version command and extract version string."""
    try:
        result = subprocess.run(
            version_command,
            capture_output=True,
            text=True,
            timeout=5,
        )
        output = (result.stdout or "") + (result.stderr or "")
        # Return first non-empty line as version info
        for line in output.strip().splitlines():
            line = line.strip()
            if line:
                return line
        return None
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        return None


def detect_cli_tools(
    adapter_types: List[str],
) -> List[CLIToolStatus]:
    """Detect CLI tools for the given adapter types.

    Only checks tools relevant to the adapters in use. Cloud adapters
    (snowflake, bigquery, redshift, databricks) are skipped since they
    use bucket-based transfer (Tier 2).

    Args:
        adapter_types: List of adapter types from profiles.yml

    Returns:
        List of CLIToolStatus for each relevant tool
    """
    results = []
    seen_tools = set()

    for tool_info in CLI_TOOL_REGISTRY:
        # Check if any of the user's adapters match this tool
        matching = set(adapter_types) & set(tool_info.compatible_adapters)
        if not matching:
            continue

        # Avoid duplicate checks (e.g., psql checked once for postgres + greenplum)
        if tool_info.tool_name in seen_tools:
            continue
        seen_tools.add(tool_info.tool_name)

        # Check if tool is on PATH
        tool_path = shutil.which(tool_info.tool_name)
        if tool_path:
            version = _get_tool_version(tool_info.version_command)
            results.append(
                CLIToolStatus(
                    tool=tool_info,
                    found=True,
                    path=tool_path,
                    version=version,
                )
            )
        else:
            results.append(
                CLIToolStatus(
                    tool=tool_info,
                    found=False,
                )
            )

    return results


def _detect_platform() -> str:
    """Detect the current platform for install instructions."""
    system = platform.system()
    if system == "Darwin":
        return "macOS"
    elif system == "Linux":
        # Try to detect distro
        try:
            with open("/etc/os-release") as f:
                content = f.read().lower()
                if "debian" in content or "ubuntu" in content:
                    return "Debian/Ubuntu"
                elif "rhel" in content or "centos" in content or "fedora" in content:
                    return "RHEL/CentOS"
                elif "alpine" in content:
                    return "Alpine"
        except (FileNotFoundError, PermissionError):
            pass
        return "Debian/Ubuntu"  # Default Linux
    return "macOS"  # Default fallback


def format_cli_tool_report(
    results: List[CLIToolStatus],
) -> str:
    """Format CLI tool detection results for display.

    Returns a formatted string suitable for terminal output.
    """
    if not results:
        return "  No on-prem database adapters detected -- CLI tool check skipped."

    lines = []
    lines.append("")
    lines.append(f"  {'Adapter':<18} {'CLI Tool':<22} {'Status'}")
    lines.append(f"  {'─' * 18} {'─' * 22} {'─' * 40}")

    found_tools = []
    missing_tools = []

    for status in results:
        adapter = status.tool.adapter_type
        tool = status.tool.tool_name

        if status.found:
            version_str = ""
            if status.version:
                # Truncate long version strings
                ver = status.version
                if len(ver) > 35:
                    ver = ver[:32] + "..."
                version_str = f" ({ver})"
            lines.append(f"  {adapter:<18} {tool:<22} found{version_str}")
            found_tools.append(status)
        else:
            lines.append(f"  {adapter:<18} {tool:<22} not found")
            missing_tools.append(status)

    # Show install instructions for missing tools
    if missing_tools:
        current_platform = _detect_platform()
        lines.append("")
        lines.append("  Missing CLI tools for pipe-optimized transfer:")
        lines.append("")

        for status in missing_tools:
            tool = status.tool
            lines.append(f"    {tool.adapter_type}:")

            # Show install instruction for current platform first
            if current_platform in tool.install_instructions:
                lines.append(
                    f"      {current_platform}:  {tool.install_instructions[current_platform]}"
                )
            # Show other platforms
            for os_name, cmd in tool.install_instructions.items():
                if os_name != current_platform:
                    lines.append(f"      {os_name}:  {cmd}")
            lines.append(f"      Docs:  {tool.docs_url}")
            lines.append("")

    lines.append(
        "  Without CLI tools, DVT uses Spark JDBC fallback (higher memory usage)."
    )
    lines.append(
        "  PyArrow (built-in) handles CSV-to-Parquet conversion -- no extra tools needed."
    )

    return "\n".join(lines)
