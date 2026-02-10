"""
Column pruning for EL layer.

Identifies which columns from a source are actually used in a model,
allowing extraction of only needed columns for efficiency.

Uses SQLGlot for SQL parsing.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Set

try:
    import sqlglot
    from sqlglot import exp

    SQLGLOT_AVAILABLE = True
except ImportError:
    SQLGLOT_AVAILABLE = False


@dataclass
class SourceColumns:
    """Columns used from a specific source."""

    source_alias: str  # Table alias in query
    columns: Set[str]  # Column names used
    has_star: bool  # True if SELECT * was used for this source


def extract_required_columns(
    sql: str,
    source_alias: str,
    all_columns: Optional[List[str]] = None,
    target_dialect: str = "spark",
) -> List[str]:
    """Identify which columns from a source are actually used in the query.

    Args:
        sql: The model SQL to analyze
        source_alias: Table alias to find columns for (e.g., 'o' for orders)
        all_columns: All available columns for this source (used when SELECT * is found)
        target_dialect: SQL dialect of the model

    Returns:
        List of column names used from this source

    Example:
        SQL: SELECT o.id, o.total, c.name FROM orders o JOIN customers c ON o.cust_id = c.id

        For source 'orders' (alias 'o'):
        Returns: ['id', 'total', 'cust_id']  # includes JOIN columns
    """
    if not SQLGLOT_AVAILABLE:
        return all_columns or []

    try:
        parsed = sqlglot.parse_one(sql, read=target_dialect)
    except Exception:
        return all_columns or []

    used_columns: Set[str] = set()
    has_unqualified_star = False
    has_qualified_star = False

    # Find all column references in the entire query
    for col in parsed.find_all(exp.Column):
        table = col.table
        name = col.name

        if table == source_alias:
            used_columns.add(name)
        elif table is None:
            # Unqualified column - might be from our source or ambiguous
            used_columns.add(name)

    # Check for SELECT *
    for star in parsed.find_all(exp.Star):
        table = getattr(star, "table", None)
        if table == source_alias:
            has_qualified_star = True
        elif table is None:
            has_unqualified_star = True

    # If any star was used that could apply to this source, return all columns
    if has_qualified_star or has_unqualified_star:
        return all_columns or []

    return list(used_columns) if used_columns else (all_columns or [])


def get_columns_for_sources(
    model_sql: str,
    source_info: Dict[str, Dict],
    target_dialect: str = "spark",
) -> Dict[str, List[str]]:
    """Get required columns for multiple sources from model SQL.

    Args:
        model_sql: The model SQL to analyze
        source_info: Dict mapping source names to info:
                    {
                        'postgres__orders': {
                            'alias': 'o',
                            'columns': ['id', 'customer_id', 'total', 'date']
                        }
                    }
        target_dialect: SQL dialect of the model

    Returns:
        Dict mapping source names to list of required columns
        e.g., {'postgres__orders': ['id', 'total', 'date']}
    """
    result: Dict[str, List[str]] = {}

    for source_name, info in source_info.items():
        alias = info.get("alias", "")
        all_columns = info.get("columns", [])

        required = extract_required_columns(
            model_sql,
            alias,
            all_columns,
            target_dialect,
        )

        result[source_name] = required

    return result


def analyze_query_columns(
    sql: str,
    target_dialect: str = "spark",
) -> Dict[str, SourceColumns]:
    """Analyze a query to find all column references by table alias.

    Args:
        sql: The SQL query to analyze
        target_dialect: SQL dialect of the query

    Returns:
        Dict mapping table aliases to SourceColumns info
    """
    if not SQLGLOT_AVAILABLE:
        return {}

    try:
        parsed = sqlglot.parse_one(sql, read=target_dialect)
    except Exception:
        return {}

    result: Dict[str, SourceColumns] = {}

    # Collect all column references
    for col in parsed.find_all(exp.Column):
        table = col.table or "_unqualified_"
        name = col.name

        if table not in result:
            result[table] = SourceColumns(
                source_alias=table,
                columns=set(),
                has_star=False,
            )
        result[table].columns.add(name)

    # Check for SELECT * (qualified and unqualified)
    for star in parsed.find_all(exp.Star):
        table = getattr(star, "table", None) or "_unqualified_"
        if table not in result:
            result[table] = SourceColumns(
                source_alias=table,
                columns=set(),
                has_star=True,
            )
        else:
            result[table].has_star = True

    return result


def get_table_aliases(
    sql: str,
    target_dialect: str = "spark",
) -> Dict[str, str]:
    """Extract table name to alias mappings from SQL.

    Args:
        sql: The SQL query to analyze
        target_dialect: SQL dialect of the query

    Returns:
        Dict mapping table names to their aliases
        e.g., {'orders': 'o', 'customers': 'c'}
    """
    if not SQLGLOT_AVAILABLE:
        return {}

    try:
        parsed = sqlglot.parse_one(sql, read=target_dialect)
    except Exception:
        return {}

    result: Dict[str, str] = {}

    # Find all table references
    for table in parsed.find_all(exp.Table):
        table_name = table.name
        alias = table.alias or table_name
        result[table_name] = alias

    return result


def prune_columns_for_extraction(
    required_columns: List[str],
    available_columns: List[str],
    pk_columns: Optional[List[str]] = None,
) -> List[str]:
    """Determine final column list for extraction.

    Ensures required columns exist in available columns,
    and always includes primary key columns for incremental detection.

    Args:
        required_columns: Columns used in the model
        available_columns: All columns available in the source
        pk_columns: Primary key columns (always included for incremental)

    Returns:
        Final list of columns to extract
    """
    # If required is empty (SELECT * was used), return all
    if not required_columns:
        return available_columns

    # Build set of columns to extract
    columns_to_extract = set(required_columns)

    # Always include primary key for incremental detection
    if pk_columns:
        columns_to_extract.update(pk_columns)

    # Filter to only columns that exist in source
    available_set = set(available_columns)
    result = [c for c in columns_to_extract if c in available_set]

    # Preserve original order from available_columns
    ordered_result = [c for c in available_columns if c in result]

    return ordered_result
