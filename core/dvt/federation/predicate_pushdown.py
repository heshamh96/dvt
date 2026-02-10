"""
Predicate pushdown for EL layer.

Extracts WHERE predicates from model SQL and pushes them down
to source extraction queries for efficiency.

Uses SQLGlot for SQL parsing and manipulation.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple

try:
    import sqlglot
    from sqlglot import exp

    SQLGLOT_AVAILABLE = True
except ImportError:
    SQLGLOT_AVAILABLE = False


@dataclass
class SourcePredicate:
    """A predicate that applies to a specific source."""

    source_alias: str  # Table alias in query (e.g., 'o' for orders)
    predicate_sql: str  # SQL fragment (e.g., "date > '2024-01-01'")
    columns_used: List[str]  # Columns referenced in predicate


def extract_source_predicates(
    sql: str,
    source_alias: str,
    target_dialect: str = "spark",
) -> List[SourcePredicate]:
    """Extract WHERE predicates that apply to a specific source table.

    Args:
        sql: The model SQL to analyze
        source_alias: Table alias to extract predicates for (e.g., 'o' for orders)
        target_dialect: SQL dialect for output

    Returns:
        List of predicates that apply only to the specified source

    Example:
        SQL: SELECT * FROM orders o JOIN customers c ON o.cust_id = c.id
             WHERE o.date > '2024-01-01' AND c.country = 'US'

        For source 'orders' (alias 'o'):
        Returns: [SourcePredicate(source_alias='o', predicate_sql="date > '2024-01-01'", ...)]
    """
    if not SQLGLOT_AVAILABLE:
        return []

    try:
        parsed = sqlglot.parse_one(sql, read=target_dialect)
    except Exception:
        return []

    predicates: List[SourcePredicate] = []
    where = parsed.find(exp.Where)

    if not where:
        return []

    # Find all conditions in WHERE clause
    for condition in _get_conditions(where):
        # Check if this condition only references our source alias
        columns = list(condition.find_all(exp.Column))
        if not columns:
            continue

        # Check if all columns reference only this source
        all_match = True
        columns_used = []
        for col in columns:
            table = col.table
            if table and table != source_alias:
                all_match = False
                break
            columns_used.append(col.name)

        if all_match and columns_used:
            # This predicate applies only to our source
            # Generate SQL without the table prefix for extraction
            pred_sql = _strip_table_prefix(condition, source_alias)
            predicates.append(
                SourcePredicate(
                    source_alias=source_alias,
                    predicate_sql=pred_sql,
                    columns_used=columns_used,
                )
            )

    return predicates


def _get_conditions(where_clause: exp.Where) -> List[exp.Expression]:
    """Extract individual conditions from WHERE clause.

    Handles AND chains, returning each condition separately.
    """
    conditions = []
    expr = where_clause.this

    if isinstance(expr, exp.And):
        # Flatten AND chain
        conditions.extend(_flatten_and(expr))
    else:
        conditions.append(expr)

    return conditions


def _flatten_and(and_expr: exp.And) -> List[exp.Expression]:
    """Flatten nested AND expressions into a list."""
    conditions = []

    left = and_expr.left
    right = and_expr.right

    if isinstance(left, exp.And):
        conditions.extend(_flatten_and(left))
    else:
        conditions.append(left)

    if isinstance(right, exp.And):
        conditions.extend(_flatten_and(right))
    else:
        conditions.append(right)

    return conditions


def _strip_table_prefix(
    condition: exp.Expression,
    source_alias: str,
) -> str:
    """Generate SQL for condition without table prefix.

    Example: o.date > '2024-01-01' -> date > '2024-01-01'
    """
    # Deep copy to avoid modifying original
    cond_copy = condition.copy()

    # Remove table prefix from columns
    for col in cond_copy.find_all(exp.Column):
        if col.table == source_alias:
            col.set("table", None)

    return cond_copy.sql()


def transpile_predicates(
    predicates: List[str],
    from_dialect: str,
    to_dialect: str,
) -> List[str]:
    """Transpile predicates from one SQL dialect to another.

    Args:
        predicates: List of predicate SQL strings
        from_dialect: Source dialect (e.g., 'spark')
        to_dialect: Target dialect (e.g., 'postgres')

    Returns:
        List of transpiled predicate strings
    """
    if not SQLGLOT_AVAILABLE:
        return predicates

    result = []
    for pred in predicates:
        try:
            # Wrap in SELECT WHERE to parse as valid SQL
            wrapper = f"SELECT * FROM t WHERE {pred}"
            parsed = sqlglot.parse_one(wrapper, read=from_dialect)
            transpiled = parsed.sql(dialect=to_dialect)
            # Extract just the WHERE part
            where_match = transpiled.upper().find("WHERE")
            if where_match >= 0:
                result.append(transpiled[where_match + 6 :].strip())
            else:
                result.append(pred)
        except Exception:
            result.append(pred)

    return result


def build_export_query_with_predicates(
    schema: str,
    table: str,
    columns: Optional[List[str]],
    predicates: List[str],
    source_dialect: str,
) -> str:
    """Build an export query with pushed predicates.

    Args:
        schema: Database schema
        table: Table name
        columns: Columns to select (None = all)
        predicates: WHERE predicates to include
        source_dialect: Target SQL dialect for output

    Returns:
        SQL query string
    """
    if not SQLGLOT_AVAILABLE:
        # Fallback without sqlglot
        col_str = ", ".join(columns) if columns else "*"
        query = f"SELECT {col_str} FROM {schema}.{table}"
        if predicates:
            query += f" WHERE {' AND '.join(predicates)}"
        return query

    # Build using sqlglot for proper dialect handling
    select = sqlglot.select(*(columns or ["*"])).from_(f"{schema}.{table}")

    for pred in predicates:
        try:
            pred_expr = sqlglot.parse_one(pred)
            select = select.where(pred_expr)
        except Exception:
            # If parsing fails, add as raw string
            select = select.where(sqlglot.exp.Column(this=pred))

    return select.sql(dialect=source_dialect)


def get_pushable_predicates_for_sources(
    model_sql: str,
    source_aliases: Dict[str, str],
    target_dialect: str = "spark",
) -> Dict[str, List[str]]:
    """Extract pushable predicates for multiple sources from model SQL.

    Args:
        model_sql: The model SQL to analyze
        source_aliases: Dict mapping source names to their aliases in the SQL
                       e.g., {'postgres__orders': 'o', 'mysql__customers': 'c'}
        target_dialect: SQL dialect of the model

    Returns:
        Dict mapping source names to list of pushable predicate SQL strings
        e.g., {'postgres__orders': ["date > '2024-01-01'", "status = 'active'"]}
    """
    result: Dict[str, List[str]] = {}

    for source_name, alias in source_aliases.items():
        predicates = extract_source_predicates(model_sql, alias, target_dialect)
        if predicates:
            result[source_name] = [p.predicate_sql for p in predicates]
        else:
            result[source_name] = []

    return result
