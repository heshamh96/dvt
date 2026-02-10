"""
Query optimizer for DVT federation.

Extracts pushable operations from compiled model SQL using SQLGlot:
- WHERE predicates (per source table)
- LIMIT/TOP/FETCH
- TABLESAMPLE/SAMPLE
- Column projection (SELECT only needed columns)

SQLGlot is a HARD REQUIREMENT - this module will raise ImportError if not available.

Usage:
    from dvt.federation.query_optimizer import QueryOptimizer, PushableOperations

    optimizer = QueryOptimizer()

    # Extract operations for all sources in a query
    ops = optimizer.extract_all_pushable_operations(
        compiled_sql="SELECT o.id, c.name FROM orders o JOIN customers c ON ...",
        source_mappings={"source.proj.orders": "o", "source.proj.customers": "c"},
        source_dialect="postgres",
    )

    # Build extraction query with pushed operations
    query = optimizer.build_extraction_query(
        schema="public",
        table="orders",
        operations=ops["source.proj.orders"],
        target_dialect="postgres",
    )
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple

# SQLGlot is a HARD REQUIREMENT
import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError

from dvt.federation.dialect_fallbacks import (
    build_extraction_query_fallback,
    get_dialect_for_adapter,
    get_limit_clause,
    get_sample_clause,
    supports_sample,
)


@dataclass
class PushableOperations:
    """Operations that can be pushed down to source extraction.

    This represents all optimizations that can be applied when extracting
    data from a source table for federation:

    - predicates: WHERE conditions that filter only this source
    - columns: Columns used from this source (for projection pushdown)
    - limit: LIMIT/TOP/FETCH value (if query has global limit)
    - sample_percent: TABLESAMPLE percentage
    - sample_rows: TABLESAMPLE row count
    - order_by: ORDER BY columns (for deterministic sampling)
    """

    source_id: str  # e.g., "source.project.schema.table"
    source_alias: str  # Alias in query (e.g., "o" for orders)
    predicates: List[str] = field(default_factory=list)
    columns: List[str] = field(default_factory=list)
    limit: Optional[int] = None
    sample_percent: Optional[float] = None
    sample_rows: Optional[int] = None
    order_by: List[str] = field(default_factory=list)

    def has_optimizations(self) -> bool:
        """Check if any optimizations are available."""
        return bool(
            self.predicates
            or self.columns
            or self.limit
            or self.sample_percent
            or self.sample_rows
        )


class QueryOptimizer:
    """Extracts pushable operations from compiled SQL using SQLGlot.

    This optimizer parses the model's compiled SQL (after Jinja resolution)
    and identifies which operations can be pushed down to source extraction
    for better performance.

    Pushable operations:
    1. WHERE predicates: Conditions that reference only one source table
    2. Column projection: Only extract columns that are actually used
    3. LIMIT: If query has a global LIMIT, push it to sources
    4. SAMPLE: If query uses TABLESAMPLE, push it to sources
    """

    def __init__(self):
        """Initialize the query optimizer."""
        pass

    def extract_all_pushable_operations(
        self,
        compiled_sql: str,
        source_mappings: Dict[str, str],
        source_dialect: str,
    ) -> Dict[str, PushableOperations]:
        """Extract pushable operations for all sources in the query.

        Args:
            compiled_sql: Compiled SQL (Jinja resolved) to analyze
            source_mappings: Dict mapping source_id to alias in SQL
                            e.g., {"source.proj.orders": "o"}
            source_dialect: SQL dialect of the model (e.g., "postgres")

        Returns:
            Dict mapping source_id to PushableOperations

        Example:
            >>> optimizer.extract_all_pushable_operations(
            ...     compiled_sql="SELECT o.id FROM orders o WHERE o.date > '2024-01-01' LIMIT 100",
            ...     source_mappings={"source.proj.orders": "o"},
            ...     source_dialect="postgres",
            ... )
            {"source.proj.orders": PushableOperations(
                source_id="source.proj.orders",
                source_alias="o",
                predicates=["date > '2024-01-01'"],
                columns=["id"],
                limit=100,
            )}
        """
        # Parse SQL with SQLGlot
        sqlglot_dialect = get_dialect_for_adapter(source_dialect)

        try:
            parsed = sqlglot.parse_one(compiled_sql, read=sqlglot_dialect)
        except ParseError as e:
            # If parsing fails, return empty operations for all sources
            return {
                source_id: PushableOperations(source_id=source_id, source_alias=alias)
                for source_id, alias in source_mappings.items()
            }

        result = {}

        for source_id, alias in source_mappings.items():
            ops = PushableOperations(source_id=source_id, source_alias=alias)

            # Extract predicates for this source
            ops.predicates = self._extract_predicates(parsed, alias)

            # Extract columns used from this source
            ops.columns = self._extract_columns(parsed, alias)

            # Extract global LIMIT (applies to all sources)
            ops.limit = self._extract_limit(parsed)

            # Extract SAMPLE for this specific table
            sample = self._extract_sample(parsed, alias)
            if sample:
                ops.sample_percent = sample.get("percent")
                ops.sample_rows = sample.get("rows")

            # Extract ORDER BY (for deterministic sampling)
            ops.order_by = self._extract_order_by(parsed, alias)

            result[source_id] = ops

        return result

    def _extract_predicates(
        self,
        parsed: exp.Expression,
        alias: str,
    ) -> List[str]:
        """Extract WHERE predicates that apply only to a specific source.

        A predicate is pushable if ALL column references in it belong to
        the specified table alias.

        Args:
            parsed: Parsed SQL expression
            alias: Table alias to extract predicates for

        Returns:
            List of predicate SQL strings (without table prefix)
        """
        predicates = []
        where_clause = parsed.find(exp.Where)

        if not where_clause:
            return []

        # Get all conditions from WHERE clause
        conditions = self._flatten_conditions(where_clause.this)

        for condition in conditions:
            # Check if all columns in this condition reference our alias
            columns = list(condition.find_all(exp.Column))

            if not columns:
                continue

            all_match = True
            for col in columns:
                table = col.table
                # Column belongs to this source if:
                # 1. table prefix matches alias, OR
                # 2. no table prefix (ambiguous, but likely this source)
                if table and table.lower() != alias.lower():
                    all_match = False
                    break

            if all_match:
                # Strip table prefix and get SQL
                pred_sql = self._strip_table_prefix(condition, alias)
                predicates.append(pred_sql)

        return predicates

    def _extract_columns(
        self,
        parsed: exp.Expression,
        alias: str,
    ) -> List[str]:
        """Extract columns used from a specific source table.

        This enables projection pushdown - only extract columns that
        are actually used in the query.

        Args:
            parsed: Parsed SQL expression
            alias: Table alias to extract columns for

        Returns:
            List of column names (without table prefix)
        """
        columns: Set[str] = set()

        # Find all column references
        for col in parsed.find_all(exp.Column):
            table = col.table

            # Column belongs to this source if table matches alias
            if table and table.lower() == alias.lower():
                columns.add(col.name)
            elif not table:
                # Ambiguous - could be any table
                # Don't include to avoid incorrect projection
                pass

        return sorted(list(columns))

    def _extract_limit(self, parsed: exp.Expression) -> Optional[int]:
        """Extract LIMIT value from query.

        Handles:
        - LIMIT n
        - TOP n (SQL Server)
        - FETCH FIRST n ROWS ONLY (Oracle/DB2)

        Args:
            parsed: Parsed SQL expression

        Returns:
            Limit value or None if not present
        """
        # Try LIMIT
        limit_exp = parsed.find(exp.Limit)
        if limit_exp and limit_exp.expression:
            try:
                return int(limit_exp.expression.this)
            except (ValueError, AttributeError, TypeError):
                pass

        # Try FETCH (Oracle/DB2)
        fetch_exp = parsed.find(exp.Fetch)
        if fetch_exp and fetch_exp.count:
            try:
                return int(fetch_exp.count.this)
            except (ValueError, AttributeError, TypeError):
                pass

        return None

    def _extract_sample(
        self,
        parsed: exp.Expression,
        alias: str,
    ) -> Optional[Dict[str, float]]:
        """Extract TABLESAMPLE for a specific table.

        Args:
            parsed: Parsed SQL expression
            alias: Table alias to check for sampling

        Returns:
            Dict with 'percent' and/or 'rows' keys, or None
        """
        # Find all table samples
        for table in parsed.find_all(exp.Table):
            # Check if this table matches our alias
            table_alias = table.alias
            if table_alias and table_alias.lower() != alias.lower():
                continue

            # Look for TABLESAMPLE
            sample = table.find(exp.TableSample)
            if sample:
                result = {}

                # Try to extract percentage
                if sample.this:
                    try:
                        # Could be percent or rows depending on syntax
                        value = float(sample.this.this)
                        if sample.args.get("percent"):
                            result["percent"] = value
                        else:
                            result["rows"] = int(value)
                    except (ValueError, AttributeError, TypeError):
                        pass

                if result:
                    return result

        return None

    def _extract_order_by(
        self,
        parsed: exp.Expression,
        alias: str,
    ) -> List[str]:
        """Extract ORDER BY columns for a source.

        Used for deterministic sampling when TABLESAMPLE is not supported.

        Args:
            parsed: Parsed SQL expression
            alias: Table alias to extract ORDER BY for

        Returns:
            List of ORDER BY expressions
        """
        order_by_cols = []
        order = parsed.find(exp.Order)

        if not order:
            return []

        for ordered in order.expressions:
            if isinstance(ordered, exp.Ordered):
                expr = ordered.this
                if isinstance(expr, exp.Column):
                    table = expr.table
                    if table and table.lower() == alias.lower():
                        order_by_cols.append(expr.name)

        return order_by_cols

    def _flatten_conditions(self, expr: exp.Expression) -> List[exp.Expression]:
        """Flatten AND conditions into a list.

        Args:
            expr: Expression to flatten

        Returns:
            List of individual conditions
        """
        if isinstance(expr, exp.And):
            left = self._flatten_conditions(expr.left)
            right = self._flatten_conditions(expr.right)
            return left + right
        else:
            return [expr]

    def _strip_table_prefix(
        self,
        condition: exp.Expression,
        alias: str,
    ) -> str:
        """Generate SQL for condition without table prefix.

        Example: o.date > '2024-01-01' -> date > '2024-01-01'

        Args:
            condition: Condition expression
            alias: Table alias to strip

        Returns:
            SQL string without table prefix
        """
        # Deep copy to avoid modifying original
        cond_copy = condition.copy()

        # Remove table prefix from columns
        for col in cond_copy.find_all(exp.Column):
            if col.table and col.table.lower() == alias.lower():
                col.set("table", None)

        return cond_copy.sql()

    def build_extraction_query(
        self,
        schema: str,
        table: str,
        operations: PushableOperations,
        target_dialect: str,
    ) -> str:
        """Build extraction query with all pushed operations.

        Uses SQLGlot to generate the query, with fallback to
        dialect_fallbacks for edge cases.

        Args:
            schema: Database schema
            table: Table name
            operations: Pushable operations to apply
            target_dialect: Target database dialect

        Returns:
            SQL query string with all optimizations applied

        Example:
            >>> optimizer.build_extraction_query(
            ...     schema="public",
            ...     table="orders",
            ...     operations=PushableOperations(
            ...         source_id="...",
            ...         source_alias="o",
            ...         predicates=["date > '2024-01-01'"],
            ...         columns=["id", "date", "amount"],
            ...         limit=1000,
            ...     ),
            ...     target_dialect="postgres",
            ... )
            "SELECT id, date, amount FROM public.orders WHERE date > '2024-01-01' LIMIT 1000"
        """
        sqlglot_dialect = get_dialect_for_adapter(target_dialect)

        try:
            return self._build_query_with_sqlglot(
                schema=schema,
                table=table,
                operations=operations,
                dialect=sqlglot_dialect,
            )
        except Exception:
            # Fallback to dialect-specific builder
            return build_extraction_query_fallback(
                dialect=target_dialect,
                schema=schema,
                table=table,
                columns=operations.columns or None,
                predicates=operations.predicates or None,
                limit=operations.limit,
                sample_percent=operations.sample_percent,
                order_by=operations.order_by or None,
            )

    def _build_query_with_sqlglot(
        self,
        schema: str,
        table: str,
        operations: PushableOperations,
        dialect: str,
    ) -> str:
        """Build extraction query using SQLGlot.

        Args:
            schema: Database schema
            table: Table name
            operations: Pushable operations
            dialect: SQLGlot dialect

        Returns:
            SQL query string
        """
        # Build SELECT clause
        if operations.columns:
            select = sqlglot.select(*operations.columns)
        else:
            select = sqlglot.select("*")

        # Build FROM clause
        full_table = f"{schema}.{table}"
        select = select.from_(full_table)

        # Add TABLESAMPLE if supported and requested
        if operations.sample_percent and supports_sample(dialect):
            sample_clause = get_sample_clause(dialect, operations.sample_percent)
            if sample_clause:
                # SQLGlot doesn't have great TABLESAMPLE support for all dialects
                # We'll handle this in the final SQL generation
                pass

        # Add WHERE predicates
        for predicate in operations.predicates:
            try:
                pred_expr = sqlglot.parse_one(predicate, read=dialect)
                select = select.where(pred_expr)
            except ParseError:
                # If predicate parsing fails, add as raw SQL
                select = select.where(sqlglot.exp.Literal.string(predicate))

        # Add ORDER BY if needed for sampling fallback
        if operations.order_by:
            for col in operations.order_by:
                select = select.order_by(col)

        # Add LIMIT
        if operations.limit:
            select = select.limit(operations.limit)

        # Generate SQL
        sql = select.sql(dialect=dialect)

        # Handle TABLESAMPLE post-generation if needed
        if operations.sample_percent and supports_sample(dialect):
            sample_clause = get_sample_clause(dialect, operations.sample_percent)
            if sample_clause:
                # Insert TABLESAMPLE after table name
                sql = sql.replace(
                    f"FROM {full_table}",
                    f"FROM {full_table} {sample_clause}",
                )

        return sql


def extract_source_predicates(
    sql: str,
    source_alias: str,
    target_dialect: str = "spark",
) -> List[str]:
    """Convenience function to extract predicates for a single source.

    This is a simpler interface for the existing predicate_pushdown.py usage.

    Args:
        sql: SQL to analyze
        source_alias: Table alias to extract predicates for
        target_dialect: SQL dialect

    Returns:
        List of predicate SQL strings
    """
    optimizer = QueryOptimizer()
    ops = optimizer.extract_all_pushable_operations(
        compiled_sql=sql,
        source_mappings={f"source._.{source_alias}": source_alias},
        source_dialect=target_dialect,
    )
    return ops.get(f"source._.{source_alias}", PushableOperations("", "")).predicates


def transpile_predicates(
    predicates: List[str],
    from_dialect: str,
    to_dialect: str,
) -> List[str]:
    """Transpile predicates from one SQL dialect to another.

    Args:
        predicates: List of predicate SQL strings
        from_dialect: Source dialect (e.g., 'postgres')
        to_dialect: Target dialect (e.g., 'spark')

    Returns:
        List of transpiled predicate strings
    """
    from_sqlglot = get_dialect_for_adapter(from_dialect)
    to_sqlglot = get_dialect_for_adapter(to_dialect)

    result = []
    for pred in predicates:
        try:
            # Wrap in SELECT WHERE to parse as valid SQL
            wrapper = f"SELECT * FROM t WHERE {pred}"
            parsed = sqlglot.parse_one(wrapper, read=from_sqlglot)
            transpiled = parsed.sql(dialect=to_sqlglot)

            # Extract just the WHERE part
            where_match = transpiled.upper().find("WHERE")
            if where_match >= 0:
                result.append(transpiled[where_match + 6 :].strip())
            else:
                result.append(pred)
        except Exception:
            result.append(pred)

    return result
