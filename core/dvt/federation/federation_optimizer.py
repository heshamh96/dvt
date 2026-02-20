"""
Federation Optimizer — decomposes Spark SQL into per-source extraction plans.

This is the single optimization point between query transpilation and source
extraction. Everything that reduces data fetched from source databases goes
through here:

- Column projection: only extract columns the query actually references
- Predicate pushdown: single-source WHERE conditions transpiled to source dialect
- LIMIT pushdown: for single-source queries only, transpiled to source syntax
- Full dialect transpilation: the complete extraction SQL is generated via sqlglot
  in each source's native dialect — column quoting, LIMIT/TOP/FETCH, predicate
  syntax are all handled by a single .sql(dialect=...) call

The optimizer works on the TRANSPILED Spark SQL (not the original model SQL)
to ensure dialect-neutral analysis. Output extraction plans are fully
transpiled to each source's native dialect.

Usage:
    from dvt.federation.federation_optimizer import FederationOptimizer, SourceInfo

    optimizer = FederationOptimizer()

    plans = optimizer.optimize(
        spark_sql="SELECT c.`Customer Code`, r.region_name FROM _dvt_abc_c c ...",
        source_info={
            "source.proj.customers": SourceInfo(
                source_id="source.proj.customers",
                view_name="_dvt_abc_c",
                schema="public",
                table="customers_db_1",
                columns=["Customer Code", "Customer name", "region_code", ...],
                dialect="postgres",
            ),
            ...
        },
    )

    # plans["source.proj.customers"].extraction_sql →
    #   SELECT "Customer Code", "region_code" FROM public.customers_db_1
    #   WHERE "Customer Channel type" = 'Modern Trade' LIMIT 25
"""

import logging
import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError

from dvt.federation.dialect_fallbacks import get_dialect_for_adapter

logger = logging.getLogger(__name__)


# =============================================================================
# Data Structures
# =============================================================================


@dataclass
class SourceInfo:
    """Source metadata needed by the optimizer.

    Populated by the engine from the manifest + pre-fetched column metadata.
    """

    source_id: str  # "source.Coke_DB.pg_source.customers_db_1"
    view_name: str  # "_dvt_abc12345_c" (temp view name in Spark SQL)
    schema: str  # "public" (for building extraction SQL FROM clause)
    table: str  # "customers_db_1"
    columns: List[str]  # ["Customer Code", "Customer name", ...] real DB names
    dialect: str  # "postgres" — the source database's adapter type


@dataclass
class ExtractionPlan:
    """Per-source extraction plan, fully transpiled to source dialect.

    Every field is in the source database's native dialect:
    - columns use source-dialect quoting ("PG", `DBX`, [MSSQL])
    - predicates use source-dialect syntax
    - extraction_sql is a complete, ready-to-execute query
    """

    source_id: str
    columns: Optional[List[str]] = None  # None = SELECT *, else specific column names
    predicates: List[str] = field(default_factory=list)  # In source dialect
    limit: Optional[int] = None  # Only set for single-source queries
    source_dialect: str = ""  # e.g., "postgres", "databricks"
    extraction_sql: str = ""  # Complete query in source dialect


# =============================================================================
# Federation Optimizer
# =============================================================================


class FederationOptimizer:
    """Blackbox: Spark SQL → per-source extraction plans.

    Takes the transpiled Spark SQL (with temp view names) and decomposes
    it into one ExtractionPlan per source, each fully transpiled to the
    source's native SQL dialect.

    The optimizer performs three optimizations:

    1. Column Projection — Only extract columns that the query references
       from each source. This includes columns in SELECT, JOIN ON, WHERE,
       GROUP BY, ORDER BY, HAVING, WINDOW PARTITION BY/ORDER BY, CASE WHEN,
       and function arguments. Everything that sqlglot's scope analysis finds.

    2. Predicate Pushdown — WHERE conditions that reference columns from only
       one source are extracted and transpiled to that source's dialect. Cross-
       source predicates (e.g., s.id = r.id in WHERE) are NOT pushed down.

    3. LIMIT Pushdown — A global LIMIT is pushed to the source only when
       there is a single source (no JOINs). For multi-source queries, LIMIT
       stays in Spark to avoid incorrect row reduction before the join.

    On failure (sqlglot parse error, qualify error, etc.), the optimizer
    gracefully degrades to columns=None + no predicates — the same behavior
    as today's extraction.
    """

    # Simple identifier pattern: letters, digits, underscores (no spaces/special chars)
    _SIMPLE_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

    @staticmethod
    def _needs_quoting(identifier: str) -> bool:
        """Check if a schema or table identifier needs quoting.

        Simple identifiers (letters, digits, underscores) do NOT need quoting.
        Databases like Snowflake and Oracle auto-uppercase unquoted identifiers,
        so quoting a lowercase name like "ods" forces case-sensitive matching
        and causes errors. Only quote when the name contains spaces, special
        characters, or starts with a digit.

        Column identifiers are NOT affected by this — they always use
        quoted=True because column names can contain spaces.
        """
        return not FederationOptimizer._SIMPLE_IDENTIFIER_RE.match(identifier)

    def optimize(
        self,
        spark_sql: str,
        source_info: Dict[str, SourceInfo],
    ) -> Dict[str, ExtractionPlan]:
        """Decompose Spark SQL into per-source extraction plans.

        Args:
            spark_sql: Fully transpiled Spark SQL (with temp view names)
            source_info: Dict mapping source_id to SourceInfo metadata

        Returns:
            Dict mapping source_id to ExtractionPlan
        """
        if not source_info:
            return {}

        # Build reverse lookup: view_name -> source_id
        view_to_source: Dict[str, str] = {}
        for source_id, info in source_info.items():
            view_to_source[info.view_name] = source_id
            # Also map lowercased for case-insensitive matching
            view_to_source[info.view_name.lower()] = source_id

        try:
            return self._optimize_with_sqlglot(spark_sql, source_info, view_to_source)
        except Exception as e:
            logger.warning(
                "FederationOptimizer: sqlglot analysis failed (%s). "
                "Falling back to SELECT * with no predicate pushdown.",
                str(e),
            )
            return self._fallback_plans(source_info)

    # =========================================================================
    # Core Optimization Logic
    # =========================================================================

    def _optimize_with_sqlglot(
        self,
        spark_sql: str,
        source_info: Dict[str, SourceInfo],
        view_to_source: Dict[str, str],
    ) -> Dict[str, ExtractionPlan]:
        """Run full sqlglot-based optimization.

        Steps:
            1. Parse Spark SQL
            2. Build scope for column resolution
            3. Detect if SELECT * needs expansion
            4. For each source: extract columns, predicates, limit
            5. Build fully-transpiled extraction SQL per source
        """
        from sqlglot.optimizer.scope import build_scope

        parsed = sqlglot.parse_one(spark_sql, read="spark")
        has_star = self._has_select_star(parsed)

        # If SELECT * is present, try qualify_columns to expand it
        if has_star:
            parsed = self._try_qualify_columns(parsed, source_info)

        scope = build_scope(parsed)
        if scope is None:
            logger.warning("FederationOptimizer: Could not build scope.")
            return self._fallback_plans(source_info)

        # Build alias -> source_id mapping from scope
        alias_to_source: Dict[str, str] = {}
        for alias in scope.sources:
            # scope.sources has alias -> Table node
            source_node = scope.sources[alias]
            table_name = (
                source_node.name if hasattr(source_node, "name") else str(alias)
            )

            # Match by view_name (the table name in Spark SQL IS the view name)
            source_id = view_to_source.get(
                table_name, view_to_source.get(table_name.lower())
            )
            if source_id is None:
                # Try matching by alias itself
                source_id = view_to_source.get(alias, view_to_source.get(alias.lower()))
            if source_id:
                alias_to_source[alias] = source_id

        # Determine if LIMIT can be pushed (single source only)
        is_single_source = len(source_info) == 1
        global_limit = self._extract_limit(parsed) if is_single_source else None

        # Extract predicates from WHERE clause
        predicate_map = self._extract_predicates(parsed, alias_to_source)

        # Build extraction plan per source
        plans: Dict[str, ExtractionPlan] = {}

        for source_id, info in source_info.items():
            # Find which alias(es) map to this source
            aliases = [a for a, sid in alias_to_source.items() if sid == source_id]

            # Extract columns from all aliases for this source
            columns = self._extract_columns_for_source(scope, aliases, info.columns)

            # Get predicates for this source (from all its aliases)
            source_predicates: List[exp.Expression] = []
            for alias in aliases:
                source_predicates.extend(predicate_map.get(alias, []))

            # Build the fully-transpiled extraction SQL
            plan = self._build_extraction_plan(
                info=info,
                columns=columns,
                predicate_asts=source_predicates,
                limit=global_limit,
            )
            plans[source_id] = plan

        return plans

    # =========================================================================
    # Column Extraction
    # =========================================================================

    def _extract_columns_for_source(
        self,
        scope: "Scope",  # noqa: F821
        aliases: List[str],
        real_columns: List[str],
    ) -> Optional[List[str]]:
        """Extract all columns referenced from a source.

        Uses sqlglot's scope.source_columns() which captures columns from
        ALL parts of the query: SELECT, JOIN ON, WHERE, GROUP BY, ORDER BY,
        HAVING, WINDOW PARTITION BY/ORDER BY, CASE WHEN, function args, etc.

        Args:
            scope: sqlglot Scope from build_scope()
            aliases: List of SQL aliases for this source (e.g., ["c", "s"])
            real_columns: Actual database column names for case resolution

        Returns:
            List of real column names (in DB casing), or None if columns
            could not be determined (fallback to SELECT *)
        """
        if not aliases:
            return None

        # Collect all column names from sqlglot scope
        sqlglot_columns: Set[str] = set()
        for alias in aliases:
            try:
                scope_cols = scope.source_columns(alias)
                for col in scope_cols:
                    sqlglot_columns.add(col.name)
            except Exception:
                # If source_columns fails, include all columns
                return None

        if not sqlglot_columns:
            return None

        # Resolve sqlglot column names (potentially lowercased or case-
        # sensitive depending on quoting) back to real database column names
        resolved = self._resolve_to_real_columns(sqlglot_columns, real_columns)

        if not resolved:
            return None  # No columns could be resolved — fallback to SELECT *

        return sorted(resolved)

    def _resolve_to_real_columns(
        self,
        sqlglot_names: Set[str],
        real_columns: List[str],
    ) -> List[str]:
        """Map sqlglot column names to real database column names.

        sqlglot may lowercase unquoted identifiers. Quoted identifiers
        preserve their original case. We match case-insensitively and
        return the real database column name.

        Args:
            sqlglot_names: Column names from sqlglot scope analysis
            real_columns: Actual column names from the database

        Returns:
            List of resolved real column names
        """
        # Build case-insensitive lookup: lowered -> real
        real_map: Dict[str, str] = {}
        for col in real_columns:
            real_map[col.lower()] = col

        resolved: List[str] = []
        for name in sqlglot_names:
            # Try exact match first
            if name in real_map.values():
                resolved.append(name)
            # Try case-insensitive match
            elif name.lower() in real_map:
                resolved.append(real_map[name.lower()])
            else:
                # Column from sqlglot not found in real columns.
                # This can happen with computed columns (e.g., COUNT(*)).
                # Ignore — computed columns don't need to be extracted.
                logger.debug(
                    "FederationOptimizer: Column %r not found in source columns, skipping.",
                    name,
                )

        return resolved

    # =========================================================================
    # Predicate Extraction
    # =========================================================================

    def _extract_predicates(
        self,
        parsed: exp.Expression,
        alias_to_source: Dict[str, str],
    ) -> Dict[str, List[exp.Expression]]:
        """Extract pushable WHERE predicates per source alias.

        A predicate is pushable to a source if ALL column references in
        the predicate belong to the same source alias. Cross-source
        predicates (e.g., s.region_code = r.region_code) are not pushed.

        Args:
            parsed: Parsed Spark SQL AST
            alias_to_source: Mapping of alias -> source_id

        Returns:
            Dict mapping alias -> list of predicate AST nodes
        """
        result: Dict[str, List[exp.Expression]] = {
            alias: [] for alias in alias_to_source
        }

        where = parsed.find(exp.Where)
        if not where:
            return result

        conditions = list(self._flatten_and(where.this))

        for condition in conditions:
            columns = list(condition.find_all(exp.Column))
            if not columns:
                continue

            # Check if ALL columns reference a single alias
            tables: Set[str] = set()
            for col in columns:
                if col.table:
                    tables.add(col.table)

            # Pushable only if all columns reference exactly one alias
            if len(tables) == 1:
                alias = tables.pop()
                if alias in result:
                    result[alias].append(condition)

        return result

    def _flatten_and(self, node: exp.Expression):
        """Recursively flatten AND conditions into individual predicates."""
        if isinstance(node, exp.And):
            yield from self._flatten_and(node.left)
            yield from self._flatten_and(node.right)
        else:
            yield node

    # =========================================================================
    # LIMIT Extraction
    # =========================================================================

    def _extract_limit(self, parsed: exp.Expression) -> Optional[int]:
        """Extract global LIMIT value from the query.

        Args:
            parsed: Parsed SQL AST

        Returns:
            Integer LIMIT value, or None if no LIMIT
        """
        limit_node = parsed.find(exp.Limit)
        if limit_node is None:
            return None

        try:
            limit_expr = limit_node.expression
            if isinstance(limit_expr, exp.Literal):
                return int(limit_expr.this)
        except (ValueError, AttributeError):
            pass

        return None

    # =========================================================================
    # Extraction Plan Builder
    # =========================================================================

    def _build_extraction_plan(
        self,
        info: SourceInfo,
        columns: Optional[List[str]],
        predicate_asts: List[exp.Expression],
        limit: Optional[int],
    ) -> ExtractionPlan:
        """Build a fully-transpiled extraction plan for a source.

        Constructs a sqlglot AST for the extraction query and serializes
        it in the source's native dialect. This single .sql(dialect=...)
        call handles ALL dialect differences:
        - Column quoting: "PG" vs `DBX` vs [MSSQL]
        - LIMIT syntax: LIMIT n vs TOP n vs FETCH FIRST n ROWS ONLY
        - Predicate syntax: CAST(x AS DATE) vs x::date etc.

        Args:
            info: Source metadata
            columns: Columns to extract (None = SELECT *)
            predicate_asts: Predicate AST nodes (in Spark SQL dialect)
            limit: LIMIT value (None = no limit)

        Returns:
            ExtractionPlan with fully-transpiled extraction_sql
        """
        source_sqlglot_dialect = get_dialect_for_adapter(info.dialect)

        # Build the extraction query AST
        extraction_ast = self._build_extraction_ast(
            schema=info.schema,
            table=info.table,
            columns=columns,
            predicate_asts=predicate_asts,
            limit=limit,
        )

        # Serialize in source dialect — this is where all transpilation happens
        try:
            extraction_sql = extraction_ast.sql(dialect=source_sqlglot_dialect)
        except Exception as e:
            logger.warning(
                "FederationOptimizer: Failed to generate extraction SQL for %s (%s): %s. "
                "Falling back to SELECT *.",
                info.source_id,
                info.dialect,
                str(e),
            )
            # Fallback: SELECT * FROM schema.table
            fallback = sqlglot.select("*").from_(f"{info.schema}.{info.table}")
            extraction_sql = fallback.sql(dialect=source_sqlglot_dialect)
            columns = None

        # Extract predicate strings in source dialect for SourceConfig
        predicate_strings: List[str] = []
        for pred_ast in predicate_asts:
            try:
                # Strip the source view alias from column references before
                # transpiling — the extraction query uses bare column names
                stripped = self._strip_table_prefix(pred_ast)
                pred_sql = stripped.sql(dialect=source_sqlglot_dialect)
                predicate_strings.append(pred_sql)
            except Exception:
                pass  # Skip predicates that can't be transpiled

        return ExtractionPlan(
            source_id=info.source_id,
            columns=columns,
            predicates=predicate_strings,
            limit=limit,
            source_dialect=info.dialect,
            extraction_sql=extraction_sql,
        )

    def _build_extraction_ast(
        self,
        schema: str,
        table: str,
        columns: Optional[List[str]],
        predicate_asts: List[exp.Expression],
        limit: Optional[int],
    ) -> exp.Select:
        """Build a sqlglot AST for the extraction query.

        Args:
            schema: Database schema
            table: Table name
            columns: Column names to SELECT (None = *)
            predicate_asts: WHERE predicate AST nodes
            limit: LIMIT value (None = no limit)

        Returns:
            sqlglot Select AST node
        """
        # SELECT clause
        if columns:
            col_exprs = [
                exp.Column(this=exp.Identifier(this=c, quoted=True)) for c in columns
            ]
            select = sqlglot.select(*col_exprs)
        else:
            select = sqlglot.select("*")

        # FROM clause — only quote schema/table if they contain spaces or special
        # characters. Simple names stay unquoted so databases like Snowflake and
        # Oracle can apply their default case folding (uppercase).
        select = select.from_(
            exp.Table(
                this=exp.Identifier(this=table, quoted=self._needs_quoting(table)),
                db=exp.Identifier(this=schema, quoted=self._needs_quoting(schema)),
            )
        )

        # WHERE clause — strip table aliases from predicate columns
        for pred_ast in predicate_asts:
            stripped = self._strip_table_prefix(pred_ast)
            select = select.where(stripped, copy=False)

        # LIMIT clause
        if limit is not None:
            select = select.limit(limit)

        return select

    def _strip_table_prefix(self, condition: exp.Expression) -> exp.Expression:
        """Strip table alias prefixes from column references in a predicate.

        Extraction queries use bare column names (no alias prefix) since
        they query a single table. e.g., `c."Customer Code"` → `"Customer Code"`

        Args:
            condition: Predicate AST node

        Returns:
            Copy of condition with table prefixes removed
        """
        cond_copy = condition.copy()
        for col in cond_copy.find_all(exp.Column):
            if col.table:
                col.set("table", None)
        return cond_copy

    # =========================================================================
    # SELECT * Detection and Expansion
    # =========================================================================

    def _has_select_star(self, parsed: exp.Expression) -> bool:
        """Check if the query contains SELECT * or SELECT alias.*"""
        for sel in parsed.find_all(exp.Star):
            return True
        return False

    def _try_qualify_columns(
        self,
        parsed: exp.Expression,
        source_info: Dict[str, SourceInfo],
    ) -> exp.Expression:
        """Try to expand SELECT * using qualify_columns.

        If the schema is available (source column names), attempt to use
        sqlglot's qualify_columns to expand * into explicit column references.
        On failure, return the original AST unchanged.

        Args:
            parsed: Parsed Spark SQL AST
            source_info: Source metadata with column names

        Returns:
            Qualified AST (with * expanded) or original AST on failure
        """
        try:
            from sqlglot.optimizer.qualify_columns import qualify_columns
            from sqlglot.schema import MappingSchema

            # Build schema dict: view_name -> {col_name: type}
            schema_dict: Dict[str, Dict[str, str]] = {}
            for info in source_info.values():
                schema_dict[info.view_name] = {col: "VARCHAR" for col in info.columns}

            ms = MappingSchema(
                schema=schema_dict,
                dialect="spark",
                normalize=False,
            )

            qualified = qualify_columns(parsed.copy(), schema=ms, dialect="spark")
            return qualified

        except Exception as e:
            logger.debug(
                "FederationOptimizer: qualify_columns failed (%s). "
                "Proceeding without star expansion.",
                str(e),
            )
            return parsed

    # =========================================================================
    # Fallback
    # =========================================================================

    def _fallback_plans(
        self,
        source_info: Dict[str, SourceInfo],
    ) -> Dict[str, ExtractionPlan]:
        """Generate fallback plans: SELECT * with no predicates.

        This is the safe degradation — equivalent to today's behavior
        where columns=None and predicates are not transpiled.

        Args:
            source_info: Source metadata

        Returns:
            Dict of ExtractionPlans with columns=None and no predicates
        """
        plans: Dict[str, ExtractionPlan] = {}

        for source_id, info in source_info.items():
            source_sqlglot_dialect = get_dialect_for_adapter(info.dialect)

            fallback_ast = sqlglot.select("*").from_(
                exp.Table(
                    this=exp.Identifier(
                        this=info.table, quoted=self._needs_quoting(info.table)
                    ),
                    db=exp.Identifier(
                        this=info.schema, quoted=self._needs_quoting(info.schema)
                    ),
                )
            )

            plans[source_id] = ExtractionPlan(
                source_id=source_id,
                columns=None,
                predicates=[],
                limit=None,
                source_dialect=info.dialect,
                extraction_sql=fallback_ast.sql(dialect=source_sqlglot_dialect),
            )

        return plans
