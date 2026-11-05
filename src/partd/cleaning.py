from pyspark.sql import Column
from pyspark.sql import functions as F


def normalize_name(col_name: str) -> Column:
    """Trim, collapse repeated spaces, and uppercase, so joins match across files."""
    cleaned = F.upper(F.regexp_replace(F.trim(F.col(col_name)), r"\s+", " "))
    return F.when(cleaned == "", None).otherwise(cleaned)


def safe_cast(col_name: str, data_type: str) -> Column:
    """Return NULL instead of raising under ANSI mode."""
    return F.expr(f"try_cast(trim(`{col_name}`) as {data_type})")


def is_blank(col_name: str) -> Column:
    return F.col(col_name).isNull() | (F.trim(F.col(col_name)) == "")


def is_invalid_number(col_name: str, data_type: str) -> Column:
    """True for bad text like 'abc'. False for a blank, which means suppressed."""
    return ~is_blank(col_name) & safe_cast(col_name, data_type).isNull()


def reject_reasons(rules: dict[str, Column]) -> Column:
    """Build an array of reason codes for the rules that are true."""
    return F.array_compact(F.array(*[F.when(rule, code) for code, rule in rules.items()]))
