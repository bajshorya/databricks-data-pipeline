from pyspark.sql import Column
from pyspark.sql import functions as F


def safe_cast(col_name: str, data_type: str) -> Column:
    """Return NULL instead of failing the job when a value doesn't fit the type (ANSI mode is on)."""
    return F.expr(f"try_cast(trim(`{col_name}`) as {data_type})")


def is_blank(col_name: str) -> Column:
    return F.col(col_name).isNull() | (F.trim(F.col(col_name)) == "")


def is_bad_type(col_name: str, data_type: str) -> Column:
    """True when there is a value but it can't be converted, like 'abc' for a number."""
    return ~is_blank(col_name) & safe_cast(col_name, data_type).isNull()


def reject_reasons(rules: dict[str, Column]) -> Column:
    """Array of the reason codes whose rule is true. Empty array means the row is valid."""
    return F.array_compact(F.array(*[F.when(rule, code) for code, rule in rules.items()]))
