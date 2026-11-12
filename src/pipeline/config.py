import re
import tomllib
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    catalog: str
    bronze: str
    silver: str
    gold: str
    ops: str
    landing_root: str
    invalid_row_max_pct: float

    def table(self, layer: str, name: str) -> str:
        return f"{self.catalog}.{getattr(self, layer)}.{name}"


def load_settings(config_dir: str | Path, catalog: str | None = None,
                  schema_prefix: str = "pipeline") -> Settings:
    """Read settings.toml. Tests pass catalog='spark_catalog' to use the local catalog."""
    with open(Path(config_dir) / "settings.toml", "rb") as f:
        raw = tomllib.load(f)
    cat = catalog or raw["catalog"]["name"]
    names = {k: f"{schema_prefix}_{k}" for k in ("bronze", "silver", "gold", "ops")}
    return Settings(
        catalog=cat,
        landing_root=f"/Volumes/{cat}/{names['ops']}/{raw['landing']['volume']}",
        invalid_row_max_pct=raw["thresholds"]["invalid_row_max_pct"],
        **names,
    )


def check_month(month: str) -> str:
    """The month goes into SQL filters, so only YYYY-MM is accepted."""
    if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", month):
        raise ValueError(f"month must be YYYY-MM, got {month!r}")
    return month
