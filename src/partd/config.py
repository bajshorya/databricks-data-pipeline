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
    sample_state: str
    project_states: tuple[str, ...]
    years: tuple[int, ...]
    malformed_max_pct: float
    quarantine_max_pct: float
    unmatched_drug_max_pct: float
    min_prescribers_specialty: int

    def table(self, layer: str, name: str) -> str:
        return f"{self.catalog}.{getattr(self, layer)}.{name}"


def load_settings(config_dir: str | Path, catalog: str | None = None,
                  schema_prefix: str = "partd") -> Settings:
    """Read settings.toml. Tests pass catalog='spark_catalog' to use the local catalog."""
    with open(Path(config_dir) / "settings.toml", "rb") as f:
        raw = tomllib.load(f)
    cat = catalog or raw["catalog"]["name"]
    # Schema names come from the prefix so every environment uses only partd_* schemas.
    names = {k: f"{schema_prefix}_{k}" for k in ("bronze", "silver", "gold", "ops")}
    return Settings(
        catalog=cat,
        landing_root=f"/Volumes/{cat}/{names['ops']}/{raw['landing']['volume']}",
        sample_state=raw["data"]["sample_state"],
        project_states=tuple(raw["data"]["project_states"]),
        years=tuple(raw["data"]["years"]),
        malformed_max_pct=raw["thresholds"]["malformed_max_pct"],
        quarantine_max_pct=raw["thresholds"]["quarantine_max_pct"],
        unmatched_drug_max_pct=raw["thresholds"]["unmatched_drug_max_pct"],
        min_prescribers_specialty=raw["thresholds"]["min_prescribers_specialty"],
        **names,
    )


def load_sources(config_dir: str | Path) -> dict[str, dict[str, str]]:
    with open(Path(config_dir) / "sources.toml", "rb") as f:
        return tomllib.load(f)
