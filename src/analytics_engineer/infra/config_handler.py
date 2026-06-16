"""Load the pipeline configuration from ``config.yaml``.

Centralizing the catalog and the landing paths keeps the fully-qualified table
names consistent everywhere: ``config.table("silver", "frequentation_clean")``
always yields ``sncf_gc.silver.frequentation_clean`` — no hand-written prefixes to
drift out of sync.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml

# src/analytics_engineer/infra/config_handler.py -> project root holds config.yaml
DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[3] / "config.yaml"


@dataclass(frozen=True)
class Config:
    app_name: str
    catalog: str
    landing_paths: dict[str, str]

    def table(self, schema: str, name: str) -> str:
        """Fully-qualified table name, e.g. ``sncf_gc.bronze.frequentation``."""
        return f"{self.catalog}.{schema}.{name}"

    def landing(self, name: str) -> str:
        try:
            return self.landing_paths[name]
        except KeyError:
            raise KeyError(
                f"no landing path '{name}' in config; known: "
                f"{sorted(self.landing_paths)}"
            ) from None


def load_config(path: str | Path = DEFAULT_CONFIG_PATH) -> Config:
    raw = yaml.safe_load(Path(path).read_text()) or {}
    return Config(
        app_name=raw.get("app_name", "analytics-engineer"),
        catalog=raw.get("catalog", "sncf_gc"),
        landing_paths=raw.get("landing", {}),
    )


_config: Config | None = None


def get_config() -> Config:
    """Process-wide config singleton. Call set_config() first when a custom path is needed."""
    global _config
    if _config is None:
        _config = load_config()
    return _config


def set_config(config: Config) -> None:
    """Seed the process-wide singleton (called from main before stage functions run)."""
    global _config
    _config = config
