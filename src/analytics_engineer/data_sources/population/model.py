from __future__ import annotations

from dataclasses import dataclass


@dataclass
class PopulationCommune:
    """Commune population reference (latest INSEE figures)."""

    nom_commune: str
    code_insee: str
    population: int

