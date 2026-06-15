from __future__ import annotations

from dataclasses import dataclass


@dataclass
class PopulationCommune:
    """Commune population reference (latest INSEE figures)."""

    nom_commune: str
    code_insee: str
    population: int


# Seed reference data: latest INSEE population per commune.
POPULATION_COMMUNES = [
    PopulationCommune("Paris", "75056", 2133111),
    PopulationCommune("Marseille", "13055", 873076),
    PopulationCommune("Lyon", "69123", 522969),
    PopulationCommune("Toulouse", "31555", 504078),
    PopulationCommune("Bordeaux", "33063", 261804),
    PopulationCommune("Lille", "59350", 236710),
    PopulationCommune("Strasbourg", "67482", 290576),
    PopulationCommune("Nantes", "44109", 320732),
    PopulationCommune("Rennes", "35238", 221272),
    PopulationCommune("Nice", "06088", 348085),
    PopulationCommune("Montpellier", "34172", 299096),
    PopulationCommune("Le Havre", "76351", 165830),
    PopulationCommune("Dijon", "21231", 159346),
    PopulationCommune("Reims", "51454", 181194),
    PopulationCommune("Tours", "37261", 137658),
    PopulationCommune("Grenoble", "38185", 156389),
    PopulationCommune("Angers", "49007", 155850),
]
