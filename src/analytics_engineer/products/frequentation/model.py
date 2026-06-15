"""Typed models for the ``frequentation`` data product (Silver + Gold)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Optional


@dataclass
class FrequentationClean:
    """Validated footfall enriched with holiday flag and commune population (Silver)."""

    gare_id: int
    code_uic: str
    nom_gare: str
    ville: str
    region: str
    type_gare: str
    segment: str
    latitude: Optional[float]
    longitude: Optional[float]
    date: Optional[date]
    heure_tranche: int
    nb_voyageurs: Optional[int]
    nb_non_voyageurs: Optional[int]
    est_jour_ferie: bool
    code_insee: Optional[str]
    population: Optional[int]


@dataclass
class TrancheHoraire:
    """A three-hour time slot with its business labels (static Gold reference)."""

    heure_tranche: int
    libelle: str
    periode_journee: str
    type_tranche: str


TRANCHE_HORAIRE_REF = [
    TrancheHoraire(0, "00h-03h", "Nuit", "Nuit"),
    TrancheHoraire(3, "03h-06h", "Nuit", "Nuit"),
    TrancheHoraire(6, "06h-09h", "Matin", "Heure de pointe"),
    TrancheHoraire(9, "09h-12h", "Matinée", "Heures creuses"),
    TrancheHoraire(12, "12h-15h", "Après-midi", "Heures creuses"),
    TrancheHoraire(15, "15h-18h", "Fin d'après-midi", "Heures creuses"),
    TrancheHoraire(18, "18h-21h", "Soir", "Heure de pointe"),
    TrancheHoraire(21, "21h-24h", "Soirée tardive", "Nuit"),
]
