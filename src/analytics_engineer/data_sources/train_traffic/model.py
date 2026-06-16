from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Optional


@dataclass
class Frequentation:
    """One station footfall record after Bronze technical cleaning.

    Measures and coordinates stay ``Optional`` because Bronze preserves raw nulls
    (validity rules are applied in Silver, not here).
    """

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
