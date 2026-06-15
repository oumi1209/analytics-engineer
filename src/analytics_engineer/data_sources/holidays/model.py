from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass
class RefHoliday:
    """A French public holiday (datagouv reference), after Bronze cleaning."""

    date: date
    nom_jour_ferie: str
