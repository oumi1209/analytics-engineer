"""The ``frequentation`` data product: footfall joined from train traffic,
holidays and population, modeled as a star schema.

* ``silver`` — validate + enrich (the cross-source joins)
* ``gold`` — the star schema (dim_gare, dim_date, dim_tranche_horaire, fait)
"""

from analytics_engineer.products.frequentation.gold import run_gold
from analytics_engineer.products.frequentation.silver import run_silver

__all__ = ["run_silver", "run_gold"]
