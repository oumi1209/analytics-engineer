from __future__ import annotations

from analytics_engineer.data_sources.population.model import (
    POPULATION_COMMUNES,
    PopulationCommune,
)
from analytics_engineer.infra.data_source_handler import BaseDataSourceHandler
from analytics_engineer.infra.dataset import Dataset


class PopulationHandler(BaseDataSourceHandler):
    """Load the INSEE population seed into ``bronze.population_communes``.

    The seed is built straight from typed dataclasses via ``Dataset.create``, so
    the table schema is the one derived from :class:`PopulationCommune`.
    """

    model = PopulationCommune
    table_name = "population_communes"

    def read(self) -> Dataset[PopulationCommune]:
        return Dataset.create(self.spark, self.model, POPULATION_COMMUNES)

    def clean(self, raw: Dataset[PopulationCommune]) -> Dataset[PopulationCommune]:
        return raw  # seed reference data is already typed and clean
