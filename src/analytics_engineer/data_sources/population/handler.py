from __future__ import annotations

import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType

from analytics_engineer.data_sources.population.model import PopulationCommune
from analytics_engineer.infra.data_source_handler import BaseDataSourceHandler
from analytics_engineer.infra.dataset import Dataset


class PopulationHandler(BaseDataSourceHandler):
    """Load the INSEE population CSV into ``bronze.population_communes``."""

    model = PopulationCommune
    table_name = "population_communes"

    def read(self) -> Dataset[PopulationCommune]:
        raw = self.spark.read.option("header", True).csv(
            self.config.landing("population")
        )
        return Dataset(raw, self.model)

    def clean(self, raw: Dataset[PopulationCommune]) -> Dataset[PopulationCommune]:
        cleaned = raw.df.withColumn("population", F.col("population").cast(IntegerType()))
        return Dataset(cleaned, self.model)
