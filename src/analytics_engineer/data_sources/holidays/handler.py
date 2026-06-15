from __future__ import annotations

import pyspark.sql.functions as F

from analytics_engineer.data_sources.holidays.model import RefHoliday
from analytics_engineer.infra.data_source_handler import BaseDataSourceHandler
from analytics_engineer.infra.dataset import Dataset


class HolidayHandler(BaseDataSourceHandler):
    """Ingest the public-holidays CSV into ``bronze.jours_feries``."""

    model = RefHoliday
    table_name = "jours_feries"

    def read(self) -> Dataset[RefHoliday]:
        raw = self.spark.read.option("header", True).csv(
            self.config.landing("jours_feries")
        )
        return Dataset(raw, self.model)

    def clean(self, raw: Dataset[RefHoliday]) -> Dataset[RefHoliday]:
        cleaned = raw.df.select(
            F.to_date("date").alias("date"),
            F.col("nom_jour_ferie"),
        ).dropDuplicates(["date"])
        return Dataset(cleaned, self.model)
