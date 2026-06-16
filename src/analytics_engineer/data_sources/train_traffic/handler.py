from __future__ import annotations

import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType, IntegerType

from analytics_engineer.data_sources.train_traffic.model import Frequentation
from analytics_engineer.infra.data_source_handler import BaseDataSourceHandler
from analytics_engineer.infra.dataset import Dataset


class TrainTrafficHandler(BaseDataSourceHandler):
    """Ingest the SNCF station footfall CSV into ``bronze.frequentation``."""

    model = Frequentation
    table_name = "frequentation"

    def read(self) -> Dataset[Frequentation]:
        raw = self.spark.read.option("header", True).csv(
            self.config.landing("frequentation")
        )
        return Dataset(raw, self.model)

    def clean(self, raw: Dataset[Frequentation]) -> Dataset[Frequentation]:
        """Parse dates (two source formats), cast types, normalize casing.

        Exact deduplication runs last, on normalized data, to catch duplicates
        that only differed by casing or date format.
        """
        cleaned = (
            raw.df.withColumn(
                "date",
                F.to_date(
                    F.coalesce(
                        F.try_to_timestamp(F.col("date"), F.lit("yyyy-MM-dd")),
                        F.try_to_timestamp(F.col("date"), F.lit("dd/MM/yyyy")),
                    )
                ),
            )
            .withColumn("gare_id", F.col("gare_id").cast(IntegerType()))
            .withColumn(
                "heure_tranche", F.trim(F.col("heure_tranche")).cast(IntegerType())
            )
            .withColumn("nb_voyageurs", F.col("nb_voyageurs").cast(IntegerType()))
            .withColumn(
                "nb_non_voyageurs", F.col("nb_non_voyageurs").cast(IntegerType())
            )
            .withColumn("latitude", F.col("latitude").cast(DoubleType()))
            .withColumn("longitude", F.col("longitude").cast(DoubleType()))
            .withColumn("region", F.initcap(F.trim(F.col("region"))))
            .withColumn("type_gare", F.initcap(F.trim(F.col("type_gare"))))
            .withColumn("ville", F.initcap(F.trim(F.col("ville"))))
            .withColumn("nom_gare", F.trim(F.col("nom_gare")))
            .withColumn("code_uic", F.trim(F.col("code_uic")))
            .dropDuplicates()
        )
        return Dataset(cleaned, self.model)
