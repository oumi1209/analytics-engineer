"""Silver step of the ``frequentation`` product: validate, then enrich across sources.

Inputs and outputs are typed ``Dataset[...]`` so each function is unit-testable in
isolation: build a typed input with :meth:`Dataset.create`, assert the output.
"""

from __future__ import annotations

import logging

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from analytics_engineer.data_sources.holidays.model import RefHoliday
from analytics_engineer.data_sources.population.model import PopulationCommune
from analytics_engineer.data_sources.train_traffic.model import Frequentation
from analytics_engineer.infra.config_handler import get_config
from analytics_engineer.infra.dataset import Dataset
from analytics_engineer.infra.delta import write_delta_table
from analytics_engineer.products.frequentation.model import FrequentationClean

logger = logging.getLogger(__name__)

VALID_SEGMENTS = {"A", "B", "C"}


def validate_frequentation(
    freq: Dataset[Frequentation],
) -> Dataset[Frequentation]:
    """Drop unusable rows: missing key/measure, negative counts, unknown segment."""
    validated = (
        freq.df.dropna(subset=["date", "gare_id", "nb_voyageurs"])
        .filter((F.col("nb_voyageurs") >= 0) & (F.col("nb_non_voyageurs") >= 0))
        .filter(F.col("segment").isin(*VALID_SEGMENTS))
    )
    return Dataset(validated, Frequentation)


def enrich_frequentation(
    freq: Dataset[Frequentation],
    holidays: Dataset[RefHoliday],
    population: Dataset[PopulationCommune],
) -> Dataset[FrequentationClean]:
    """Add a public-holiday flag and the commune population.

    Left joins preserve every footfall row even with no reference match.
    """
    holiday_flags = holidays.df.select("date").withColumn("est_jour_ferie", F.lit(True))
    enriched = (
        freq.df.join(holiday_flags, on="date", how="left")
        .withColumn("est_jour_ferie", F.coalesce(F.col("est_jour_ferie"), F.lit(False)))
        .join(
            population.df.select(
                F.col("nom_commune").alias("ville"), "code_insee", "population"
            ),
            on="ville",
            how="left",
        )
    )
    return Dataset(enriched, FrequentationClean)


def run_silver(spark: SparkSession) -> None:
    config = get_config()
    freq = Dataset(spark.table(config.table("bronze", "frequentation")), Frequentation)
    holidays = Dataset(spark.table(config.table("bronze", "jours_feries")), RefHoliday)
    population = Dataset(
        spark.table(config.table("bronze", "population_communes")), PopulationCommune
    )

    clean = enrich_frequentation(validate_frequentation(freq), holidays, population)

    target = config.table("silver", "frequentation_clean")
    write_delta_table(clean.df, target)
    logger.info("silver written: %s", target)
