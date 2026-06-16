"""Gold step of the ``frequentation`` product: the star schema (3 dims + 1 fact)."""

from __future__ import annotations

import logging

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from analytics_engineer.infra.config_handler import get_config
from analytics_engineer.infra.dataset import Dataset
from analytics_engineer.infra.delta import write_delta_table
from analytics_engineer.products.frequentation.model import (
    TRANCHE_HORAIRE_REF,
    FrequentationClean,
    TrancheHoraire,
)

logger = logging.getLogger(__name__)


def _with_id_date(df: DataFrame, date_col: str = "date") -> DataFrame:
    """Add the numeric date key ``id_date`` (YYYYMMDD) used by dim_date and the fact."""
    return df.withColumn("id_date", F.date_format(date_col, "yyyyMMdd").cast("int"))


def build_dim_gare(silver: Dataset[FrequentationClean]) -> DataFrame:
    return silver.df.select(
        "gare_id",
        "code_uic",
        "nom_gare",
        "ville",
        "code_insee",
        "population",
        "region",
        "type_gare",
        "segment",
        "latitude",
        "longitude",
    ).dropDuplicates(["gare_id"])


def build_dim_date(silver: Dataset[FrequentationClean]) -> DataFrame:
    return (
        _with_id_date(
            silver.df.select("date", "est_jour_ferie").dropDuplicates(["date"])
        )
        .withColumn("jour", F.dayofmonth("date"))
        .withColumn("mois", F.month("date"))
        .withColumn("annee", F.year("date"))
        .withColumn("trimestre", F.quarter("date"))
        .withColumn("jour_semaine", F.date_format("date", "EEEE"))
        .withColumn("is_weekend", F.dayofweek("date").isin([1, 7]))
    )


def build_dim_tranche_horaire(spark: SparkSession) -> DataFrame:
    return Dataset.create(spark, TrancheHoraire, TRANCHE_HORAIRE_REF).df.withColumn(
        "id_tranche", F.col("heure_tranche")
    )


def build_fait_frequentation(
    silver: Dataset[FrequentationClean], dim_tranche: DataFrame
) -> DataFrame:
    return _with_id_date(
        silver.df.join(
            dim_tranche.select("id_tranche", "heure_tranche"),
            on="heure_tranche",
            how="left",
        )
    ).select("gare_id", "id_date", "id_tranche", "nb_voyageurs", "nb_non_voyageurs")


def run_gold(spark: SparkSession) -> None:
    config = get_config()
    silver = Dataset(
        spark.table(config.table("silver", "frequentation_clean")), FrequentationClean
    )

    dim_gare = build_dim_gare(silver)
    dim_date = build_dim_date(silver)
    dim_tranche_horaire = build_dim_tranche_horaire(spark)
    fait = build_fait_frequentation(silver, dim_tranche_horaire)

    tables = {
        "dim_gare": dim_gare,
        "dim_date": dim_date,
        "dim_tranche_horaire": dim_tranche_horaire,
        "fait_frequentation": fait,
    }
    for name, df in tables.items():
        target = config.table("gold", name)
        write_delta_table(df, target)
        logger.info("gold written: %s", target)
