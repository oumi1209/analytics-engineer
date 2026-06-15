"""Unit tests for the frequentation Gold star-schema builders, typed via Dataset[T]."""

from __future__ import annotations

from datetime import date

from pyspark.testing.utils import assertDataFrameEqual

from analytics_engineer.infra.dataset import Dataset
from analytics_engineer.products.frequentation.gold import (
    build_dim_date,
    build_dim_gare,
)
from analytics_engineer.products.frequentation.model import FrequentationClean


def frequentation_clean(**overrides) -> FrequentationClean:
    base = dict(
        gare_id=1,
        code_uic="87722025",
        nom_gare="Lyon Part-Dieu",
        ville="Lyon",
        region="Auvergne-Rhône-Alpes",
        type_gare="Terminus",
        segment="A",
        latitude=45.76,
        longitude=4.86,
        date=date(2022, 1, 1),
        heure_tranche=6,
        nb_voyageurs=100,
        nb_non_voyageurs=10,
        est_jour_ferie=False,
        code_insee="69123",
        population=522969,
    )
    base.update(overrides)
    return FrequentationClean(**base)


def test_dim_gare_dedupes_by_gare_id(spark_fixture):
    given = Dataset.create(
        spark_fixture,
        FrequentationClean,
        [frequentation_clean(), frequentation_clean()],
    )

    result = build_dim_gare(given).select("gare_id")

    expected = spark_fixture.createDataFrame([(1,)], "gare_id INT")
    assertDataFrameEqual(result, expected)


def test_dim_date_derives_calendar_features(spark_fixture):
    given = Dataset.create(
        spark_fixture, FrequentationClean, [frequentation_clean(date=date(2022, 1, 1))]
    )

    row = build_dim_date(given).collect()[0]

    assert row["id_date"] == 20220101
    assert (row["jour"], row["mois"], row["annee"], row["trimestre"]) == (1, 1, 2022, 1)
    assert row["jour_semaine"] == "Saturday"
    assert row["is_weekend"] is True
