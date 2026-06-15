"""Unit tests for the frequentation Silver step, using typed Dataset[T] inputs."""

from __future__ import annotations

from datetime import date

from pyspark.testing.utils import assertDataFrameEqual

from analytics_engineer.data_sources.train_traffic.model import Frequentation
from analytics_engineer.infra.dataset import Dataset
from analytics_engineer.products.frequentation.silver import validate_frequentation


def frequentation(**overrides) -> Frequentation:
    base = dict(
        gare_id=1,
        code_uic="87722025",
        nom_gare="Paris Gare de Lyon",
        ville="Paris",
        region="Île-de-France",
        type_gare="Terminus",
        segment="A",
        latitude=48.84,
        longitude=2.37,
        date=date(2024, 1, 1),
        heure_tranche=8,
        nb_voyageurs=100,
        nb_non_voyageurs=10,
    )
    base.update(overrides)
    return Frequentation(**base)


def test_validate_drops_unusable_rows(spark_fixture):
    rows = [
        frequentation(gare_id=1),  # valid
        frequentation(gare_id=2, date=None),  # missing key
        frequentation(gare_id=3, nb_voyageurs=-5),  # negative measure
        frequentation(gare_id=4, segment="Z"),  # unknown segment
    ]
    given = Dataset.create(spark_fixture, Frequentation, rows)

    result = validate_frequentation(given)

    expected = Dataset.create(spark_fixture, Frequentation, [frequentation(gare_id=1)])
    assertDataFrameEqual(result.df, expected.df)
