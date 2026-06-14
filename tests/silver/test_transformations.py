import pytest
from pyspark.testing.utils import assertDataFrameEqual
from analytics_engineer.silver.transformations import validate_frequentation


def ligne_valide(**overrides):
    base = {
        "date": "2024-01-01",
        "gare_id": "G1",
        "nb_voyageurs": 100,
        "nb_non_voyageurs": 10,
        "segment": "A",
        "ville": "Paris",
    }
    base.update(overrides)
    return base


def test_garde_lignes_valides(spark_fixture):

    sample_data = [ligne_valide(gare_id="G1"), ligne_valide(gare_id="G2")]
    original_df = spark_fixture.createDataFrame(sample_data)

    transformed_df = validate_frequentation(original_df)

    expected_df = spark_fixture.createDataFrame(sample_data)
    assertDataFrameEqual(transformed_df, expected_df)


def test_supprime_lignes_invalides(spark_fixture):

    sample_data = [
        ligne_valide(gare_id="G1"),               # valide
        ligne_valide(gare_id="G2", date=None),    # clé manquante
        ligne_valide(gare_id="G3", nb_voyageurs=-5),  # négatif
        ligne_valide(gare_id="G4", segment="Z"),  # segment invalide
    ]
    original_df = spark_fixture.createDataFrame(sample_data)

    transformed_df = validate_frequentation(original_df)

    expected_df = spark_fixture.createDataFrame([ligne_valide(gare_id="G1")])
    assertDataFrameEqual(transformed_df, expected_df)