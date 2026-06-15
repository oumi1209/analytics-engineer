from pyspark.testing.utils import assertDataFrameEqual
from analytics_engineer.silver.transformations import validate_frequentation


def build_row(**overrides):
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

    sample_data = [build_row(gare_id="G1"), build_row(gare_id="G2")]
    original_df = spark_fixture.createDataFrame(sample_data)

    transformed_df = validate_frequentation(original_df)

    expected_df = spark_fixture.createDataFrame(sample_data)
    assertDataFrameEqual(transformed_df, expected_df)


def test_supprime_lignes_invalides(spark_fixture):

    sample_data = [
        build_row(gare_id="G1"),  # valide
        build_row(gare_id="G2", date=None),  # clé manquante
        build_row(gare_id="G3", nb_voyageurs=-5),  # négatif
        build_row(gare_id="G4", segment="Z"),  # segment invalide
    ]
    original_df = spark_fixture.createDataFrame(sample_data)

    transformed_df = validate_frequentation(original_df)

    expected_df = spark_fixture.createDataFrame([build_row(gare_id="G1")])
    assertDataFrameEqual(transformed_df, expected_df)
