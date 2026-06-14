import pyspark.testing
from pyspark.testing.utils import assertDataFrameEqual
from analytics_engineer.gold.traffic import build_dim_date,build_dim_gare,build_dim_tranche_horaire,build_fait_frequentation
'''
def test_build_dim_date(spark_fixture):
    sample_data=[{'date':'2022-01-01','est_jour_ferie':False}]
    original_df=spark_fixture.createDataFrame(sample_data)
    transformed_df=build_dim_date(original_df)

    expected_data=[{'date':'2022-01-01','est_jour_ferie':False,'id_date':20220101,'jour':1,'mois':1,'annee':2022,'trimestre':1, 'jour_semaine':'Saturday','is_weekend':True}]

    expected_df = spark_fixture.createDataFrame(expected_data,schema=transformed_df.schema)
    assertDataFrameEqual(transformed_df, expected_df,ignoreColumnOrder=True)

'''


def ligne_silver(**overrides):
    base = {
        "gare_id": "G1",
        "code_uic": "87722025",
        "nom_gare": "Lyon Part-Dieu",
        "ville": "Lyon",
        "code_insee": "69123",
        "population": 500000,
        "region": "Auvergne-Rhône-Alpes",
        "type_gare": "A",
        "segment": "A",
        "latitude": 45.76,
        "longitude": 4.86,
        "date": "2024-01-01",
        "est_jour_ferie": True,
        "heure_tranche": 6,
        "nb_voyageurs": 100,
        "nb_non_voyageurs": 10
    }
    base.update(overrides)
    return base


def test_build_dim_date(spark_fixture):
    sample_data=[{'date':'2022-01-01','est_jour_ferie':False}]
    original_df=spark_fixture.createDataFrame(sample_data)
    transformed_df=build_dim_date(original_df)

    expected_data=[{'date':'2022-01-01','est_jour_ferie':False,'id_date':20220101,'jour':1,'mois':1,'annee':2022,'trimestre':1, 'jour_semaine':'Saturday','is_weekend':True}]

    expected_df = spark_fixture.createDataFrame(expected_data,schema=transformed_df.schema)
    assertDataFrameEqual(transformed_df, expected_df,ignoreColumnOrder=True)



def test_dim_gare_deduplique(spark_fixture):
    input_df = spark_fixture.createDataFrame([ligne_silver(), ligne_silver()])

    transformed_df = build_dim_gare(input_df).select("gare_id")
    expected_df = spark_fixture.createDataFrame([{"gare_id": "G1"}])

    assertDataFrameEqual(transformed_df, expected_df)


