import pyspark.testing
from pyspark.testing.utils import assertDataFrameEqual
from analytics_engineer.gold.traffic import build_dim_date

def test_build_dim_date(spark_fixture):
    sample_data=[{'date':'2022-01-01','est_jour_ferie':False}]
    original_df=spark_fixture.createDataFrame(sample_data)
    transformed_df=build_dim_date(original_df)

    expected_data=[{'date':'2022-01-01','est_jour_ferie':False,'id_date':20220101,'jour':1,'mois':1,'annee':2022,'trimestre':1, 'jour_semaine':'Saturday','is_weekend':True}]

    expected_df = spark_fixture.createDataFrame(expected_data,schema=transformed_df.schema)
    assertDataFrameEqual(transformed_df, expected_df,ignoreColumnOrder=True)






