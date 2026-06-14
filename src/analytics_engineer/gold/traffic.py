import pyspark.sql.functions as F
from pyspark.sql import DataFrame

def build_dim_date(df: DataFrame) -> DataFrame:
    return (
        df
        .select("date", "est_jour_ferie")
        .dropDuplicates(["date"])
        .withColumn("id_date", F.date_format("date", "yyyyMMdd").cast("int"))
        .withColumn("jour", F.dayofmonth("date"))
        .withColumn("mois", F.month("date"))
        .withColumn("annee", F.year("date"))
        .withColumn("trimestre", F.quarter("date"))
        .withColumn("jour_semaine", F.date_format("date", "EEEE"))
        .withColumn("is_weekend", F.dayofweek("date").isin([1, 7]))
    )

