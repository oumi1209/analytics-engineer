import pyspark.sql.functions as F
from pyspark.sql import DataFrame
'''
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

'''

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

TRANCHE_HORAIRE_REF = [
    (0,  "00h-03h", "Nuit",             "Nuit"),
    (3,  "03h-06h", "Nuit",             "Nuit"),
    (6,  "06h-09h", "Matin",            "Heure de pointe"),
    (9,  "09h-12h", "Matinée",          "Heures creuses"),
    (12, "12h-15h", "Après-midi",       "Heures creuses"),
    (15, "15h-18h", "Fin d'après-midi", "Heures creuses"),
    (18, "18h-21h", "Soir",             "Heure de pointe"),
    (21, "21h-24h", "Soirée tardive",   "Nuit"),
]

TRANCHE_HORAIRE_SCHEMA = """
heure_tranche INT,
libelle STRING,
periode_journee STRING,
type_tranche STRING
"""


def build_dim_gare(df_silver: DataFrame) -> DataFrame:
    return (
        df_silver
        .select(
            "gare_id", "code_uic", "nom_gare", "ville", "code_insee",
            "population", "region", "type_gare", "segment", "latitude", "longitude",
        )
        .dropDuplicates(["gare_id"])
    )


def build_dim_date(df_silver: DataFrame) -> DataFrame:
    return (
        df_silver
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


def build_dim_tranche_horaire(spark: SparkSession) -> DataFrame:
    return (
        spark.createDataFrame(TRANCHE_HORAIRE_REF, schema=TRANCHE_HORAIRE_SCHEMA)
        .withColumn("id_tranche", F.col("heure_tranche"))
    )


def build_fait_frequentation(df_silver: DataFrame, dim_tranche: DataFrame) -> DataFrame:
    return (
        df_silver
        .join(
            dim_tranche.select("id_tranche", "heure_tranche"),
            on="heure_tranche",
            how="left",
        )
        .withColumn("id_date", F.date_format("date", "yyyyMMdd").cast("int"))
        .select("gare_id", "id_date", "id_tranche", "nb_voyageurs", "nb_non_voyageurs")
    )