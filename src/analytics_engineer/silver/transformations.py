from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F

VALID_SEGMENTS = {"A", "B", "C"}  # à ajuster selon tes vraies valeurs


def validate_frequentation(df: DataFrame) -> DataFrame:
    """
    Supprime les lignes inexploitables : clé manquante (date, gare_id),
    mesure manquante (nb_voyageurs), nb_voyageurs négatifs, et segments hors domaine.
    """
    return (
        df
        .dropna(subset=["date", "gare_id", "nb_voyageurs"])
        .filter((F.col("nb_voyageurs") >= 0) & (F.col("nb_non_voyageurs") >= 0))
        .filter(F.col("segment").isin(*VALID_SEGMENTS))
    )


def enrich_frequentation(
    df: DataFrame, jours_feries: DataFrame, population: DataFrame
) -> DataFrame:
    """Enrichit la fréquentation validée avec du contexte métier.

    Ajoute un flag jour férié et la population de la commune (pour la
    normalisation par habitant en Gold). Les left joins préservent
    chaque ligne même sans correspondance.
    """
    holidays = jours_feries.select("date").withColumn("est_jour_ferie", F.lit(True))

    return (
        df
        .join(holidays, on="date", how="left")
        .withColumn("est_jour_ferie", F.coalesce(F.col("est_jour_ferie"), F.lit(False)))
        .join(
            population.select(
                F.col("nom_commune").alias("ville"), "code_insee", "population"
            ),
            on="ville",
            how="left",
        )
    )

def run_silver(spark: SparkSession) -> None:

    df_silver = enrich_frequentation(
        validate_frequentation(spark.table("sncf_gc.bronze.frequentation")),
        spark.table("sncf_gc.bronze.jours_feries"),
        spark.table("sncf_gc.bronze.population_communes"),
    )

    (
        df_silver.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("sncf_gc.silver.frequentation_clean")
    )

