from analytics_engineer.gold.traffic import build_dim_date
from analytics_engineer.silver.transformations import (
    enrich_frequentation,
    validate_frequentation,
)
from pyspark.sql import SparkSession


def run() -> None:
    spark = SparkSession.builder.appName("analytics-engineer").getOrCreate()

    frequentation = spark.read.table("bronze.frequentation")
    holidays = spark.read.table("bronze.jours_feries")
    population = spark.read.table("bronze.population")

    valid = validate_frequentation(frequentation)
    enriched = enrich_frequentation(valid, holidays, population)
    enriched.write.mode("overwrite").saveAsTable("silver.frequentation")

    dim_date = build_dim_date(enriched)
    dim_date.write.mode("overwrite").saveAsTable("gold.dim_date")

    spark.stop()


if __name__ == "__main__":
    # run integration, clean, enrich, all with argument command
    run()
