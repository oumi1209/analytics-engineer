"""Single Delta writer reused by every stage and handler."""

from __future__ import annotations

from pyspark.sql import DataFrame


def write_delta_table(df: DataFrame, table_name: str, mode: str = "overwrite") -> None:
    (
        df.write.format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )
