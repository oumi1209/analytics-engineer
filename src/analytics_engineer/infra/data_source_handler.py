"""Base class for a single data source's journey up to the Bronze layer.

A handler owns *one* source end-to-end through Bronze: ``read`` the raw file,
``clean`` it (technical typing/normalization, no business rules), validate it
against the source's typed model, and ``write`` it as a Delta table. Cross-source
work (Silver enrichment, the Gold star schema) is *not* a single source's
responsibility and lives under :mod:`analytics_engineer.products`.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Optional

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from analytics_engineer.infra.config_handler import Config, get_config
from analytics_engineer.infra.dataset import Dataset
from analytics_engineer.infra.delta import write_delta_table
from analytics_engineer.infra.spark import get_spark

logger = logging.getLogger(__name__)

BRONZE_SCHEMA = "bronze"


class BaseDataSourceHandler(ABC):
    #: dataclass typing this source's rows (subclasses set it)
    model: type
    #: Bronze table short-name, under the ``bronze`` schema (subclasses set it)
    table_name: str

    def __init__(
        self, spark: Optional[SparkSession] = None, config: Optional[Config] = None
    ) -> None:
        self.spark = spark or get_spark()
        self.config = config or get_config()

    @abstractmethod
    def read(self) -> Dataset:
        """Read the raw source, tagged with :attr:`model` (not yet conforming)."""

    @abstractmethod
    def clean(self, raw: Dataset) -> Dataset:
        """Technical cleaning only: typing, date parsing, casing, dedup."""

    def schema_validate(self, dataset: Dataset) -> Dataset:
        """Enforce the source's typed contract before it reaches Bronze."""
        return dataset.validate()

    @property
    def bronze_table(self) -> str:
        return self.config.table(BRONZE_SCHEMA, self.table_name)

    def run(self) -> str:
        """read -> clean -> validate -> write Bronze. Returns the table written."""
        logger.info("ingesting %s into %s", self.model.__name__, self.bronze_table)
        cleaned = self.schema_validate(self.clean(self.read()))
        stamped = cleaned.df.withColumn("ingestion_timestamp", F.current_timestamp())
        write_delta_table(stamped, self.bronze_table)
        logger.info("bronze written: %s", self.bronze_table)
        return self.bronze_table
