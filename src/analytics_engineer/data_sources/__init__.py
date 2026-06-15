"""Bronze layer: one handler per source, run together by :func:`run_bronze`."""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession

from analytics_engineer.data_sources.holidays.handler import HolidayHandler
from analytics_engineer.data_sources.population.handler import PopulationHandler
from analytics_engineer.data_sources.train_traffic.handler import TrainTrafficHandler

logger = logging.getLogger(__name__)

HANDLERS = (TrainTrafficHandler, HolidayHandler, PopulationHandler)


def run_bronze(spark: SparkSession) -> None:
    """Ingest every source into its Bronze Delta table."""
    for handler_cls in HANDLERS:
        handler_cls(spark).run()


__all__ = ["run_bronze", "HANDLERS"]
