from __future__ import annotations

from typing import Callable, Optional

from pyspark.sql import SparkSession

SparkFactory = Callable[[], SparkSession]


class SparkSessionHandler:
    """Process-wide handle to a single SparkSession.

    Stages receive the session as a parameter (``run_silver(spark)``,
    ``run_gold(spark)``). This handler owns *how* that session is built so
    callers (entrypoint, notebooks, tests) can swap the implementation
    without touching stage code:

    - prod entrypoint: default builder (``SparkSession.builder``)
    - Databricks notebook: ``SparkSessionHandler.set(spark)`` with the
      notebook-provided session
    - tests: ``SparkSessionHandler.set_factory(lambda: DatabricksSession...)``
    """

    _instance: Optional[SparkSession] = None
    _factory: Optional[SparkFactory] = None
    _app_name: str = "analytics-engineer"

    @classmethod
    def configure(cls, app_name: str) -> None:
        cls._app_name = app_name

    @classmethod
    def set_factory(cls, factory: SparkFactory) -> None:
        cls._factory = factory
        cls._instance = None

    @classmethod
    def set(cls, spark: SparkSession) -> None:
        cls._instance = spark

    @classmethod
    def get(cls) -> SparkSession:
        if cls._instance is not None:
            return cls._instance
        if cls._factory is not None:
            cls._instance = cls._factory()
        else:
            cls._instance = SparkSession.builder.appName(cls._app_name).getOrCreate()
        return cls._instance

    @classmethod
    def stop(cls) -> None:
        if cls._instance is not None:
            cls._instance.stop()
            cls._instance = None


def get_spark() -> SparkSession:
    return SparkSessionHandler.get()
