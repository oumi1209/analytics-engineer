"""Unit tests for the Dataset[T] schema derivation — no SparkSession needed."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional

import pytest
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    TimestampType,
)

from analytics_engineer.infra.dataset import Dataset


@dataclass
class _Sample:
    an_int: int
    a_str: str
    a_float: float
    a_bool: bool
    a_date: date
    a_ts: datetime
    maybe_int: Optional[int]


def test_schema_of_maps_python_types_to_spark():
    fields = {
        f.name: (f.dataType, f.nullable) for f in Dataset.schema_of(_Sample).fields
    }
    assert fields["an_int"] == (IntegerType(), False)
    assert fields["a_str"] == (StringType(), False)
    assert fields["a_float"] == (DoubleType(), False)
    assert fields["a_bool"] == (BooleanType(), False)
    assert fields["a_date"] == (DateType(), False)
    assert fields["a_ts"] == (TimestampType(), False)


def test_optional_field_is_nullable():
    fields = {f.name: f.nullable for f in Dataset.schema_of(_Sample).fields}
    assert fields["maybe_int"] is True


def test_schema_of_rejects_non_dataclass():
    with pytest.raises(TypeError):
        Dataset.schema_of(int)
