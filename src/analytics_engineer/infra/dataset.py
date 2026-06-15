"""``Dataset[T]`` — a Spark DataFrame tagged with the dataclass that types its rows.

PySpark has no typed ``Dataset[T]`` like Scala (see ``docs/spark.md``). This thin
wrapper brings back the two things that matter for an analytics pipeline:

* a *typed object* contract — the schema is derived from a ``@dataclass`` model and
  can be asserted with :meth:`Dataset.validate`;
* *modular testing* — :meth:`Dataset.create` builds a typed DataFrame straight from
  dataclass rows, so stage functions can be unit-tested offline with no fixtures
  beyond a local SparkSession.

It deliberately stays small: it is a tag over a ``DataFrame`` (reach the underlying
frame through ``.df`` for Spark operations), not a re-implementation of the
DataFrame API.
"""

from __future__ import annotations

from dataclasses import astuple, fields, is_dataclass
from datetime import date, datetime
from typing import (
    Generic,
    Sequence,
    TypeVar,
    Union,
    get_args,
    get_origin,
    get_type_hints,
)

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BooleanType,
    DataType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

T = TypeVar("T")

# Exact-type lookup (``bool`` is a distinct key from ``int``, so it maps correctly).
_PYTHON_TO_SPARK: dict[type, DataType] = {
    str: StringType(),
    int: IntegerType(),
    float: DoubleType(),
    bool: BooleanType(),
    date: DateType(),
    datetime: TimestampType(),
}


class SchemaValidationError(Exception):
    """Raised when a DataFrame does not conform to its model's derived schema."""


def _resolve(py_type: type) -> tuple[DataType, bool]:
    """Map a Python annotation to (Spark type, nullable). ``Optional`` -> nullable."""
    if get_origin(py_type) is Union:
        non_none = [arg for arg in get_args(py_type) if arg is not type(None)]
        if len(non_none) != 1:
            raise TypeError(f"unsupported union annotation: {py_type!r}")
        spark_type, _ = _resolve(non_none[0])
        return spark_type, True
    try:
        return _PYTHON_TO_SPARK[py_type], False
    except KeyError:
        raise TypeError(
            f"unsupported field type {py_type!r}; "
            f"supported: {sorted(t.__name__ for t in _PYTHON_TO_SPARK)}"
        ) from None


class Dataset(Generic[T]):
    """A ``DataFrame`` paired with the dataclass model describing its rows."""

    def __init__(self, df: DataFrame, model: type[T]) -> None:
        if not is_dataclass(model):
            raise TypeError(f"{model!r} must be a dataclass")
        self.df = df
        self.model = model

    # -- schema -----------------------------------------------------------------

    @classmethod
    def schema_of(cls, model: type[T]) -> StructType:
        """Derive the Spark ``StructType`` from the dataclass field annotations."""
        if not is_dataclass(model):
            raise TypeError(f"{model!r} must be a dataclass")
        hints = get_type_hints(model)
        struct_fields = []
        for field in fields(model):
            spark_type, nullable = _resolve(hints[field.name])
            struct_fields.append(StructField(field.name, spark_type, nullable))
        return StructType(struct_fields)

    @property
    def schema(self) -> StructType:
        return self.schema_of(self.model)

    def validate(self) -> "Dataset[T]":
        """Assert the DataFrame carries the model's columns with matching types.

        Compares by name and data type (nullability is left to Spark, which infers
        it loosely). Returns ``self`` so it chains: ``Dataset(df, M).validate().df``.
        """
        expected = {f.name: f.dataType for f in self.schema_of(self.model).fields}
        actual = {f.name: f.dataType for f in self.df.schema.fields}

        missing = sorted(set(expected) - set(actual))
        if missing:
            raise SchemaValidationError(
                f"{self.model.__name__}: missing columns {missing}"
            )
        mismatched = {
            name: (expected[name].simpleString(), actual[name].simpleString())
            for name in expected
            if actual[name] != expected[name]
        }
        if mismatched:
            raise SchemaValidationError(
                f"{self.model.__name__}: type mismatch {mismatched}"
            )
        return self

    # -- construction -----------------------------------------------------------

    @classmethod
    def create(
        cls, spark: SparkSession, model: type[T], rows: Sequence
    ) -> "Dataset[T]":
        """Build a typed ``Dataset`` from dataclass instances, dicts, or tuples.

        The schema is always the one derived from ``model`` — that is what makes
        unit tests typed and unambiguous instead of relying on Spark inference.
        """
        schema = cls.schema_of(model)
        field_names = [f.name for f in fields(model)]
        data = []
        for row in rows:
            if is_dataclass(row):
                data.append(astuple(row))
            elif isinstance(row, dict):
                data.append(tuple(row.get(name) for name in field_names))
            else:
                data.append(tuple(row))
        return cls(spark.createDataFrame(data, schema), model)

    def __repr__(self) -> str:
        return f"Dataset[{self.model.__name__}]"


__all__ = ["Dataset", "SchemaValidationError"]
