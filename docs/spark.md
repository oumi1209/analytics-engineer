# Spark

Databricks spark

## Imports

### Spark session / core (PySpark)

```python
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, DataFrame, Row, Column, Window, GroupedData

# On Databricks `spark` and `dbutils` are pre-instantiated, but for local/standalone:
spark = SparkSession.builder.appName("app").getOrCreate()
sc = spark.sparkContext
```

### DataFrame (PySpark)

```python
from pyspark.sql import DataFrame, Row, Column, Window
from pyspark.sql import functions as F          # col, lit, sum, when, count, ...
from pyspark.sql.functions import (
    col, lit, when, expr, concat, concat_ws,
    sum, avg, count, countDistinct, min, max,
    to_date, to_timestamp, date_format, datediff,
    row_number, rank, dense_rank, lag, lead,
    explode, split, regexp_replace, udf,
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType, DoubleType, FloatType,
    BooleanType, DateType, TimestampType, ArrayType, MapType, DecimalType,
)
from pyspark.sql.window import Window
```

### SparkSQL

```python
# PySpark: SQL is run through the SparkSession
from pyspark.sql import SparkSession

df = spark.sql("SELECT * FROM my_table")
df.createOrReplaceTempView("my_view")           # query it as SQL afterwards

from pyspark.sql.functions import expr           # SQL expressions inside DataFrame API
```

```sql
-- Databricks SQL / notebook %sql cell
SELECT * FROM catalog.schema.table;
```

### Dataset (Scala / Java only — no typed Dataset in PySpark)

```scala
import org.apache.spark.sql.{SparkSession, Dataset, DataFrame, Row, Column, Encoder, Encoders}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import spark.implicits._   // enables .toDF / .toDS and $"col" syntax

case class Person(name: String, age: Long)
val ds: Dataset[Person] = spark.read.json("people.json").as[Person]
```
