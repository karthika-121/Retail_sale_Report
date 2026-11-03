from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, DoubleType


def cdr_schema():
    return StructType([
        StructField("call_id", IntegerType(), True),
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("call_type", StringType(), True),
        StructField("duration_minutes", DoubleType(), True),
        StructField("call_cost", DoubleType(), True),
        StructField("call_date", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True)
    ])