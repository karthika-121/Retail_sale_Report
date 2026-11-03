from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("RetailETL").getOrCreate()

# Test with a small DataFrame
data = [(1, "apple"), (2, "banana")]
df = spark.createDataFrame(data, ["id", "fruit"])
df.show()
