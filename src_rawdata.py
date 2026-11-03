# src/extract.py
from pyspark.sql import SparkSession

def extract_data(spark, input_path):
    data_df = spark.read.csv(
        input_path,
        header=True,
        inferSchema=True
    )
    return data_df
