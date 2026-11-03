from utils.schema_definitions import cdr_schema


def ingest_call_records(spark, input_file, bronze_path):

    df = spark.read.csv(
        input_file,
        header=True,
        schema=cdr_schema()
    )

    df.write.mode("overwrite").parquet(bronze_path)
    print("✅ Bronze Layer Completed")

    return df