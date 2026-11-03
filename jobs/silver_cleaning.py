from pyspark.sql.functions import col, to_date


def clean_call_data(df, silver_path):

    cleaned_df = df.dropDuplicates() \
        .dropna() \
        .withColumn("call_date", to_date(col("call_date")))

    cleaned_df.write.mode("overwrite").parquet(silver_path)
    print("✅ Silver Layer Completed")

    return cleaned_df