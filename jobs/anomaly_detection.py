from pyspark.sql.functions import col, when


def detect_anomalies(df, anomaly_path):

    anomaly_df = df.withColumn(
        "anomaly_flag",
        when(col("duration_minutes") > 120, "LONG_CALL")
        .when(col("call_type") == "International", "INTERNATIONAL_CALL")
        .otherwise("NORMAL")
    )

    anomaly_df.write.mode("overwrite").parquet(anomaly_path)
    print("🚨 Anomaly Detection Completed")

    return anomaly_df
