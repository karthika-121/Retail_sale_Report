def build_star_schema(df, gold_path):

    dim_customer = df.select(
        "customer_id", "customer_name", "city", "country"
    ).distinct()

    dim_call_type = df.select(
        "call_type"
    ).distinct()

    fact_calls = df.select(
        "call_id",
        "customer_id",
        "call_type",
        "call_date",
        "duration_minutes",
        "call_cost",
        "anomaly_flag"
    )

    dim_customer.write.mode("overwrite").parquet(gold_path + "/dim_customer")
    dim_call_type.write.mode("overwrite").parquet(gold_path + "/dim_call_type")
    fact_calls.write.mode("overwrite").parquet(gold_path + "/fact_calls")

    print("✅ Gold Layer Completed: Star Schema Created")

    return fact_calls