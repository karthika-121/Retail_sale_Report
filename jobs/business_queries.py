from pyspark.sql.functions import sum, count, desc


def top_customers_by_call_cost(df):
    return df.groupBy("customer_name") \
        .agg(sum("call_cost").alias("total_spent")) \
        .orderBy(desc("total_spent"))


def revenue_by_call_type(df):
    return df.groupBy("call_type") \
        .agg(sum("call_cost").alias("total_revenue")) \
        .orderBy(desc("total_revenue"))


def anomaly_breakdown(df):
    return df.groupBy("anomaly_flag") \
        .agg(count("*").alias("call_count")) \
        .orderBy(desc("call_count"))