from config import RAW_FILE, BRONZE_PATH, SILVER_PATH, ANOMALY_PATH, GOLD_PATH
from utils.spark_session import get_spark_session

from jobs.bronze_ingestion import ingest_call_records
from jobs.silver_cleaning import clean_call_data
from jobs.anomaly_detection import detect_anomalies
from jobs.gold_star_schema import build_star_schema

from jobs.business_queries import (
    top_customers_by_call_cost,
    revenue_by_call_type,
    anomaly_breakdown
)


def main():
    spark = get_spark_session()

    print("\n--- Running Telecom CDR Pipeline ---\n")

    bronze_df = ingest_call_records(spark, RAW_FILE, BRONZE_PATH)
    silver_df = clean_call_data(bronze_df, SILVER_PATH)
    anomaly_df = detect_anomalies(silver_df, ANOMALY_PATH)

    build_star_schema(anomaly_df, GOLD_PATH)

    print("\n📊 Top Customers by Call Spending:")
    top_customers_by_call_cost(anomaly_df).show()

    print("\n📞 Revenue by Call Type:")
    revenue_by_call_type(anomaly_df).show()

    print("\n🚨 Anomaly Breakdown:")
    anomaly_breakdown(anomaly_df).show()

    spark.stop()
    print("\n✅ Telecom Pipeline Completed Successfully!\n")


if __name__ == "__main__":
    main()