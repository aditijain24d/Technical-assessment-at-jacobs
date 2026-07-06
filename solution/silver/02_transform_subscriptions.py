"""Build silver.subscriptions from bronze.raw_subscriptions."""
from solution.helpers.silver_helpers import bronze_table_name, silver_table_name, write_to_silver
from solution.helpers.spark_session import get_spark
TABLE_NAME = "subscriptions"

def transform_subscriptions(spark):
    spark = spark or get_spark("silver-subscriptions")
    subscriptions_df = spark.table(bronze_table_name("raw_subscriptions")).select(
        "subscription_id",
        "company_id",
        "plan_name",
        "monthly_amount",
        "status",
        "start_date",
        "end_date",
    )
    write_to_silver(spark, subscriptions_df, TABLE_NAME)
    return spark.table(silver_table_name(TABLE_NAME))

if __name__ == "__main__":
    transform_subscriptions().show(truncate=False)
