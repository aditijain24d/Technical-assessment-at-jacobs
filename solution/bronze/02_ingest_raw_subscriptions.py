"""
Ingest raw_subscriptions into bronze delta table
fields: subscription_id, company_id, plan_name, monthly_amount, status, start_date, end_date
"""
from pyspark.sql.types import DateType, DecimalType, StringType, StructField, StructType
from solution.helpers.bronze_helpers import add_ingestion_metadata, bronze_table_name, write_to_bronze
from solution.config.config import SOURCE_FILES
from solution.helpers.spark_session import get_spark

TABLE_NAME = "raw_subscriptions"
SOURCE_FILE = SOURCE_FILES[TABLE_NAME]

SUBSCRIPTIONS_SCHEMA = StructType(
    [
        StructField("subscription_id", StringType(), False),
        StructField("company_id", StringType(), False),
        StructField("plan_name", StringType(), True),
        StructField("monthly_amount", DecimalType(10, 2), True),
        StructField("status", StringType(), True),
        StructField("start_date", DateType(), True),
        StructField("end_date", DateType(), True),
    ]
)

def ingest_raw_subscriptions(spark):
    spark = spark or get_spark("ingest-raw-subscriptions")
    subscriptions_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("mode", "FAILFAST")
        .schema(SUBSCRIPTIONS_SCHEMA)
        .load(str(SOURCE_FILE))
    )
    subscriptions_final_df = add_ingestion_metadata(subscriptions_df, str(SOURCE_FILE))
    write_to_bronze(spark, subscriptions_final_df, TABLE_NAME)

    return spark.table(bronze_table_name(TABLE_NAME))

if __name__ == "__main__":
    ingest_raw_subscriptions().show(truncate=False)