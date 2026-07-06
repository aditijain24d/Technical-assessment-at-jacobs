"""
Ingest raw_support_tickets into bronze delta table
fields: ticket_id, company_id, created_at, resolution_time_hours, status, telemetry_metadata
"""
from pyspark.sql.types import DecimalType, StringType, StructField, StructType, TimestampType
from solution.helpers.bronze_helpers import add_ingestion_metadata, bronze_table_name, write_to_bronze
from solution.config.config import SOURCE_FILES
from solution.helpers.spark_session import get_spark

TABLE_NAME = "raw_support_tickets"
SOURCE_FILE = SOURCE_FILES[TABLE_NAME]

SUPPORT_TICKETS_SCHEMA = StructType(
    [
        StructField("ticket_id", StringType(), False),
        StructField("company_id", StringType(), False),
        StructField("created_at", TimestampType(), True),
        StructField("resolution_time_hours", DecimalType(10, 2), True),
        StructField("status", StringType(), True),
        StructField("telemetry_metadata", StringType(), True),
    ]
)

def ingest_raw_support_tickets(spark):
    spark = spark or get_spark("ingest-raw-support-tickets")
    tickets_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("escape", "\"")
        .option("quote", "\"")
        .option("mode", "FAILFAST")
        .schema(SUPPORT_TICKETS_SCHEMA)
        .load(str(SOURCE_FILE))
    )
    tickets_final_df = add_ingestion_metadata(tickets_df, str(SOURCE_FILE))
    write_to_bronze(spark, tickets_final_df, TABLE_NAME)

    return spark.table(bronze_table_name(TABLE_NAME))

if __name__ == "__main__":
    ingest_raw_support_tickets().show(truncate=False)