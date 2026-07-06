"""Build silver.support_tickets with parsed telemetry metadata."""
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType
from solution.helpers.silver_helpers import bronze_table_name, silver_table_name, write_to_silver
from solution.helpers.spark_session import get_spark

TABLE_NAME = "support_tickets"
TELEMETRY_SCHEMA = StructType(
    [
        StructField("browser", StringType()),
        StructField("os", StringType()),
        StructField("priority", StringType()),
    ]
)

def transform_support_tickets(spark):
    spark = spark or get_spark("silver-support-tickets")

    tickets_df = (
        spark.table(bronze_table_name("raw_support_tickets"))
        .withColumn("telemetry", F.from_json("telemetry_metadata", TELEMETRY_SCHEMA))
        .select(
            "ticket_id",
            "company_id",
            "created_at",
            "resolution_time_hours",
            "status",
            F.col("telemetry.browser").alias("browser"),
            F.col("telemetry.os").alias("os"),
            F.col("telemetry.priority").alias("priority"),
            F.date_trunc("month", F.col("created_at")).alias("metric_month"),
        )
    )
    write_to_silver(spark, tickets_df, TABLE_NAME)
    return spark.table(silver_table_name(TABLE_NAME))

if __name__ == "__main__":
    transform_support_tickets().show(truncate=False)
