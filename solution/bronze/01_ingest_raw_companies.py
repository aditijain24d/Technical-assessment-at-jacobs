"""
Ingest raw_companies into bronze delta table
fields: company_id, company_name, created_at, industry
"""
from pyspark.sql.types import StringType, StructField, StructType, TimestampType
from solution.helpers.bronze_helpers import bronze_table_name, add_ingestion_metadata, write_to_bronze
from solution.config.config import SOURCE_FILES
from solution.helpers.spark_session import get_spark

TABLE_NAME = "raw_companies"
SOURCE_FILE = SOURCE_FILES[TABLE_NAME]

COMPANIES_SCHEMA = StructType(
    [
        StructField("company_id", StringType(), False),
        StructField("company_name", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("industry", StringType(), True),
    ]
)

def ingest_raw_companies(spark):
    spark = spark or get_spark("ingest-raw-companies")
    companies_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("mode", "FAILFAST")
        .schema(COMPANIES_SCHEMA)
        .load(str(SOURCE_FILE))
    )
    companies_final_df = add_ingestion_metadata(companies_df, str(SOURCE_FILE))
    write_to_bronze(spark, companies_final_df, TABLE_NAME)

    return spark.table(bronze_table_name(TABLE_NAME))

if __name__ == "__main__":
    ingest_raw_companies().show(truncate=False)
