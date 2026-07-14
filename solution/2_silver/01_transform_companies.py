"""Build 2_silver.companies from 1_bronze.raw_companies."""
from solution.helpers.silver_helpers import (bronze_table_name, silver_table_name, write_to_silver)
from solution.helpers.spark_session import get_spark

TABLE_NAME = "companies"

def transform_companies(spark):
    spark = spark or get_spark("2_silver-companies")
    companies_df = (
        spark.table(bronze_table_name("raw_companies"))
        .select(
            "company_id",
            "company_name",
            "created_at",
            "industry",
        )
        .dropDuplicates(["company_id"])
    )
    write_to_silver(spark, companies_df, TABLE_NAME)
    return spark.table(silver_table_name(TABLE_NAME))

if __name__ == "__main__":
    transform_companies().show(truncate=False)
