from pyspark.sql import functions as F
from solution.config.config import BRONZE_TABLE_PATHS, bronze_schema


def add_ingestion_metadata(df, source_file):
    return (
        df.withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("source_file", F.lit(source_file))
    )

def bronze_table_name(table):
    return f"{bronze_schema}.{table}"

def write_to_bronze(spark,df,table,mode="overwrite"):
    table_name = bronze_table_name(table)
    target_path = str(BRONZE_TABLE_PATHS[table])

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {bronze_schema}")
    (
        df.write.format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
        .save(target_path)
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name}
        USING DELTA
        LOCATION '{target_path}'
        """
    )