from solution.config.config import LAKEHOUSE_DIR, SILVER_TABLE_PATHS, silver_schema, bronze_schema

def silver_table_name(table):
    return f"{silver_schema}.{table}"

def bronze_table_name(table):
    return f"{bronze_schema}.{table}"

def write_to_silver(spark, df, table, mode = "overwrite"):
    table_name = silver_table_name(table)
    target_path = str(SILVER_TABLE_PATHS[table])

    LAKEHOUSE_DIR.mkdir(parents=True, exist_ok=True)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {silver_schema}")
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