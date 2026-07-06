from solution.config.config import LAKEHOUSE_DIR, GOLD_TABLE_PATHS, silver_schema, gold_schema

def silver_table_name(table):
    return f"{silver_schema}.{table}"

def gold_table_name(table):
    return f"{gold_schema}.{table}"

def write_to_gold(spark, df, table, mode = "overwrite"):
    table_name = gold_table_name(table)
    target_path = str(GOLD_TABLE_PATHS[table])

    LAKEHOUSE_DIR.mkdir(parents=True, exist_ok=True)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {gold_schema}")
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