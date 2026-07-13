"""Build silver.calendar"""
from pyspark.sql import functions as F
from solution.helpers.silver_helpers import silver_table_name, write_to_silver
from solution.helpers.spark_session import get_spark

TABLE_NAME = "calendar"

def create_calendar(spark):
    spark = spark or get_spark("silver-calendar")
    df_cal = spark.table(silver_table_name("subscriptions")).select(F.trunc("start_date","month").alias("start_date"),
                                                                    F.coalesce(F.col("end_date"),F.trunc(F.now(),'month')).alias("end_date"))
    min_epochs = df_cal.select(F.min("start_date").alias("min_d"), F.max("end_date").alias("max_d")).first()
    min_date = min_epochs["min_d"]
    max_date = min_epochs["max_d"]
    # Generate months by using a range of months and add_months
    months_count = ( (max_date.year - min_date.year) * 12 + (max_date.month - min_date.month) + 1)

    calendar_df = (
        spark.range(months_count).withColumn("id", F.col("id")).select(F.add_months(F.lit(min_date), F.col("id")).alias("months"))
        .withColumn("year", F.year(F.col("months")))
    )
    write_to_silver(spark, calendar_df, TABLE_NAME)
    return spark.table(silver_table_name(TABLE_NAME))

if __name__ == "__main__":
    create_calendar().show(truncate=False)