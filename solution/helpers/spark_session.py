from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

def get_spark(app_name = "jacobs-ingestion"):
    builder = (
        SparkSession.builder.appName(app_name).enableHiveSupport()
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", "/Users/aditijain/PyCharmMiscProject/Jacobs/lakehouse")
                #"spark-warehouse")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()
