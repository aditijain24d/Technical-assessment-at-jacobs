"""
Run all 1_bronze-layer ingestion jobs for assignment.
Loads:
  - raw_companies
  - raw_subscriptions
  - raw_support_tickets
Each table is written as a Delta table under jacobs.1_bronze.* with ingestion metadata.
"""
import importlib.util
from solution.config.config import SOLUTION_DIR
from solution.helpers.spark_session import get_spark

def load_ingest_module(filename):
    module_path = SOLUTION_DIR/"1_bronze"/filename
    spec = importlib.util.spec_from_file_location(module_path.stem, module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

def run_all_ingestion():
    spark = get_spark("jacobs-1_bronze-ingestion")
    ingest_jobs = [
        ("raw_companies", "01_ingest_raw_companies.py", "ingest_raw_companies"),
        ("raw_subscriptions", "02_ingest_raw_subscriptions.py", "ingest_raw_subscriptions"),
        ("raw_support_tickets", "03_ingest_raw_support_tickets.py", "ingest_raw_support_tickets"),
    ]
    for table_name, script_name, function_name in ingest_jobs:
        print(f"Ingesting {table_name}...")
        module = load_ingest_module(script_name)
        df = getattr(module, function_name)(spark)
        print(f"rows loaded: {df.count()}")
        df.printSchema()
    print("Bronze ingestion complete.")

if __name__ == "__main__":
    run_all_ingestion()