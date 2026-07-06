"""Run all silver-layer transforms."""
import importlib.util
from solution.config.config import SOLUTION_DIR
from solution.helpers.spark_session import get_spark

def load_silver_module(filename):
    module_path = SOLUTION_DIR/"silver"/filename
    spec = importlib.util.spec_from_file_location(module_path.stem, module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

def run_all_silver_transforms():
    spark = get_spark("jacobs-silver-transforms")
    jobs = [
        ("companies", "01_transform_companies.py", "transform_companies"),
        ("subscriptions", "02_transform_subscriptions.py", "transform_subscriptions"),
        ("support_tickets", "03_transform_support_tickets.py", "transform_support_tickets"),
    ]

    for table_name, script_name, function_name in jobs:
        print(f"Building silver.{table_name}...")
        module = load_silver_module(script_name)
        df = getattr(module, function_name)(spark)
        print(f"  rows loaded: {df.count()}")
        df.printSchema()
    print("Silver transforms complete.")

if __name__ == "__main__":
    run_all_silver_transforms()