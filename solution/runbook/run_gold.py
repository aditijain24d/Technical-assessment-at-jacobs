"""Run all silver-layer transforms."""
import importlib.util
from solution.config.config import SOLUTION_DIR
from solution.helpers.spark_session import get_spark

def load_gold_module(filename):
    module_path = SOLUTION_DIR /"gold"/filename
    spec = importlib.util.spec_from_file_location(module_path.stem, module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

def run_gold_transforms():
    spark = get_spark("jacobs-gold-transforms")
    jobs = [
        ("company_monthly_metrics", "build_company_monthly_metrics.py", "build_company_monthly_metrics"),
    ]

    for table_name, script_name, function_name in jobs:
        print(f"Building gold.{table_name}...")
        module = load_gold_module(script_name)
        df = getattr(module, function_name)(spark)
        print(f"  rows loaded: {df.count()}")
        df.printSchema()
    print("Gold transforms complete.")

if __name__ == "__main__":
    run_gold_transforms()