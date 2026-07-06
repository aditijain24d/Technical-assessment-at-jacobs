from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
SOLUTION_DIR = Path(__file__).resolve().parent.parent
SOURCE_DIR = PROJECT_ROOT / "source"
LAKEHOUSE_DIR = PROJECT_ROOT / "lakehouse"

# Fabric / Unity Catalog naming (adjust catalog_name in Fabric workspace if needed)
catalog_name = "jacobs"
bronze_schema = "bronze"
silver_schema = "silver"
gold_schema = "gold"

# Raw CSV landing files (provided in source/)
SOURCE_FILES = {
    "raw_companies": SOURCE_DIR/"raw_companies 1.csv",
    "raw_subscriptions": SOURCE_DIR/"raw_subscriptions 1.csv",
    "raw_support_tickets": SOURCE_DIR/"raw_support_tickets 1.csv",
}
# Local Delta table storage (used when not running inside Fabric)
BRONZE_TABLE_PATHS = {table: LAKEHOUSE_DIR / bronze_schema / table for table in SOURCE_FILES}

# Silver Tables
SILVER_TABLES = [ "companies", "subscriptions", "support_tickets" ]
SILVER_TABLE_PATHS = {table: LAKEHOUSE_DIR / silver_schema / table for table in SILVER_TABLES}

# Gold Tables
GOLD_TABLES = [ "company_monthly_metrics" ]
GOLD_TABLE_PATHS = {table: LAKEHOUSE_DIR/ gold_schema / table for table in GOLD_TABLES}