"""Validate source CSV files and silver-layer business logic."""
import json
import pandas as pd
from solution.config.config import SOURCE_FILES

def validate_source_csvs() -> None:
    companies = pd.read_csv(SOURCE_FILES["raw_companies"], parse_dates=["created_at"])
    subscriptions = pd.read_csv(
        SOURCE_FILES["raw_subscriptions"],
        parse_dates=["start_date", "end_date"],
    )
    tickets = pd.read_csv(
        SOURCE_FILES["raw_support_tickets"],
        parse_dates=["created_at"],
    )

    assert companies["company_id"].is_unique
    assert subscriptions["subscription_id"].is_unique
    assert tickets["ticket_id"].is_unique
    assert (subscriptions["monthly_amount"] >= 0).all()

    priorities = tickets["telemetry_metadata"].map(
        lambda value: json.loads(value)["priority"]
    )
    assert priorities.isin(["High", "Medium", "Low"]).all()

    print("Source CSV validation passed.")
    print(f"  companies: {len(companies)} rows")
    print(f"  subscriptions: {len(subscriptions)} rows")
    print(f"  support_tickets: {len(tickets)} rows")

if __name__ == "__main__":
    validate_source_csvs()
