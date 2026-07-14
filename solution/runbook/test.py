"""Validate source CSV files and 2_silver-layer business logic."""
import json
import pandas as pd
from solution.config.config import SOURCE_FILES
from solution.helpers.spark_session import get_spark
from pyspark.sql import functions as F

spark=get_spark()

def validate_source_csvs():
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
    assert (subscriptions["monthly_amount"] >= 0).all()
    assert tickets["ticket_id"].is_unique

    priorities = tickets["telemetry_metadata"].map(
        lambda value: json.loads(value)["priority"]
    )
    assert priorities.isin(["High", "Medium", "Low"]).all()

    print("Source CSV validation passed.")
    print(f"  companies: {len(companies)} rows")
    print(f"  subscriptions: {len(subscriptions)} rows")
    print(f"  support_tickets: {len(tickets)} rows")

def validate_silver():
    companies = spark.read.format("delta").load("/Users/aditijain/Technical-assessment-at-jacobs/lakehouse/silver/companies")
    assert companies.select("company_id").count() == companies.select("company_id").distinct().count()

    s = spark.read.format("delta").load("/Users/aditijain/Technical-assessment-at-jacobs/lakehouse/silver/subscriptions")
    assert s.select("subscription_id").count() == s.select("subscription_id").distinct().count()
    assert s.filter(F.col("monthly_amount") == 0 ).count() == 0, "Some rows have monthly_amount as 0"

    t = spark.read.format("delta").load("/Users/aditijain/Technical-assessment-at-jacobs/lakehouse/silver/support_tickets")
    assert t.select("ticket_id").count() == t.select("ticket_id").distinct().count()
    assert t.filter(F.col("resolution_time_hours") <= 0 ).count() == 0, "Some rows have resolution_time_hours <= 0>"
    values=["High", "Medium", "Low"]
    assert t.filter(~F.col("priority").isin(values)).count() == 0, "Some rows have priority other than High, Medium, Low>"

    c = spark.read.format("delta").load("/Users/aditijain/Technical-assessment-at-jacobs/lakehouse/silver/calendar")
    assert c.select("months").count() == c.select("months").distinct().count()
    assert c.filter(F.col("months") > F.now()).count() == 0, "Invalid months"

    print("Silver validation passed.")
    print(f"  companies: {companies.count()} rows")
    print(f"  subscriptions: {s.count()} rows")
    print(f"  support_tickets: {t.count()} rows")
    print(f"  calendar: {c.count()} rows")

def validate_gold():
    cmm = spark.read.format("delta").load("/Users/aditijain/Technical-assessment-at-jacobs/lakehouse/gold/company_monthly_metrics")
    assert cmm.select("company_id","metric_month").count() == cmm.select("company_id","metric_month").distinct().count()
    assert cmm.filter(F.col("mrr") < 0).count() == 0, "Some rows have MRR less than 0"
    assert cmm.filter(F.col("priority_tickets") < 0).count() == 0, "Some rows have priority tickets less than 0"
    assert cmm.filter(F.col("support_load_score") < 0).count() == 0, "Some rows have support_load_score less than 0"
    assert cmm.filter(F.col("high_priority_mom_pct_spike") < 0).count() == 0, "Some rows have high_priority_mom_pct_spike less than 0"
    assert cmm.filter(~F.col("high_priority_mom_spike_flag").isin("true","false")).count() == 0, "Some rows have invalid high_priority_mom_spike_flag"

    print("Gold validation passed.")
    print(f"  companies: {cmm.count()} rows")

if __name__ == "__main__":
    validate_source_csvs()
    validate_silver()
    validate_gold()