"""
Build company_monthly_metrics at Company x Month grain.
Includes:
  - MRR from active subscription months
  - Support Load Score (ticket volume + resolution hours)
  - High-priority ticket MoM spike flag (>200%, zero-safe)
"""
from pyspark.sql import functions as F
from solution.helpers.silver_helpers import silver_table_name
from solution.helpers.gold_helpers import write_to_gold, gold_table_name
from solution.helpers.spark_session import get_spark

TABLE_NAME = "company_monthly_metrics"

def build_company_mrr(spark):
    df_subscriptions = spark.table(silver_table_name("subscriptions"))
    df_subscriptions.createOrReplaceTempView("subscriptions")

    df_companies = spark.table(silver_table_name("companies"))
    df_companies.createOrReplaceTempView("companies")

    sql_stmt = (f"select c.company_id, trunc(s.start_date,'month') as metric_month, \
            s.monthly_amount * floor(months_between(coalesce(s.end_date,current_date()), s.start_date)) as MRR \
            from companies c join subscriptions s on c.company_id = s.company_id \
            where s.status ='active'")
    return spark.sql(f"{sql_stmt}")

def build_support_metrics(spark):
    df_tickets = spark.table(silver_table_name("support_tickets"))
    df_tickets.createOrReplaceTempView("tickets")
    sql_stmt = ("with t as(\
                   select company_id, trunc(created_at,'month') as metric_month, ticket_id, resolution_time_hours, CASE WHEN upper(priority) == 'HIGH' THEN 1 ELSE 0 END as high_priority_ticket\
                   from tickets)\
                   select company_id, metric_month, count(ticket_id) as ticket_count, sum(coalesce(resolution_time_hours,0)) as total_resolution_hours, \
                   sum(high_priority_ticket) as total_priority_ticket_count\
                   from t group by company_id, metric_month")
    result = spark.sql(f"{sql_stmt}")
    return result.withColumn("support_load_score",(F.col("total_resolution_hours")+F.col("ticket_count")))

def apply_high_priority_spike_flags(metrics_df,spark):
    metrics_df.createOrReplaceTempView("metrics_df")
    sql_stmt = ("with cte as( \
            select company_id, company_name, metric_month, industry, mrr, ticket_count, total_resolution_hours, total_priority_ticket_count, support_load_score, \
            lag(total_priority_ticket_count,1) over(partition by company_id order by metric_month) as prev_high_priority_ticket_count from metrics_df)\
            select company_id, company_name, metric_month, industry, mrr, ticket_count, total_resolution_hours, total_priority_ticket_count, support_load_score,\
            CASE \
            WHEN prev_high_priority_ticket_count == 0 OR prev_high_priority_ticket_count is NULL THEN 0 \
            WHEN prev_high_priority_ticket_count > 0 THEN (total_priority_ticket_count - prev_high_priority_ticket_count)/prev_high_priority_ticket_count * 100 END as high_priority_mom_pct_spike\
            from cte")
    result = spark.sql(f"{sql_stmt}")
    return result.withColumn("high_priority_mom_spike_flag", F.col("high_priority_mom_pct_spike") > 200)

def build_company_monthly_metrics(spark):
    spark = spark or get_spark("gold-company-monthly-metrics")

    companies = spark.table(silver_table_name("companies"))
    company_mrr = build_company_mrr(spark)
    support_metrics = build_support_metrics(spark)

    all_months = (
        company_mrr.select("metric_month")
        .union(support_metrics.select("metric_month"))
        .distinct()
    )
    spine = companies.select("company_id").crossJoin(all_months)

    metrics_df = (
        spine.join(companies, on="company_id", how="left")
        .join(company_mrr, on=["company_id", "metric_month"], how="left")
        .join(support_metrics, on=["company_id", "metric_month"], how="left")
        .fillna(
            0,
            subset=[
                "mrr",
                "ticket_count",
                "total_resolution_hours",
                "total_priority_ticket_count",
                "support_load_score",
            ],
        )
    )

    metrics_df=metrics_df.drop("created_at")
    final_df = apply_high_priority_spike_flags(metrics_df,spark)
    final_df.select(
        "company_id",
        "company_name",
        "metric_month",
        "industry",
        "mrr",
        "ticket_count",
        "total_resolution_hours",
        "total_priority_ticket_count",
        "support_load_score",
        "high_priority_mom_pct_spike",
        "high_priority_mom_spike_flag",
    )
    write_to_gold(spark, final_df, TABLE_NAME)
    return spark.table(gold_table_name(TABLE_NAME))

if __name__ == "__main__":
    build_company_monthly_metrics().show(truncate=False)
