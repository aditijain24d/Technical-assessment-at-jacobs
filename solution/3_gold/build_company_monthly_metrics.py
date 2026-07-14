"""
Build company_monthly_metrics at Company x Month grain.
Includes:
  - MRR from active subscription months
  - Support Load Score (ticket volume + resolution hours)
  - High-priority ticket MoM spike flag (>200%, zero-safe)
"""
from functools import reduce

from pyspark.sql import functions as F
from solution.helpers.silver_helpers import silver_table_name
from solution.helpers.gold_helpers import write_to_gold, gold_table_name
from solution.helpers.spark_session import get_spark

TABLE_NAME = "company_monthly_metrics"

def build_company_mrr(spark):
    spark.table(silver_table_name("subscriptions")).createOrReplaceTempView("subscriptions")
    sql_stmt = ("select company_id, trunc(start_date,'month') as start_date, trunc(coalesce(end_date,now()),'month') as end_date, sum(monthly_amount) as mrr\
                from subscriptions\
                group by company_id, start_date, end_date")

    return spark.sql(f"{sql_stmt}")

def build_support_metrics(spark):
    spark.table(silver_table_name("support_tickets")).createOrReplaceTempView("tickets")
    sql_stmt = ("with t as(\
                           select company_id, trunc(created_at,'month') as ticket_month, ticket_id, resolution_time_hours,\
                           CASE WHEN UPPER(priority) != 'HIGH' THEN 0 ELSE 1 END as priority_ticket\
                           from tickets)\
                           select company_id, ticket_month, sum(priority_ticket) as priority_tickets, \
                           count(ticket_id) + sum(coalesce(resolution_time_hours,0)) as support_load_score\
                           from t group by company_id, ticket_month")

    return spark.sql(f"{sql_stmt}")

def apply_high_priority_spike_flags(metrics_df,spark):
    metrics_df.createOrReplaceTempView("metrics_df")
    sql_stmt = ("with df as( \
                select company_id, company_name, industry, metric_month, mrr, priority_tickets, support_load_score, \
                lag(priority_tickets,1) over(partition by company_id order by metric_month) as prev_priority_tickets from metrics_df)\
                select company_id, company_name, industry, metric_month, mrr, priority_tickets, support_load_score,\
                CASE \
                WHEN prev_priority_tickets == 0 OR prev_priority_tickets is NULL THEN 0 \
                WHEN prev_priority_tickets > 0 THEN (priority_tickets - prev_priority_tickets)/prev_priority_tickets * 100 END as high_priority_mom_pct_spike\
                from df")
    result = spark.sql(f"{sql_stmt}")
    return result.withColumn("high_priority_mom_spike_flag", F.col("high_priority_mom_pct_spike") > 200).filter(F.col("high_priority_mom_pct_spike")>=0)

def build_company_monthly_metrics(spark):
    spark = spark or get_spark("3_gold-company-monthly-metrics")

    spark.table(silver_table_name("companies")).createOrReplaceTempView("companies")
    spark.table(silver_table_name("calendar")).createOrReplaceTempView("cal")
    build_company_mrr(spark).createOrReplaceTempView("company_mrr")
    build_support_metrics(spark).createOrReplaceTempView("support_metrics")

    sql_stmt="select c.company_id, c.company_name, c.industry, cal.months as metric_month, cm.mrr, sm.priority_tickets, sm.support_load_score\
            from companies c \
            cross join cal \
            left join company_mrr cm on c.company_id = cm.company_id  and cal.months between cm.start_date and cm.end_date\
            left join support_metrics sm on c.company_id = sm.company_id and cal.months = sm.ticket_month\
            order by c.company_id, cal.months"
    metrics_df = spark.sql(f"{sql_stmt}").fillna(0, subset=["mrr", "support_load_score", "priority_tickets"])

    final_df = apply_high_priority_spike_flags(metrics_df,spark)
    final_df.select(
        "company_id",
        "company_name",
        "metric_month",
        "industry",
        "mrr",
        "priority_tickets",
        "support_load_score",
        "high_priority_mom_pct_spike",
        "high_priority_mom_spike_flag",
    )
    write_to_gold(spark, final_df, TABLE_NAME)
    return spark.table(gold_table_name(TABLE_NAME))

if __name__ == "__main__":
    build_company_monthly_metrics().show(truncate=False)
