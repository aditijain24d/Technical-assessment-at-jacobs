'''finding Top 5 most expensive industries to support relative to their MRR contribution.'''

from solution.helpers.gold_helpers import gold_table_name
from solution.helpers.spark_session import get_spark
spark=get_spark()

spark.table(gold_table_name("company_monthly_metrics")).createOrReplaceTempView("company_monthly_metrics")

df= spark.sql("select industry, (sum(support_load_score) / sum(MRR)) as expense_ratio \
                    from company_monthly_metrics where 0 not in(mrr,support_load_score)\
                    group by industry\
                    order by expense_ratio desc\
                    limit 5")
print(df.show(df.count(),truncate=False))
