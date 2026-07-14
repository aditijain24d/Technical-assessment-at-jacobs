'''1. If an industry has a rapidly growing MRR but its Support Load Score is increasing at twice that rate, what strategy should the account management team take?
Pricing and contract restructuring
	•	Introduce / increase support tiers:
	•	Basic: self-serve + limited support.
	•	Premium: higher cost, dedicated support, and success resources.
	•	For high-load, low-engagement customers, move them to lower-touch plans or renegotiate contracts with clearer scope.

2. Looking at your final data layout, how would you define a "toxic" customer segment? What combination of data thresholds would you use to flag them automatically?
I'd tag segment as “toxic” customer segment where the combination of revenue, support cost, and retention risk makes the segment disproportionately unprofitable and/or
dangerous to long-term health.
Combination of MRR and Support Cost Ratio, this captures segments where the cost-to-serve is unsustainable.
Metric: Support Cost Load per MRR = (Total Support Cost ) / ( MRR).
Threshold: Flag as toxic if Support Cost per MRR > 0.30 (i.e., >30% of revenue is consumed by support)'''
from pyspark.sql.functions import count

from solution.helpers.gold_helpers import gold_table_name
from solution.helpers.spark_session import get_spark
spark=get_spark()

spark.table(gold_table_name("company_monthly_metrics")).createOrReplaceTempView("company_monthly_metrics")
print(spark.sql("select * from company_monthly_metrics").show())

df= spark.sql("select company_name, industry, metric_month, cast((support_load_score / MRR) * 100 as decimal(10,2)) as toxic_score \
                    from company_monthly_metrics where 0 not in(mrr,support_load_score)")
print(df.show(df.count(),truncate=False))


