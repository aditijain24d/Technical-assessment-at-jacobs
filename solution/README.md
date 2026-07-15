# Jacobs Assignment
Bronze-layer ingestion scripts load the provided CSV files from `../source/` into Delta tables that mirror the assignment's raw lakehouse tables. 
Silver-layer transforms clean the data and 
Gold-layer build the Company × Month analytical table required by the assignment.

## Source files
| CSV | Bronze table | Key fields |
|-----|--------------|------------|
| `raw_companies 1.csv` | `bronze.raw_companies` | company_id, company_name, created_at, industry |
| `raw_subscriptions 1.csv` | `bronze.raw_subscriptions` | subscription_id, company_id, monthly_amount, status, start_date, end_date |
| `raw_support_tickets 1.csv` | `bronze.raw_support_tickets` | ticket_id, company_id, created_at, resolution_time_hours, status, telemetry_metadata |

## Silver tables
| Table | Purpose |
|-------|---------|
| `silver.companies` | Cleaned company dimension |
| `silver.subscriptions` | Cleaned subscription history |
| `silver.support_tickets` | Tickets with parsed JSON (`priority`, `browser`, `os`) |
| `silver.calendar` | Calendar table for monthly matrix |

## Gold table
| Table                    | Purpose |
|--------------------------|---------|
| `company_monthly_metrics` | Company × Month grain with MRR, Support Load Score, high-priority MoM spike flag |

### Business logic
- **MRR**: Sum of `monthly_amount` for each month a subscription is active (start month through end month, or current month if still active).
- **Support Load Score**: `ticket_count + total_resolution_hours`.
- **High-priority MoM spike**: Flag when high-priority tickets increase by more than 200% month-over-month. If the previous month had zero high-priority tickets and the current month has any, the row is flagged.

## Run pipeline
cd `Technical-assessment-at-jacobs/solution/` :

```bash
python3 run_ingestion.py          # 1_bronze layer 
python3 run_silver.py             # 2_silver layer 
python3 run_gold.py               # 3_gold layer 
python3 test.py                   # validate formats 
```
Delta files are written to `Technical-assessment-at-jacobs/lakehouse/<schema>/<table_name>/`.

## Fabric notebook usage
In Microsoft Fabric, reuse the same read schemas and table names. Replace local paths with your lakehouse volume path and use the existing `spark` session instead of `get_spark()`.

Each bronze ingest script:
1. Reads the CSV with an explicit schema (`FAILFAST` mode)
2. Adds `ingestion_timestamp` and `source_file` metadata columns
3. Writes to a Delta table in the bronze schema

Silver/Gold PySpark scripts read from bronze/silver Delta tables and apply the transforms above.

## Configuration
Update `config.py` if your Fabric catalog name or source file locations differ.