# Dynamic Tables to dbt Migration Reference

## Quick Reference: SQL to dbt Model Mapping

| Original Dynamic Table | dbt Model | Materialization | Schedule | Description |
|------------------------|-----------|-----------------|----------|-------------|
| `dt_transactions` | `int_transactions_cleaned` | incremental | 1 minute | Cleaned transactions with validation |
| `dt_merchants` | `int_merchants_scd1` | table | 15 minutes | Merchants with SCD Type 1 |
| `dt_clients` | `int_clients_scd2` | table | 5 minutes | Clients with SCD Type 2 |
| `dt_invalid_trans` | `dt_invalid_trans` | table | 1 minute | Invalid transactions for analysis |
| `dt_fraud_full` | `dt_fraud_full` | table | 1 minute | Enriched fraud dataset |

## Feature Comparison

### Original SQL Implementation
```sql
-- sql/dynamic_tables.sql (lines 200-395)

CREATE OR REPLACE DYNAMIC TABLE dt_transactions
  TARGET_LAG = '1 minute'
  WAREHOUSE = dynamic_wh
AS
SELECT ...
```

### dbt Implementation
```sql
-- models/intermediate/int_transactions_cleaned.sql

{{
  config(
    materialized='incremental',
    unique_key='trans_num',
    cluster_by=['trans_ts']
  )
}}

SELECT ...
{% if is_incremental() %}
  WHERE meta_load_ts > (SELECT MAX(meta_load_ts) FROM {{ this }})
{% endif %}
```

## Key Differences

### 1. Scheduling
- **Original**: `TARGET_LAG` in SQL (e.g., '1 minute', '15 minutes', 'DOWNSTREAM')
- **dbt**: Scheduled jobs via dbt Cloud or orchestration tool (e.g., Airflow)

### 2. Dependencies
- **Original**: `DOWNSTREAM` keyword for automatic dependency management
- **dbt**: `ref()` function creates automatic dependency graph

### 3. Incremental Updates
- **Original**: Built into Dynamic Tables (automatic change tracking)
- **dbt**: Explicit incremental logic with `is_incremental()` macro

### 4. Materialization
- **Original**: Always materialized as tables with auto-refresh
- **dbt**: Flexible (view, table, incremental) based on use case

## Scheduling Recommendations

### Option 1: Single Job (Simplest)
Run all models every minute:
```bash
dbt run --select int_transactions_cleaned+ dt_fraud_full+
```
Schedule: `*/1 * * * *` (every minute)

### Option 2: Separate Jobs (Cost-Optimized)

**Job 1: High-frequency (1 minute)**
```bash
dbt run --select int_transactions_cleaned dt_invalid_trans dt_fraud_full
```
Schedule: `*/1 * * * *`

**Job 2: Medium-frequency (5 minutes)**
```bash
dbt run --select int_clients_scd2+
```
Schedule: `*/5 * * * *`

**Job 3: Low-frequency (15 minutes)**
```bash
dbt run --select int_merchants_scd1+
```
Schedule: `*/15 * * * *`

### Option 3: Dependency-Based (dbt Cloud)
Use dbt Cloud's built-in scheduling with job dependencies:
1. Job A: Run transactions model every 1 minute
2. Job B: Run clients model every 5 minutes
3. Job C: Run merchants model every 15 minutes
4. Job D: Run mart models every 1 minute (depends on A, B, C)

## Data Flow

```
S3 Buckets
    ↓
Auto-Ingest Pipes
    ↓
Raw Tables (raw_trans, raw_clients, raw_merch)
    ↓
┌───────────────────────────────────────────────┐
│           dbt Transformation Layer            │
├───────────────────────────────────────────────┤
│ Staging (views)                               │
│   • stg_raw_transactions                      │
│   • stg_raw_clients                           │
│   • stg_raw_merchants                         │
│                                               │
│ Intermediate (incremental/tables)            │
│   • int_transactions_cleaned (1 min)         │
│   • int_clients_scd2 (5 min)                 │
│   • int_merchants_scd1 (15 min)              │
│                                               │
│ Mart (tables)                                │
│   • dt_invalid_trans (1 min)                 │
│   • dt_fraud_full (1 min)                    │
└───────────────────────────────────────────────┘
    ↓
Analytics & Reporting
```

## Running the Migration

### Step 1: Ensure Infrastructure Exists
```sql
-- Run lines 1-196 from sql/dynamic_tables.sql
-- This creates:
-- - Warehouse
-- - Schema
-- - Raw tables
-- - File formats
-- - Stages
-- - Pipes
```

### Step 2: Install dbt Dependencies
```bash
cd /path/to/snowflake-ingestion-patterns
dbt deps  # Installs dbt_utils package
```

### Step 3: Configure Connection
```bash
# Copy template and fill in credentials
cp profiles.yml.template profiles.yml
# OR create ~/.dbt/profiles.yml with connection details
```

### Step 4: Test Connection
```bash
dbt debug
```

### Step 5: Run Initial Load
```bash
# Full refresh - builds all models from scratch
dbt run --full-refresh

# Run tests to validate
dbt test
```

### Step 6: Schedule Jobs
- **dbt Cloud**: Create jobs with schedules as documented above
- **Airflow**: Create DAGs for each schedule frequency
- **Cron**: Use cron jobs to run dbt commands

## Testing the Migration

### Validate Data Matches Original
```sql
-- Compare row counts
SELECT 'dt_transactions' AS table_name, COUNT(*) FROM dt_transactions
UNION ALL
SELECT 'int_transactions_cleaned', COUNT(*) FROM int_transactions_cleaned;

-- Compare specific records
SELECT * FROM dt_transactions WHERE trans_num = 'TEST123'
EXCEPT
SELECT * FROM int_transactions_cleaned WHERE trans_num = 'TEST123';
```

### Run dbt Tests
```bash
# Run all tests
dbt test

# Run tests for specific model
dbt test --select int_transactions_cleaned
```

## Troubleshooting

### Issue: "Source not found"
**Solution**: Ensure raw tables exist in Snowflake first
```bash
# Check if raw tables exist
snowsql -q "SHOW TABLES IN POC_DB.POC2_DYNAMIC LIKE 'raw_%';"
```

### Issue: "Compilation Error"
**Solution**: Ensure profiles.yml is configured correctly
```bash
dbt debug  # Will show connection issues
```

### Issue: Incremental model not updating
**Solution**: Force full refresh
```bash
dbt run --full-refresh --select int_transactions_cleaned
```

### Issue: Test failures
**Solution**: Check data quality in source tables
```sql
-- Check for NULL values
SELECT COUNT(*) FROM raw_trans WHERE trans_num_raw IS NULL;

-- Check for duplicates
SELECT trans_num_raw, COUNT(*) 
FROM raw_trans 
GROUP BY trans_num_raw 
HAVING COUNT(*) > 1;
```

## Migration Checklist

- [ ] Review original SQL implementation
- [ ] Install dbt-snowflake
- [ ] Create profiles.yml with Snowflake credentials
- [ ] Run `dbt deps` to install packages
- [ ] Run `dbt debug` to test connection
- [ ] Run `dbt run --full-refresh` for initial load
- [ ] Run `dbt test` to validate
- [ ] Compare results with original Dynamic Tables
- [ ] Set up scheduled jobs (dbt Cloud/Airflow/cron)
- [ ] Monitor first few runs for issues
- [ ] Deprecate original Dynamic Tables (optional)

## Additional Resources

- [DBT_README.md](./DBT_README.md) - Detailed documentation
- [sql/dynamic_tables.sql](./sql/dynamic_tables.sql) - Original implementation
- [dbt Documentation](https://docs.getdbt.com/)
- [Snowflake Dynamic Tables](https://docs.snowflake.com/en/user-guide/dynamic-tables-about)

---
Last updated: 2026-01-31
