# Snowflake Ingestion Patterns

This repository demonstrates modern data ingestion and transformation patterns in Snowflake, with a focus on real-time fraud detection using both native Snowflake Dynamic Tables and dbt (data build tool).

## Overview

A complete data pipeline implementation showing:
- **Auto-ingest from S3** using Snowflake Pipes
- **Real-time transformations** with Dynamic Tables or dbt
- **SCD (Slowly Changing Dimensions)** Type 1 and Type 2 implementations
- **Point-in-time joins** for historical accuracy
- **Data quality validation** and error handling
- **Fraud detection** use case with enriched analytical datasets

## Repository Structure

```
├── sql/                        # Native Snowflake SQL implementations
│   ├── dynamic_tables.sql      # Dynamic Tables with TARGET_LAG (original)
│   ├── main.sql                # Main pipeline setup
│   └── alerts.sql              # Alerting configuration
│
├── models/                     # dbt transformation models (NEW)
│   ├── staging/                # Raw data views
│   ├── intermediate/           # Cleaned data with SCD logic
│   └── mart/                   # Final enriched tables
│
├── macros/                     # dbt utility macros
├── airflow/                    # Airflow DAG examples for scheduling
├── terraform/                  # Infrastructure as Code
└── scripts/                    # Utility scripts

Documentation:
├── DBT_README.md              # Detailed dbt project documentation
├── MIGRATION_GUIDE.md         # Step-by-step migration guide
└── README.md                  # This file
```

## Two Implementation Approaches

### 1. Snowflake Dynamic Tables (Original)
**Location**: `sql/dynamic_tables.sql`

Native Snowflake feature with automatic refresh based on TARGET_LAG:
```sql
CREATE DYNAMIC TABLE dt_transactions
  TARGET_LAG = '1 minute'
  WAREHOUSE = dynamic_wh
AS
SELECT ... FROM raw_trans;
```

**Pros:**
- Native Snowflake feature, no external tools
- Automatic dependency management with DOWNSTREAM
- Built-in change tracking and incremental refresh
- Simple to set up and maintain

**Cons:**
- Less flexible than dbt
- Limited testing capabilities
- Vendor lock-in
- No modular/reusable transformations

### 2. dbt (Data Build Tool) (NEW)
**Location**: `models/`, `dbt_project.yml`

Modern transformation framework with version control and testing:
```sql
-- models/intermediate/int_transactions_cleaned.sql
{{
  config(
    materialized='incremental',
    unique_key='trans_num'
  )
}}

SELECT ... FROM {{ ref('stg_raw_transactions') }}
```

**Pros:**
- Industry-standard tool, widely adopted
- Built-in testing and documentation
- Version control friendly
- Modular and reusable transformations
- Rich ecosystem of packages and utilities

**Cons:**
- Requires external orchestration (dbt Cloud, Airflow)
- Learning curve for Jinja templating
- More complex setup

## Data Pipeline Architecture

### Source Data (S3)
```
s3://bucket/fraud_transactions/ → Snowpipe → raw_trans
s3://bucket/fraud_clients/      → Snowpipe → raw_clients
s3://bucket/fraud_merchant/     → Snowpipe → raw_merch
```

### Transformation Layers

**Staging** (Views)
- `stg_raw_transactions` - Transaction data passthrough
- `stg_raw_clients` - Client data passthrough
- `stg_raw_merchants` - Merchant data passthrough

**Intermediate** (Incremental/Tables)
- `int_transactions_cleaned` - Type conversion & validation (1 min refresh)
- `int_clients_scd2` - SCD Type 2 for clients (5 min refresh)
- `int_merchants_scd1` - SCD Type 1 for merchants (15 min refresh)

**Mart** (Tables)
- `dt_invalid_trans` - Invalid transactions for analysis
- `dt_fraud_full` - Enriched fraud dataset with point-in-time joins

## Key Features

### 1. Data Validation
Uses Snowflake's `TRY_TO_*` functions for graceful type conversion:
```sql
TRY_TO_TIMESTAMP(trans_date_raw, 'YYYY-MM-DD HH24:MI:SS') AS trans_ts,
TRY_TO_DECIMAL(amount_raw, 18, 2) AS amount,
TRY_TO_BOOLEAN(is_fraud_raw) AS is_fraud
```

### 2. SCD Type 1 (Merchants)
Keeps only the latest record per merchant:
```sql
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY merchant_raw 
    ORDER BY meta_load_ts DESC
) = 1
```

### 3. SCD Type 2 (Clients)
Maintains full historical tracking with temporal columns:
```sql
LEAD(valid_from) OVER (
    PARTITION BY card_num 
    ORDER BY valid_from
) AS valid_to
```

### 4. Point-in-Time Joins
Accurately joins historical client data with transactions:
```sql
LEFT JOIN dt_clients c 
    ON t.card_num = c.card_num
    AND t.trans_ts >= c.valid_from 
    AND (t.trans_ts < c.valid_to OR c.valid_to IS NULL)
```

## Getting Started

### Prerequisites
- Snowflake account with necessary privileges
- S3 bucket for source data (optional)
- Python 3.8+ (for dbt)
- dbt-snowflake package

### Option 1: Snowflake Dynamic Tables

1. **Set up infrastructure**:
   ```sql
   -- Run sql/dynamic_tables.sql (lines 1-196)
   -- Creates warehouse, schema, raw tables, stages, and pipes
   ```

2. **Create Dynamic Tables**:
   ```sql
   -- Run sql/dynamic_tables.sql (lines 200-395)
   -- Creates 5 dynamic tables with different TARGET_LAG
   ```

3. **Monitor refreshes**:
   ```sql
   SELECT * FROM INFORMATION_SCHEMA.DYNAMIC_TABLES;
   ```

### Option 2: dbt Implementation

1. **Install dbt**:
   ```bash
   pip install dbt-snowflake
   ```

2. **Configure connection**:
   ```bash
   cp profiles.yml.template profiles.yml
   # Edit profiles.yml with your Snowflake credentials
   ```

3. **Install dependencies**:
   ```bash
   dbt deps
   ```

4. **Test connection**:
   ```bash
   dbt debug
   ```

5. **Run models**:
   ```bash
   # Full refresh (first run)
   dbt run --full-refresh
   
   # Run tests
   dbt test
   
   # Generate documentation
   dbt docs generate
   dbt docs serve
   ```

6. **Schedule jobs**:
   - Use `run_dbt.sh` for cron scheduling
   - Or use Airflow DAGs in `airflow/` directory
   - Or use dbt Cloud for managed scheduling

## Scheduling Recommendations

| Model | Frequency | Justification |
|-------|-----------|---------------|
| Transactions | 1 minute | Time-sensitive fraud detection |
| Clients | 5 minutes | Balance cost and freshness for new clients |
| Merchants | 15 minutes | Relatively static location data |
| Mart tables | 1 minute | Run after upstream models complete |

## Documentation

- **[DBT_README.md](./DBT_README.md)** - Complete dbt project documentation
  - Data pipeline details
  - Model descriptions
  - Testing strategy
  - Performance considerations

- **[MIGRATION_GUIDE.md](./MIGRATION_GUIDE.md)** - Migration instructions
  - SQL to dbt mapping
  - Step-by-step guide
  - Troubleshooting tips
  - Validation queries

- **[airflow/README.md](./airflow/README.md)** - Airflow orchestration
  - DAG setup
  - Scheduling patterns
  - Monitoring

## Testing

### dbt Tests
```bash
# Run all tests
dbt test

# Test specific model
dbt test --select int_transactions_cleaned

# Test with fail-fast
dbt test --fail-fast
```

### Test Coverage
- Uniqueness: `trans_num`, `merchant`
- Not null: Key columns across all models
- Accepted values: `validation_status`
- SCD Type 2: Unique combination of `(card_num, valid_from)`

## Monitoring & Observability

### Key Metrics
- Model execution time
- Row counts and data freshness
- Test pass/fail rates
- Warehouse credit usage
- Data quality scores

### Lineage Tracking
All models preserve metadata:
- `meta_filename` - Source file
- `meta_file_row_number` - Row in source
- `meta_load_ts` - Load timestamp

## Performance Optimization

### Clustering
Models are clustered on key columns:
```sql
cluster_by=['trans_ts', 'card_num']
```

### Incremental Models
```sql
{% if is_incremental() %}
    WHERE meta_load_ts > (SELECT MAX(meta_load_ts) FROM {{ this }})
{% endif %}
```

## Use Cases

This pattern is applicable for:
- **Fraud Detection** - Real-time transaction monitoring
- **Customer 360** - Historical customer views with SCD Type 2
- **Product Analytics** - User behavior tracking
- **Financial Reporting** - Accurate point-in-time snapshots
- **IoT Sensor Data** - Continuous device monitoring

## Contributing

Contributions are welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is licensed under the MIT License.

## Support

For questions or issues:
1. Check the documentation files
2. Review model-specific comments in SQL files
3. Open an issue on GitHub

## Resources

- [Snowflake Dynamic Tables](https://docs.snowflake.com/en/user-guide/dynamic-tables-about)
- [dbt Documentation](https://docs.getdbt.com/)
- [Snowflake Snowpipe](https://docs.snowflake.com/en/user-guide/data-load-snowpipe)
- [SCD Best Practices](https://en.wikipedia.org/wiki/Slowly_changing_dimension)

---

**Last Updated**: 2026-01-31  
**Snowflake Version**: Compatible with Dynamic Tables feature  
**dbt Version**: 1.0+
