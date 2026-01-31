# dbt Project: Snowflake Dynamic Tables Migration

This dbt project is a migration from the original Snowflake Dynamic Tables implementation in `sql/dynamic_tables.sql`.

## Overview

This project implements a fraud detection data pipeline using dbt to transform raw transaction, client, and merchant data into enriched analytical datasets.

### Original Implementation
- **Source**: `sql/dynamic_tables.sql`
- **Technology**: Snowflake Dynamic Tables with TARGET_LAG configurations
- **Data Loading**: Auto-ingest pipes from S3 with CHANGE_TRACKING enabled

### Key Features Preserved
- ✅ Data validation and type conversion with error handling
- ✅ SCD Type 1 logic for merchants (latest record only)
- ✅ SCD Type 2 logic for clients (full historical tracking)
- ✅ Point-in-time joins for accurate historical analysis
- ✅ Metadata columns for lineage tracking
- ✅ All timestamp conversions and decimal precision (18,2)

## Project Structure

```
models/
├── staging/                    # Raw data passthrough (views)
│   ├── stg_raw_transactions.sql
│   ├── stg_raw_clients.sql
│   └── stg_raw_merchants.sql
│
├── intermediate/               # Cleaned data with transformations (incremental/tables)
│   ├── int_transactions_cleaned.sql   # Data cleaning & validation
│   ├── int_merchants_scd1.sql        # SCD Type 1 logic
│   └── int_clients_scd2.sql          # SCD Type 2 logic
│
├── mart/                       # Final enriched tables
│   ├── dt_invalid_trans.sql          # Invalid transactions
│   └── dt_fraud_full.sql             # Enriched fraud dataset
│
└── schema.yml                  # Sources, models, tests, documentation

macros/
└── generate_hash.sql           # Utility macro for surrogate keys

dbt_project.yml                 # Project configuration
```

## Materialization Strategy

| Layer | Materialization | Rationale |
|-------|----------------|-----------|
| **Staging** | `view` | Lightweight passthrough, no transformations |
| **Intermediate** | `incremental` | Balance between performance and freshness |
| **Mart** | `table` | Query performance for analytical workloads |

## Scheduling Recommendations

The original Dynamic Tables used TARGET_LAG to control refresh frequency. In dbt, these map to scheduled job frequencies:

| Model | Original TARGET_LAG | Recommended Schedule | Business Justification |
|-------|-------------------|---------------------|----------------------|
| `int_transactions_cleaned` | 1 minute | **Every 1 minute** | Time-sensitive fraud detection requires near real-time data |
| `int_clients_scd2` | 5 minutes | **Every 5 minutes** | Balance between cost and new client data freshness |
| `int_merchants_scd1` | 15 minutes | **Every 15 minutes** | Relatively static location data, lower update frequency acceptable |
| `dt_invalid_trans` | DOWNSTREAM | **Every 1 minute** | Depends on transactions, run after upstream completes |
| `dt_fraud_full` | DOWNSTREAM | **Every 1 minute** | Final table, run after all upstream models complete |

### Recommended Job Configuration

#### Option 1: Single Job with Run Frequency
```yaml
# Run all models together every 1 minute
schedule: "*/1 * * * *"
models:
  - int_transactions_cleaned
  - int_clients_scd2
  - int_merchants_scd1
  - dt_invalid_trans
  - dt_fraud_full
```

#### Option 2: Separate Jobs by Cadence (Cost-Optimized)
```yaml
# Job 1: High-frequency models (1 minute)
schedule: "*/1 * * * *"
models:
  - int_transactions_cleaned
  - dt_invalid_trans
  - dt_fraud_full

# Job 2: Medium-frequency models (5 minutes)
schedule: "*/5 * * * *"
models:
  - int_clients_scd2

# Job 3: Low-frequency models (15 minutes)
schedule: "*/15 * * * *"
models:
  - int_merchants_scd1
```

## Configuration

### Database Connection (profiles.yml)

Create a `profiles.yml` file in your `~/.dbt/` directory or project root:

```yaml
snowflake_patterns:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: <your_account>
      user: <your_username>
      password: <your_password>
      role: sysadmin
      database: POC_DB
      warehouse: dynamic_wh
      schema: POC2_DYNAMIC
      threads: 4
      client_session_keep_alive: False
      query_tag: dbt_snowflake_patterns
```

### Variables (dbt_project.yml)

Key variables are defined in `dbt_project.yml`:
- `database`: 'POC_DB'
- `schema`: 'POC2_DYNAMIC'
- `warehouse`: 'dynamic_wh'
- `timezone`: 'Europe/Warsaw'

## Data Pipeline

### 1. Raw Data Layer
Raw tables are loaded via Snowflake pipes from S3:
- `raw_trans` - Transaction data
- `raw_clients` - Client data
- `raw_merch` - Merchant data

All raw tables have `CHANGE_TRACKING = TRUE` enabled for incremental processing.

### 2. Staging Layer
Simple views that provide a clean interface to raw data:
- No transformations
- Just column selection
- Fast and lightweight

### 3. Intermediate Layer

#### Transactions (int_transactions_cleaned)
- Type conversion with `TRY_TO_*` functions
- Data validation with status flags:
  - `VALID` - All conversions successful
  - `INVALID_DATE` - Timestamp conversion failed
  - `INVALID_AMOUNT` - Decimal conversion failed
  - `INVALID_IS_FRAUD` - Boolean conversion failed

#### Merchants (int_merchants_scd1)
**SCD Type 1 - Current State Only**
- Keeps only the latest record per merchant
- Uses `QUALIFY ROW_NUMBER()` for deduplication
- Sorted by: `meta_load_ts DESC, meta_file_row_number DESC`
- No historical tracking (location data only)

#### Clients (int_clients_scd2)
**SCD Type 2 - Full Historical Tracking**
- Maintains complete change history
- Temporal columns:
  - `valid_from` - Start of validity period
  - `valid_to` - End of validity period (NULL = current)
  - `is_current` - Boolean flag for current records
- Uses `LEAD()` window function to create continuous timeline
- Enables point-in-time queries

### 4. Mart Layer

#### Invalid Transactions (dt_invalid_trans)
- Captures all transactions with `validation_status != 'VALID'`
- Separate table for data quality monitoring
- Prevents pollution of analytical datasets

#### Fraud Full (dt_fraud_full)
**Enriched Analytical Dataset**
- Only validated transactions
- **Point-in-time joins** with SCD Type 2 client data:
  ```sql
  AND t.trans_ts >= c.valid_from 
  AND (t.trans_ts < c.valid_to OR c.valid_to IS NULL)
  ```
- Enriched with current merchant location data
- Complete dataset for fraud analysis

## Running the Project

### Initial Setup
```bash
# Install dbt (if not already installed)
pip install dbt-snowflake

# Install dependencies (if using packages)
dbt deps

# Test connection
dbt debug
```

### Development Workflow
```bash
# Run all models
dbt run

# Run specific models
dbt run --select int_transactions_cleaned
dbt run --select mart.*

# Run tests
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

### Production Deployment
```bash
# Full refresh (rebuild from scratch)
dbt run --full-refresh

# Run with tests
dbt build

# Run specific layer
dbt run --select staging.*
dbt run --select intermediate.*
dbt run --select mart.*
```

## Testing

The project includes comprehensive tests in `models/schema.yml`:
- **Uniqueness tests**: `trans_num`, `merchant`
- **Not null tests**: Key columns across all models
- **Accepted values tests**: `validation_status`
- **Relationship tests**: Foreign key constraints
- **Custom tests**: SCD Type 2 uniqueness on (card_num, valid_from)

Run tests:
```bash
dbt test
```

## Data Quality

### Validation Logic
All type conversions use `TRY_TO_*` functions to gracefully handle invalid data:
- `TRY_TO_TIMESTAMP()` - Date/time conversion
- `TRY_TO_DECIMAL()` - Numeric conversion with precision (18,2) for amounts
- `TRY_TO_BOOLEAN()` - Boolean conversion
- `TRY_TO_DATE()` - Date conversion

### Invalid Data Handling
- Invalid records are flagged in `validation_status`
- Separated into `dt_invalid_trans` for analysis
- Valid records flow to `dt_fraud_full`
- No data loss - both valid and invalid records are preserved

## Metadata and Lineage

All models preserve metadata columns:
- `meta_filename` - Source file from S3
- `meta_file_row_number` - Row number in source file
- `meta_load_ts` - Timestamp when data entered Snowflake

This enables:
- Complete data lineage tracking
- Debugging data quality issues
- Audit trail for compliance

## Performance Considerations

### Incremental Models
- `int_transactions_cleaned` - Incremental on `meta_load_ts`
- `int_merchants_scd1` - Incremental with merge logic
- Reduces processing time and warehouse costs

### Clustering
Models are clustered on key columns for query performance:
- `int_transactions_cleaned`: `trans_ts`
- `int_merchants_scd1`: `merchant`
- `int_clients_scd2`: `card_num`, `valid_from`
- `dt_fraud_full`: `trans_ts`, `card_num`

### Query Optimization
- Point-in-time joins use indexed temporal columns
- SCD Type 1 eliminates unnecessary historical data
- Staging layer uses views to avoid data duplication

## Migration Notes

### Differences from Original SQL
1. **Materialization**: Dynamic Tables → dbt materializations (view/incremental/table)
2. **Scheduling**: TARGET_LAG → dbt Cloud/Airflow scheduling
3. **Incremental Logic**: Built-in → dbt incremental models with `is_incremental()`
4. **Dependencies**: DOWNSTREAM → dbt ref() function

### What Was NOT Migrated
This dbt project focuses on the transformation layer. The following remain in SQL:
- Warehouse creation and configuration
- Raw table DDL (with CHANGE_TRACKING)
- File format definitions
- External stages and storage integration
- Auto-ingest pipes

These infrastructure components should be managed separately via:
- Terraform (see `/terraform` directory)
- SQL scripts (see `/sql` directory)
- Snowflake web UI

## Troubleshooting

### Common Issues

**Issue**: Models fail with "source not found"
```bash
# Solution: Ensure raw tables exist in Snowflake
# Run the infrastructure SQL first: sql/dynamic_tables.sql (lines 1-196)
```

**Issue**: Incremental models not updating
```bash
# Solution: Force full refresh
dbt run --full-refresh --select <model_name>
```

**Issue**: SCD Type 2 tests failing
```bash
# Solution: Check for duplicate records in same load batch
# Review deduplication logic in int_clients_scd2.sql
```

## Additional Resources

- [dbt Documentation](https://docs.getdbt.com/)
- [Snowflake Documentation](https://docs.snowflake.com/)
- [Original SQL Implementation](../sql/dynamic_tables.sql)

## Support

For questions or issues with this dbt project, please refer to:
1. Model-specific comments in SQL files
2. Documentation in `models/schema.yml`
3. This README

---

**Last Updated**: 2026-01-31
**dbt Version**: 1.0+
**Snowflake Version**: Compatible with Dynamic Tables feature
