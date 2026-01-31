# Airflow DAGs for dbt Models

This directory contains sample Airflow DAGs for scheduling the dbt models.

## Files

- `dbt_fraud_detection_dag.py` - Main DAG file with multiple scheduling strategies

## DAGs Included

### 1. dbt_fraud_detection_high_freq
- **Schedule**: Every 1 minute (`*/1 * * * *`)
- **Models**: int_transactions_cleaned, dt_invalid_trans, dt_fraud_full
- **Purpose**: Time-sensitive fraud detection models

### 2. dbt_fraud_detection_medium_freq
- **Schedule**: Every 5 minutes (`*/5 * * * *`)
- **Models**: int_clients_scd2
- **Purpose**: Client data with SCD Type 2

### 3. dbt_fraud_detection_low_freq
- **Schedule**: Every 15 minutes (`*/15 * * * *`)
- **Models**: int_merchants_scd1
- **Purpose**: Merchant data with SCD Type 1

### 4. dbt_fraud_detection_full_refresh
- **Schedule**: Daily at 2 AM (`0 2 * * *`)
- **Models**: All models (full refresh)
- **Purpose**: Daily maintenance and documentation generation

## Setup

### 1. Install Airflow
```bash
pip install apache-airflow apache-airflow-providers-dbt-cloud
# OR if using local dbt
pip install apache-airflow dbt-core dbt-snowflake
```

### 2. Configure Airflow
```bash
# Initialize Airflow database
airflow db init

# Create admin user
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
```

### 3. Copy DAG to Airflow
```bash
# Copy DAG file to Airflow DAGs folder
cp airflow/dbt_fraud_detection_dag.py ~/airflow/dags/

# Update dbt_project_dir in the DAG file
sed -i 's|/path/to/snowflake-ingestion-patterns|'"$(pwd)"'|g' ~/airflow/dags/dbt_fraud_detection_dag.py
```

### 4. Start Airflow
```bash
# Start webserver (in one terminal)
airflow webserver --port 8080

# Start scheduler (in another terminal)
airflow scheduler
```

### 5. Access Airflow UI
Open http://localhost:8080 in your browser

## Usage

### Enable DAGs
In Airflow UI:
1. Navigate to DAGs page
2. Toggle the switch to enable each DAG
3. Monitor execution in the Grid/Graph view

### Manual Trigger
```bash
# Trigger a specific DAG
airflow dags trigger dbt_fraud_detection_high_freq

# Trigger with specific date
airflow dags trigger dbt_fraud_detection_high_freq --exec-date 2026-01-31
```

### View Logs
```bash
# List task instances
airflow tasks list dbt_fraud_detection_high_freq

# View task logs
airflow tasks logs dbt_fraud_detection_high_freq run_transactions_cleaned 2026-01-31
```

## Customization

### Change Schedule
Edit the `schedule_interval` parameter:
```python
schedule_interval='*/1 * * * *',  # Every minute
schedule_interval='*/5 * * * *',  # Every 5 minutes
schedule_interval='0 * * * *',    # Hourly
schedule_interval='0 0 * * *',    # Daily at midnight
```

### Change dbt Project Path
Update the `params` in each BashOperator:
```python
params={'dbt_project_dir': '/your/path/to/snowflake-ingestion-patterns'},
```

### Add Email Notifications
Update `default_args`:
```python
default_args = {
    ...
    'email': ['your-email@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}
```

## Alternative: dbt Cloud Integration

If using dbt Cloud, you can trigger jobs via API instead:

```python
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator

trigger_job = DbtCloudRunJobOperator(
    task_id='trigger_dbt_cloud_job',
    job_id=12345,  # Your dbt Cloud job ID
    check_interval=60,
    timeout=3600,
)
```

## Monitoring

### Key Metrics to Monitor
- DAG run duration
- Task success/failure rates
- dbt model execution times
- Warehouse credit usage

### Set Up Alerts
Configure Airflow to send alerts on:
- Task failures
- DAG execution time > threshold
- SLA misses

## Troubleshooting

### DAG Not Appearing in UI
```bash
# Check DAG parsing errors
airflow dags list-import-errors

# Test DAG file
python ~/airflow/dags/dbt_fraud_detection_dag.py
```

### Task Failures
1. Check task logs in Airflow UI
2. Verify dbt profiles.yml configuration
3. Test dbt commands manually:
   ```bash
   cd /path/to/project
   dbt run --select int_transactions_cleaned
   ```

### Connection Issues
1. Verify Snowflake credentials in profiles.yml
2. Test connection: `dbt debug`
3. Check Airflow connections: `airflow connections list`

## Best Practices

1. **Use Connection Pooling**: Configure Snowflake connection pool in profiles.yml
2. **Set Timeouts**: Add execution_timeout to prevent hanging tasks
3. **Monitor Costs**: Track Snowflake warehouse usage per DAG
4. **Use Task Groups**: Organize related tasks for better visualization
5. **Implement SLAs**: Set SLA deadlines for critical models
6. **Tag DAGs**: Use meaningful tags for easy filtering
7. **Version Control**: Keep DAG files in git repository

## Resources

- [Airflow Documentation](https://airflow.apache.org/docs/)
- [dbt + Airflow Best Practices](https://docs.getdbt.com/guides/orchestration/airflow-and-dbt-cloud/1-airflow-and-dbt-cloud)
- [Astronomer Cosmos](https://astronomer.github.io/astronomer-cosmos/) - dbt + Airflow integration
