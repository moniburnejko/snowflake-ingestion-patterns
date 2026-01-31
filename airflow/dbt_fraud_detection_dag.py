"""
Airflow DAG for dbt Snowflake Dynamic Tables Migration

This DAG demonstrates how to schedule dbt models with different frequencies
matching the original Dynamic Tables TARGET_LAG configurations.

Schedule:
- High-frequency (1 minute): transactions, invalid_trans, fraud_full
- Medium-frequency (5 minutes): clients
- Low-frequency (15 minutes): merchants

Requirements:
    pip install apache-airflow-providers-dbt-cloud
    OR
    pip install dbt-core dbt-snowflake
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

# Default arguments for all tasks
default_args = {
    'owner': 'data-eng',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=10),
}

# DAG 1: High-frequency models (every 1 minute)
with DAG(
    'dbt_fraud_detection_high_freq',
    default_args=default_args,
    description='High-frequency dbt models (1 minute)',
    schedule_interval='*/1 * * * *',  # Every minute
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['dbt', 'fraud', 'high-frequency'],
) as dag_high_freq:
    
    # Run transactions model (incremental)
    run_transactions = BashOperator(
        task_id='run_transactions_cleaned',
        bash_command='cd {{ params.dbt_project_dir }} && dbt run --select int_transactions_cleaned',
        params={'dbt_project_dir': '/path/to/snowflake-ingestion-patterns'},
    )
    
    # Run mart models (dependent on transactions)
    with TaskGroup(group_id='run_mart_models') as mart_models:
        run_invalid_trans = BashOperator(
            task_id='run_invalid_trans',
            bash_command='cd {{ params.dbt_project_dir }} && dbt run --select dt_invalid_trans',
            params={'dbt_project_dir': '/path/to/snowflake-ingestion-patterns'},
        )
        
        run_fraud_full = BashOperator(
            task_id='run_fraud_full',
            bash_command='cd {{ params.dbt_project_dir }} && dbt run --select dt_fraud_full',
            params={'dbt_project_dir': '/path/to/snowflake-ingestion-patterns'},
        )
    
    # Test the models
    test_models = BashOperator(
        task_id='test_models',
        bash_command='cd {{ params.dbt_project_dir }} && dbt test --select int_transactions_cleaned dt_invalid_trans dt_fraud_full',
        params={'dbt_project_dir': '/path/to/snowflake-ingestion-patterns'},
    )
    
    # Define dependencies
    run_transactions >> mart_models >> test_models


# DAG 2: Medium-frequency models (every 5 minutes)
with DAG(
    'dbt_fraud_detection_medium_freq',
    default_args=default_args,
    description='Medium-frequency dbt models (5 minutes)',
    schedule_interval='*/5 * * * *',  # Every 5 minutes
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['dbt', 'fraud', 'medium-frequency'],
) as dag_medium_freq:
    
    # Run clients model with SCD Type 2
    run_clients = BashOperator(
        task_id='run_clients_scd2',
        bash_command='cd {{ params.dbt_project_dir }} && dbt run --select int_clients_scd2',
        params={'dbt_project_dir': '/path/to/snowflake-ingestion-patterns'},
    )
    
    # Test the model
    test_clients = BashOperator(
        task_id='test_clients',
        bash_command='cd {{ params.dbt_project_dir }} && dbt test --select int_clients_scd2',
        params={'dbt_project_dir': '/path/to/snowflake-ingestion-patterns'},
    )
    
    run_clients >> test_clients


# DAG 3: Low-frequency models (every 15 minutes)
with DAG(
    'dbt_fraud_detection_low_freq',
    default_args=default_args,
    description='Low-frequency dbt models (15 minutes)',
    schedule_interval='*/15 * * * *',  # Every 15 minutes
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['dbt', 'fraud', 'low-frequency'],
) as dag_low_freq:
    
    # Run merchants model with SCD Type 1
    run_merchants = BashOperator(
        task_id='run_merchants_scd1',
        bash_command='cd {{ params.dbt_project_dir }} && dbt run --select int_merchants_scd1',
        params={'dbt_project_dir': '/path/to/snowflake-ingestion-patterns'},
    )
    
    # Test the model
    test_merchants = BashOperator(
        task_id='test_merchants',
        bash_command='cd {{ params.dbt_project_dir }} && dbt test --select int_merchants_scd1',
        params={'dbt_project_dir': '/path/to/snowflake-ingestion-patterns'},
    )
    
    run_merchants >> test_merchants


# DAG 4: Full refresh (daily)
with DAG(
    'dbt_fraud_detection_full_refresh',
    default_args=default_args,
    description='Full refresh of all dbt models (daily)',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['dbt', 'fraud', 'full-refresh'],
) as dag_full_refresh:
    
    # Full refresh all models
    full_refresh = BashOperator(
        task_id='full_refresh_all',
        bash_command='cd {{ params.dbt_project_dir }} && dbt run --full-refresh',
        params={'dbt_project_dir': '/path/to/snowflake-ingestion-patterns'},
    )
    
    # Run all tests
    test_all = BashOperator(
        task_id='test_all',
        bash_command='cd {{ params.dbt_project_dir }} && dbt test',
        params={'dbt_project_dir': '/path/to/snowflake-ingestion-patterns'},
    )
    
    # Generate documentation
    generate_docs = BashOperator(
        task_id='generate_docs',
        bash_command='cd {{ params.dbt_project_dir }} && dbt docs generate',
        params={'dbt_project_dir': '/path/to/snowflake-ingestion-patterns'},
    )
    
    full_refresh >> test_all >> generate_docs


# Alternative: Single DAG with sensors (more complex but efficient)
"""
This alternative shows how to use ExternalTaskSensor to coordinate
different frequencies in a single DAG structure.

from airflow.sensors.external_task import ExternalTaskSensor

with DAG('dbt_fraud_detection_coordinated', ...) as dag:
    
    # High frequency: every minute
    high_freq_sensor = ExternalTaskSensor(
        task_id='wait_for_high_freq',
        external_dag_id='dbt_fraud_detection_high_freq',
        external_task_id='test_models',
        mode='reschedule',
        timeout=120,
    )
    
    # Trigger mart refresh after all upstream complete
    refresh_marts = BashOperator(
        task_id='refresh_marts',
        bash_command='...',
    )
    
    high_freq_sensor >> refresh_marts
"""
