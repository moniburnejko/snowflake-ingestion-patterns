#!/bin/bash

# dbt Run Script for Snowflake Dynamic Tables Migration
# This script demonstrates how to run dbt models on different schedules

set -e  # Exit on error

# Function to run dbt with error handling
run_dbt() {
    local command=$1
    local description=$2
    
    echo "================================================"
    echo "Running: $description"
    echo "Command: dbt $command"
    echo "================================================"
    
    if dbt $command; then
        echo "✓ Success: $description"
    else
        echo "✗ Failed: $description"
        exit 1
    fi
    echo ""
}

# Parse command line arguments
SCHEDULE="${1:-all}"

case $SCHEDULE in
    "high-freq"|"1min")
        # High-frequency models (every 1 minute)
        # Includes transactions, invalid trans, and fraud full
        echo "Running high-frequency models (1 minute schedule)..."
        run_dbt "run --select int_transactions_cleaned dt_invalid_trans dt_fraud_full" "High-frequency models"
        ;;
        
    "medium-freq"|"5min")
        # Medium-frequency models (every 5 minutes)
        # Includes clients with SCD Type 2
        echo "Running medium-frequency models (5 minute schedule)..."
        run_dbt "run --select int_clients_scd2" "Medium-frequency models"
        ;;
        
    "low-freq"|"15min")
        # Low-frequency models (every 15 minutes)
        # Includes merchants with SCD Type 1
        echo "Running low-frequency models (15 minute schedule)..."
        run_dbt "run --select int_merchants_scd1" "Low-frequency models"
        ;;
        
    "staging")
        # Run only staging models
        echo "Running staging models..."
        run_dbt "run --select staging.*" "Staging models"
        ;;
        
    "intermediate")
        # Run only intermediate models
        echo "Running intermediate models..."
        run_dbt "run --select intermediate.*" "Intermediate models"
        ;;
        
    "mart")
        # Run only mart models
        echo "Running mart models..."
        run_dbt "run --select mart.*" "Mart models"
        ;;
        
    "test")
        # Run tests only
        echo "Running tests..."
        run_dbt "test" "All tests"
        ;;
        
    "full-refresh")
        # Full refresh of all models
        echo "Running full refresh..."
        run_dbt "run --full-refresh" "Full refresh"
        ;;
        
    "build")
        # Run and test all models
        echo "Building all models with tests..."
        run_dbt "build" "Build (run + test)"
        ;;
        
    "all"|*)
        # Run all models (default)
        echo "Running all models..."
        run_dbt "run" "All models"
        ;;
esac

echo "================================================"
echo "dbt run completed successfully!"
echo "================================================"

# Optional: Generate documentation after successful run
read -p "Generate documentation? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Generating documentation..."
    dbt docs generate
    echo "✓ Documentation generated"
    echo "Run 'dbt docs serve' to view"
fi
