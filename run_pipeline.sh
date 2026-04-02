#!/bin/bash
set -e

echo "Starting Full CSE Pipeline..."

# 1. Run sync logic
echo "[Step 1/2] Syncing Cloud to Local..."
python -m src.sync.sync_cloud_to_local

# 2. Run the dbt transformation
echo "[Step 2/2] Running dbt Transformation..."
cd cse_transform
dbt run --profiles-dir .

echo "Pipeline Success..!"