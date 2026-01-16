#!/usr/bin/env bash
set -euo pipefail

STATES=(
  # alabama
  # alaska
  # arizona
  # arkansas
  # california
  # colorado
  connecticut
  delaware
  district-of-columbia
  florida
  georgia
  hawaii
  idaho
  illinois
  indiana
  iowa
  kansas
  kentucky
  louisiana
  maine
  maryland
  massachusetts
  michigan
  minnesota
  mississippi
  missouri
  montana
  nebraska
  nevada
  new-hampshire
  new-jersey
  new-mexico
  new-york
  north-carolina
  north-dakota
  ohio
  oklahoma
  oregon
  pennsylvania
  rhode-island
  south-carolina
  south-dakota
  tennessee
  texas
  utah
  vermont
  virginia
  washington
  west-virginia
  wisconsin
  wyoming
)

for state in "${STATES[@]}"; do
  echo "================================================"
  echo "Running state: $state"
  echo "================================================"

  python scripts/pipeline_get_datacenter.py \
    --states "$state" \
    --output "data/processed_data/datacenters_${state}.csv"

  echo "Done: $state"
  echo "Sleeping 300 seconds..."
  sleep 300
done
