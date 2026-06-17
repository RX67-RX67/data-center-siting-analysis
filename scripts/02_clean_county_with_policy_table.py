"""
Pipeline: Clean county-with-policy table: drop rows without FIPS, fill policy defaults.

Input: data_revealed/02_tables/county_with_policy_table.csv

Steps:
- Drop rows where county_fips is missing or empty.
- Fill missing has_policy_signal with 0 and missing policy_direction_score with 0.

Output: data_revealed/02_tables/county_with_policy_table_clean.csv
"""

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_INPUT = "data_revealed/02_tables/county_with_policy_table.csv"
DEFAULT_OUTPUT = "data_revealed/02_tables/county_with_policy_table_clean.csv"

FIPS_COL = "county_fips"
HAS_POLICY_COL = "has_policy_signal"
POLICY_SCORE_COL = "policy_direction_score"


def main():
    parser = argparse.ArgumentParser(
        description="Clean county-with-policy table: drop rows without FIPS, fill policy columns with 0."
    )
    parser.add_argument(
        "--input",
        type=str,
        default=DEFAULT_INPUT,
        help=f"Input CSV (default: {DEFAULT_INPUT})",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=DEFAULT_OUTPUT,
        help=f"Output CSV (default: {DEFAULT_OUTPUT})",
    )
    parser.add_argument(
        "--base-path",
        type=str,
        default=None,
        help="Project root (default: script parent)",
    )
    args = parser.parse_args()

    base_path = Path(args.base_path) if args.base_path else project_root
    input_path = base_path / args.input
    output_path = base_path / args.output

    if not input_path.exists():
        print(f"Error: input not found: {input_path}", file=sys.stderr)
        sys.exit(1)

    df = pd.read_csv(input_path)
    n_before = len(df)

    # 1) Drop rows without FIPS code
    if FIPS_COL not in df.columns:
        print(f"Error: column '{FIPS_COL}' not found", file=sys.stderr)
        sys.exit(1)
    fips_str = df[FIPS_COL].astype(str).str.strip()
    mask_missing = (fips_str == "") | (fips_str.str.lower() == "nan")
    df = df[~mask_missing].copy()
    n_dropped = n_before - len(df)
    logger.info(f"Dropped {n_dropped} rows with missing or empty {FIPS_COL}")

    # 2) Fill missing has_policy_signal with 0, policy_direction_score with 0
    if HAS_POLICY_COL in df.columns:
        df[HAS_POLICY_COL] = pd.to_numeric(df[HAS_POLICY_COL], errors="coerce").fillna(0)
    if POLICY_SCORE_COL in df.columns:
        df[POLICY_SCORE_COL] = pd.to_numeric(df[POLICY_SCORE_COL], errors="coerce").fillna(0)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"Saved {len(df)} rows to {output_path}")


if __name__ == "__main__":
    main()
    sys.exit(0)
