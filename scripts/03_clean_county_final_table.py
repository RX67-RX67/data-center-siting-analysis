"""
Pipeline: Clean county_final_table: drop rows without state/county; drop U.S. overseas territories.

Input: data_revealed/03_tables/county_final_table.csv

Steps:
- Drop rows where state or county is missing or empty.
- Drop rows where state is a U.S. overseas territory:
  Puerto Rico, Guam, U.S. Virgin Islands, American Samoa, Northern Mariana Islands.

Output: data_revealed/03_tables/county_final_table_clean.csv
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

DEFAULT_INPUT = "data_revealed/03_tables/county_final_table.csv"
DEFAULT_OUTPUT = "data_revealed/03_tables/county_final_table_clean.csv"

STATE_COL = "state"
COUNTY_COL = "county"

# U.S. overseas territories to exclude (exact match, stripped)
US_TERRITORIES = {
    "Puerto Rico",
    "Guam",
    "U.S. Virgin Islands",
    "American Samoa",
    "Northern Mariana Islands",
}


def main():
    parser = argparse.ArgumentParser(
        description="Clean county_final_table: drop missing state/county and U.S. territories."
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

    base = Path(args.base_path) if args.base_path else project_root
    input_path = base / args.input
    output_path = base / args.output

    if not input_path.exists():
        print(f"Error: input not found: {input_path}", file=sys.stderr)
        sys.exit(1)

    df = pd.read_csv(input_path)
    for col in (STATE_COL, COUNTY_COL):
        if col not in df.columns:
            print(f"Error: column '{col}' not found in input", file=sys.stderr)
            sys.exit(1)
    n_start = len(df)

    # 1) Drop rows without state or county name
    df[STATE_COL] = df[STATE_COL].astype(str).str.strip()
    df[COUNTY_COL] = df[COUNTY_COL].astype(str).str.strip()
    mask_missing = (df[STATE_COL] == "") | (df[STATE_COL].str.lower() == "nan") | (df[COUNTY_COL] == "") | (df[COUNTY_COL].str.lower() == "nan")
    df = df[~mask_missing].copy()
    n_after_missing = len(df)
    logger.info(f"Dropped {n_start - n_after_missing} rows with missing or empty state/county")

    # 2) Drop U.S. overseas territories
    mask_territory = df[STATE_COL].isin(US_TERRITORIES)
    n_territory = mask_territory.sum()
    df = df[~mask_territory].copy()
    if n_territory:
        logger.info(f"Dropped {n_territory} rows from U.S. territories: {US_TERRITORIES}")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"Saved {len(df)} rows to {output_path}")


if __name__ == "__main__":
    main()
    sys.exit(0)
