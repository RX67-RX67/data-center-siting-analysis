"""
Pipeline: Merge county_fips_merged_table and county_with_policy_table_clean.

Inputs (under data_revealed/02_tables/):
- county_fips_merged_table.csv
- county_with_policy_table_clean.csv

- Common key: county_fips (string, 5-digit; preserved on read and output).
- Merge method: outer.
- Keep all features (including state and county names).

Output: data_revealed/03_tables/county_final_table.csv
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

INPUT_DIR = "data_revealed/02_tables"
OUTPUT_DIR = "data_revealed/03_tables"
DEFAULT_OUTPUT = "data_revealed/03_tables/county_final_table.csv"
KEY_COL = "county_fips"


def _normalize_fips(series: pd.Series) -> pd.Series:
    """Normalize FIPS to 5-digit string; leave missing as empty."""
    s = series.astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
    s = s.str.replace("(?i)^nan$", "", regex=True)
    mask = s == ""
    return s.where(mask, s.str.zfill(5)).astype("string")


def main():
    parser = argparse.ArgumentParser(
        description="Merge county_fips_merged_table and county_with_policy_table_clean on county_fips (outer); keep all features."
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
    input_dir = base / INPUT_DIR
    output_path = base / args.output

    path_fips = input_dir / "county_fips_merged_table.csv"
    path_policy = input_dir / "county_with_policy_table_clean.csv"
    for p in (path_fips, path_policy):
        if not p.exists():
            print(f"Error: not found {p}", file=sys.stderr)
            sys.exit(1)

    df_fips = pd.read_csv(path_fips, dtype={KEY_COL: str})
    df_fips[KEY_COL] = _normalize_fips(df_fips[KEY_COL])
    logger.info(f"county_fips_merged_table: {len(df_fips)} rows")

    df_policy = pd.read_csv(path_policy, dtype={KEY_COL: str})
    df_policy[KEY_COL] = _normalize_fips(df_policy[KEY_COL])
    logger.info(f"county_with_policy_table_clean: {len(df_policy)} rows")

    # Resolve overlapping non-key columns to avoid duplicate column names
    overlap = [c for c in df_policy.columns if c != KEY_COL and c in df_fips.columns]
    if overlap:
        df_policy = df_policy.rename(columns={c: f"{c}_policy" for c in overlap})

    out = df_fips.merge(df_policy, on=KEY_COL, how="outer")
    out[KEY_COL] = _normalize_fips(out[KEY_COL])
    logger.info(f"Merged: {len(out)} rows, {len(out.columns)} columns")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(output_path, index=False)
    print(f"Saved {len(out)} rows to {output_path}")


if __name__ == "__main__":
    main()
    sys.exit(0)
