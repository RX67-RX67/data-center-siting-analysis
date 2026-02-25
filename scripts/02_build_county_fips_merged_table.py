"""
Pipeline: Merge county_fips_table, county_from_zip_table_elec_price, county_from_zip_table_num_dc.

Inputs (under data_revealed/01_tables/):
- county_fips_table.csv
- county_from_zip_table_elec_price.csv
- county_from_zip_table_num_dc.csv

- Common key: county_fips (string, 5-digit; normalized on read and in output).
- Drop state and county/county_name in each table before merging.
- Merge method: outer.
- Clean: drop rows where county_fips is missing or empty.

Output: data_revealed/02_tables/county_fips_merged_table.csv
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

INPUT_DIR = "data_revealed/01_tables"
DEFAULT_OUTPUT = "data_revealed/02_tables/county_fips_merged_table.csv"
KEY_COL = "county_fips"

# Columns to drop from each table (state and county name variants)
DROP_COLS = {"state", "county", "county_name"}


def _normalize_fips(series: pd.Series) -> pd.Series:
    """Normalize FIPS to 5-digit string; leave missing as empty."""
    s = series.astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
    s = s.str.replace("(?i)^nan$", "", regex=True)
    mask = s == ""
    return s.where(mask, s.str.zfill(5)).astype("string")


def _load_and_prep(path: Path, name: str) -> pd.DataFrame:
    """Load CSV, normalize county_fips, drop state/county name columns."""
    df = pd.read_csv(path, dtype={KEY_COL: str})
    if KEY_COL not in df.columns:
        raise ValueError(f"{name} missing column '{KEY_COL}'")
    df[KEY_COL] = _normalize_fips(df[KEY_COL])
    to_drop = [c for c in DROP_COLS if c in df.columns]
    if to_drop:
        df = df.drop(columns=to_drop)
    logger.info(f"{name}: {len(df)} rows, columns: {list(df.columns)}")
    return df


def main():
    parser = argparse.ArgumentParser(
        description="Merge county_fips_table, elec_price, num_dc on county_fips (outer); drop state/county name."
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

    paths = {
        "county_fips": input_dir / "county_fips_table.csv",
        "elec_price": input_dir / "county_from_zip_table_elec_price.csv",
        "num_dc": input_dir / "county_from_zip_table_num_dc.csv",
    }
    for name, p in paths.items():
        if not p.exists():
            print(f"Error: not found {p}", file=sys.stderr)
            sys.exit(1)

    dfs = []
    for name, p in paths.items():
        dfs.append(_load_and_prep(p, name))

    out = dfs[0]
    for df in dfs[1:]:
        overlap = [c for c in df.columns if c != KEY_COL and c in out.columns]
        if overlap:
            df = df.rename(columns={c: f"{c}_right" for c in overlap})
        out = out.merge(df, on=KEY_COL, how="outer")
        logger.info(f"After merge: {len(out)} rows")

    out[KEY_COL] = _normalize_fips(out[KEY_COL])
    # Clean: drop rows without county_fips
    n_before = len(out)
    out = out[out[KEY_COL].astype(str).str.strip() != ""].copy()
    n_dropped = n_before - len(out)
    if n_dropped:
        logger.info(f"Dropped {n_dropped} rows with missing or empty {KEY_COL}")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(output_path, index=False)
    print(f"Saved {len(out)} rows to {output_path}")


if __name__ == "__main__":
    main()
    sys.exit(0)
