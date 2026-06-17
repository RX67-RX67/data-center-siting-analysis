"""
Pipeline: Build county-level table with policy signals.

Inputs (under data_revealed/01_tables/):
- county_table.csv            (core county-level features)
- county_policy_signal.csv    (LLM-derived policy_direction_score, has_policy_signal)

Both tables are merged with an outer join on (state, county). The policy table
uses `mentioned_state` and `mentioned_county` as keys, which are renamed to
`state` and `county` before merging.

Output:
- data_revealed/02_tables/county_with_policy_table.csv
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
DEFAULT_OUTPUT = "data_revealed/02_tables/county_with_policy_table.csv"


def _normalize_fips_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure any column with 'fips' in the name is string dtype, 5-digit (left zero-padded)."""
    for col in df.columns:
        if "fips" not in col.lower():
            continue
        s = df[col].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
        s = s.str.replace("(?i)^nan$", "", regex=True)
        # Zfill only non-empty values; leave missing as empty string
        mask_empty = s == ""
        df[col] = s.where(mask_empty, s.str.zfill(5)).astype("string")
        logger.info(f"Normalized {col} to 5-digit string")
    return df


def _load_county_table(base_path: Path) -> pd.DataFrame:
    path = base_path / INPUT_DIR / "county_table.csv"
    if not path.exists():
        raise FileNotFoundError(f"county_table not found: {path}")
    # Read keys and FIPS as string so leading zeros are not lost
    df = pd.read_csv(path, dtype={"state": str, "county": str, "county_fips": str})
    # Normalize keys
    for col in ("state", "county"):
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
        else:
            raise ValueError(f"county_table is missing key column '{col}'")
    _normalize_fips_columns(df)
    logger.info(f"county_table: {len(df)} rows, columns: {list(df.columns)}")
    return df


def _load_policy_table(base_path: Path) -> pd.DataFrame:
    path = base_path / INPUT_DIR / "county_policy_signal.csv"
    if not path.exists():
        raise FileNotFoundError(f"county_policy_signal not found: {path}")
    df = pd.read_csv(path)

    # Rename keys to match county_table
    rename_map = {}
    if "mentioned_state" in df.columns:
        rename_map["mentioned_state"] = "state"
    if "mentioned_county" in df.columns:
        rename_map["mentioned_county"] = "county"
    df = df.rename(columns=rename_map)

    # Normalize keys
    for col in ("state", "county"):
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
        else:
            raise ValueError(f"county_policy_signal is missing key column '{col}' after renaming")

    logger.info(f"county_policy_signal: {len(df)} rows, columns: {list(df.columns)}")
    return df


def build_county_with_policy_table(base_path: Path) -> pd.DataFrame:
    county_df = _load_county_table(base_path)
    policy_df = _load_policy_table(base_path)

    key_cols = ["state", "county"]
    logger.info("Merging county_table with county_policy_signal on (state, county)")
    out = county_df.merge(policy_df, on=key_cols, how="outer")
    # Ensure FIPS columns in output are string, 5-digit
    _normalize_fips_columns(out)
    logger.info(f"Final county_with_policy table: {len(out)} rows, columns: {len(out.columns)}")
    return out


def main():
    parser = argparse.ArgumentParser(
        description="Build county-level table with policy signals from county_table and county_policy_signal."
    )
    parser.add_argument(
        "--output",
        type=str,
        default=DEFAULT_OUTPUT,
        help=f"Output CSV path (default: {DEFAULT_OUTPUT})",
    )
    parser.add_argument(
        "--base-path",
        type=str,
        default=None,
        help="Project root (default: script parent)",
    )
    args = parser.parse_args()

    base_path = Path(args.base_path) if args.base_path else project_root
    output_path = base_path / args.output
    output_path.parent.mkdir(parents=True, exist_ok=True)

    print("Building county_with_policy_table from county_table and county_policy_signal...")
    df = build_county_with_policy_table(base_path)
    df.to_csv(output_path, index=False)
    print(f"Saved {len(df)} rows to {output_path}")


if __name__ == "__main__":
    main()
    sys.exit(0)

