"""
Pipeline: Build county-grain table of allocated data center counts from ZIP-grain table.

Joins zip_table_num_dc and reference_table on zip_code (outer). Allocates num_datacenters
to counties by business_ratio and sums by county:
  county_num_datacenters = sum(num_datacenters × business_ratio)
Result may be fractional (expected count under uncertain spatial assignment).
Writes county_from_zip_table_num_dc.csv under data/processed_data/data_build/ by default.

See document/plan_zip_to_county_num_dc.md for the transformation logic.
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

_verbose = True

# Default paths (relative to project root)
ZIP_TABLE_PATH = "data/processed_data/data_build/zip_table_num_dc.csv"
REFERENCE_TABLE_PATH = "data/processed_data/data_build/reference_table.csv"
DEFAULT_OUTPUT_PATH = "data/processed_data/data_build/county_from_zip_table_num_dc.csv"

# Column to allocate from ZIP to county using business_ratio
COUNT_COLUMN = "num_datacenters"
RATIO_COLUMN = "business_ratio"
COUNTY_ID_COLUMN = "county_fips"

# Dtype constraints: string columns kept as string on read, during compute, and on output
STRING_COLUMNS = ["zip_code", "county_fips", "county_name", "state"]


def _resolve_path(path_str: str, base_path: Path) -> Path:
    p = Path(path_str)
    return base_path / p if not p.is_absolute() else p


def _ensure_string_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Ensure listed columns are pandas string dtype (strip and normalize)."""
    for c in columns:
        if c not in df.columns:
            continue
        df[c] = df[c].astype(str).str.strip()
        df[c] = df[c].astype("string")
    return df


def _report_missing_per_feature(df: pd.DataFrame, table_name: str) -> None:
    """Log and optionally print missing value counts for each column (feature)."""
    n = len(df)
    missing = df.isna().sum()
    lines = [f"Missing values in {table_name} (n={n} rows):"]
    for col in df.columns:
        cnt = missing[col]
        pct = (100.0 * cnt / n) if n else 0
        lines.append(f"  {col}: {cnt} ({pct:.2f}%)")
    msg = "\n".join(lines)
    logger.info(msg)
    if _verbose:
        print(f"\n{msg}\n")


def build_county_from_zip(base_path: Path) -> pd.DataFrame:
    """
    Load zip_table_num_dc and reference_table with dtype constraints, join on zip_code (outer),
    compute allocated_count = num_datacenters × business_ratio per row, aggregate by county_fips
    by summing. Result is county-level expected data center count (may be fractional).
    """
    zip_path = _resolve_path(ZIP_TABLE_PATH, base_path)
    ref_path = _resolve_path(REFERENCE_TABLE_PATH, base_path)

    if not zip_path.exists():
        raise FileNotFoundError(f"ZIP table not found: {zip_path}")
    if not ref_path.exists():
        raise FileNotFoundError(f"Reference table not found: {ref_path}")

    # Read with explicit dtypes: string for ids, float for numeric
    zip_dtypes = {"zip_code": str}
    ref_dtypes = {"county_fips": str, "zip_code": str, "business_ratio": float}
    for c in ["county_name", "state_cap"]:
        ref_dtypes[c] = str

    logger.info(f"Reading zip table: {zip_path.name}")
    zip_df = pd.read_csv(zip_path, dtype=zip_dtypes)
    _ensure_string_columns(zip_df, ["zip_code"])
    zip_df["zip_code"] = zip_df["zip_code"].str.strip().str.zfill(5)
    if COUNT_COLUMN in zip_df.columns:
        zip_df[COUNT_COLUMN] = pd.to_numeric(zip_df[COUNT_COLUMN], errors="coerce")
    logger.info(f"ZIP table: {len(zip_df)} rows, columns: {list(zip_df.columns)}")
    _report_missing_per_feature(zip_df, "zip_table_num_dc")
    if _verbose:
        print(f"\n--- zip_table_num_dc (head) ---\n{zip_df.head()}\n")

    logger.info(f"Reading reference table: {ref_path.name}")
    ref_df = pd.read_csv(ref_path, dtype=ref_dtypes)
    _ensure_string_columns(ref_df, [c for c in STRING_COLUMNS if c in ref_df.columns])
    ref_df["zip_code"] = ref_df["zip_code"].str.strip().str.zfill(5)
    ref_df[COUNTY_ID_COLUMN] = ref_df[COUNTY_ID_COLUMN].astype(str).str.strip().str.zfill(5)
    ref_df[RATIO_COLUMN] = pd.to_numeric(ref_df[RATIO_COLUMN], errors="coerce")
    logger.info(f"Reference table: {len(ref_df)} rows, columns: {list(ref_df.columns)}")
    _report_missing_per_feature(ref_df, "reference_table")
    if _verbose:
        print(f"\n--- reference_table (head) ---\n{ref_df.head()}\n")

    # Join on zip_code (outer)
    merged = zip_df.merge(
        ref_df,
        on="zip_code",
        how="outer",
        suffixes=("", "_ref"),
    )
    # Re-apply string type after merge (merge can yield object dtype)
    _ensure_string_columns(merged, [c for c in STRING_COLUMNS if c in merged.columns])
    merged["zip_code"] = merged["zip_code"].astype(str).str.strip().str.zfill(5)
    merged[COUNTY_ID_COLUMN] = merged[COUNTY_ID_COLUMN].astype(str).str.strip().str.zfill(5)

    logger.info(f"After join on zip_code: {len(merged)} rows")
    if _verbose:
        print(f"\n--- merged (head) ---\n{merged.head()}\n")

    # Allocate: allocated_count = num_datacenters × business_ratio (treat missing num_datacenters as 0)
    merged[COUNT_COLUMN] = pd.to_numeric(merged[COUNT_COLUMN], errors="coerce").fillna(0)
    merged[RATIO_COLUMN] = pd.to_numeric(merged[RATIO_COLUMN], errors="coerce").fillna(0)
    merged["_allocated"] = merged[COUNT_COLUMN] * merged[RATIO_COLUMN]

    # Aggregate by county: sum allocated contributions
    agg_dict = {"_allocated": "sum"}
    for c in ["county_name", "state"]:
        if c in merged.columns:
            agg_dict[c] = "first"

    out = merged.groupby(COUNTY_ID_COLUMN, as_index=False).agg(agg_dict)
    out[COUNT_COLUMN] = out["_allocated"]
    out.drop(columns=["_allocated"], inplace=True)

    # Drop counties with missing county_fips
    out = out.dropna(subset=[COUNTY_ID_COLUMN])
    # Re-apply string dtypes after groupby
    _ensure_string_columns(out, [c for c in STRING_COLUMNS if c in out.columns])
    out[COUNTY_ID_COLUMN] = out[COUNTY_ID_COLUMN].astype(str).str.strip().str.zfill(5)

    logger.info(f"After aggregate by {COUNTY_ID_COLUMN}: {len(out)} rows, columns: {list(out.columns)}")
    if _verbose:
        print(f"\n--- county_from_zip_num_dc (head) ---\n{out.head()}\n")

    return out


def main():
    global _verbose
    parser = argparse.ArgumentParser(
        description="Build county-grain table of allocated data center counts from ZIP table (sum of num_datacenters × business_ratio)."
    )
    parser.add_argument(
        "--output",
        type=str,
        default=DEFAULT_OUTPUT_PATH,
        help=f"Output CSV path (default: {DEFAULT_OUTPUT_PATH})",
    )
    parser.add_argument(
        "--base-path",
        type=str,
        default=None,
        help="Project root (default: script parent)",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress table head prints (only show INFO logs)",
    )
    args = parser.parse_args()
    _verbose = not args.quiet

    base_path = Path(args.base_path) if args.base_path else project_root
    output_path = base_path / args.output
    output_path.parent.mkdir(parents=True, exist_ok=True)

    print("Building county table from ZIP table (plan_zip_to_county_num_dc)...")
    df = build_county_from_zip(base_path)
    # Ensure string columns are string before write
    _ensure_string_columns(df, [c for c in STRING_COLUMNS if c in df.columns])
    df.to_csv(output_path, index=False)
    print(f"Saved {len(df)} rows to {output_path}")


if __name__ == "__main__":
    main()
    sys.exit(0)
