"""
Pipeline: Build zip-level table of datacenter counts.

Concatenates all CSVs under data/processed_data whose names start with "datacenter"
(e.g. datacenters_alabama.csv, datacenters_alaska.csv). Counts how many datacenters
fall in each zip code. Outputs zip_code and num_datacenters to
data/processed_data/data_build/zip_table_num_dc.csv.
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

DATA_DIR = "data/processed_data"
DATACENTER_PREFIX = "datacenter"
DEFAULT_OUTPUT_PATH = "data/processed_data/data_build/zip_table_num_dc.csv"
ZIP_COLUMN_INPUT = "zip"
ZIP_COLUMN_OUTPUT = "zip_code"
COUNT_COLUMN = "num_datacenters"


def _resolve_path(path_str: str, base_path: Path) -> Path:
    p = Path(path_str)
    return base_path / p if not p.is_absolute() else p


def _discover_datacenter_csvs(data_dir: Path) -> list[Path]:
    """Return paths to CSV files whose name starts with DATACENTER_PREFIX."""
    if not data_dir.is_dir():
        return []
    paths = sorted(p for p in data_dir.glob(f"{DATACENTER_PREFIX}*.csv") if p.is_file())
    return paths


def build_zip_table_num_dc(base_path: Path, data_dir: str | Path | None = None) -> pd.DataFrame:
    """
    Load all datacenter_*.csv under data_dir, concatenate, then count rows per zip code.
    Returns a DataFrame with zip_code and num_datacenters.
    """
    dir_path = _resolve_path(data_dir or DATA_DIR, base_path)
    paths = _discover_datacenter_csvs(dir_path)
    if not paths:
        logger.warning("No datacenter_*.csv files found under %s", dir_path)
        return pd.DataFrame(columns=[ZIP_COLUMN_OUTPUT, COUNT_COLUMN])

    # Read zip column as string so leading zeros and type are preserved
    read_dtype = {ZIP_COLUMN_INPUT: str, "zip_code": str}
    dfs = []
    for p in paths:
        try:
            df = pd.read_csv(p, dtype=read_dtype)
            dfs.append(df)
            logger.info("Read %s: %d rows", p.name, len(df))
        except Exception as e:
            logger.warning("Skip %s: %s", p.name, e)

    if not dfs:
        return pd.DataFrame(columns=[ZIP_COLUMN_OUTPUT, COUNT_COLUMN])

    combined = pd.concat(dfs, ignore_index=True)
    logger.info("Combined: %d rows from %d files", len(combined), len(dfs))
    if _verbose:
        print(f"\n--- combined datacenters (head) ---\n{combined.head()}\n")

    # Normalize zip column (input may be "zip")
    zip_col = ZIP_COLUMN_INPUT if ZIP_COLUMN_INPUT in combined.columns else "zip_code"
    if zip_col not in combined.columns:
        raise ValueError(f"Datacenter tables must have a '{ZIP_COLUMN_INPUT}' or 'zip_code' column. Found: {list(combined.columns)}")
    # Drop rows with missing zip so they don't become "00nan" after zfill
    combined = combined.dropna(subset=[zip_col])
    combined[ZIP_COLUMN_OUTPUT] = combined[zip_col].astype(str).str.strip().str.zfill(5).astype("string")
    # Drop rows where normalized zip is empty or looks like "nan" (e.g. literal "nan" in CSV)
    invalid_zip = combined[ZIP_COLUMN_OUTPUT].str.contains("nan", case=False, na=True) | (combined[ZIP_COLUMN_OUTPUT].str.strip() == "")
    if invalid_zip.any():
        n_drop = invalid_zip.sum()
        combined = combined[~invalid_zip]
        logger.warning("Dropped %d rows with missing or invalid zip", n_drop)

    out = (
        combined.groupby(ZIP_COLUMN_OUTPUT, as_index=False)
        .size()
        .rename(columns={"size": COUNT_COLUMN})
    )
    # Ensure zip_code is string dtype after groupby (for consistent output)
    out[ZIP_COLUMN_OUTPUT] = out[ZIP_COLUMN_OUTPUT].astype(str).str.strip().str.zfill(5).astype("string")
    logger.info("Zip-level counts: %d distinct zip codes", len(out))
    if _verbose:
        print(f"\n--- zip_table_num_dc (head) ---\n{out.head()}\n")

    return out


def main():
    global _verbose
    parser = argparse.ArgumentParser(
        description="Build zip-level table of datacenter counts from datacenter_*.csv files."
    )
    parser.add_argument(
        "--output",
        type=str,
        default=DEFAULT_OUTPUT_PATH,
        help=f"Output CSV path (default: {DEFAULT_OUTPUT_PATH})",
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default=DATA_DIR,
        help=f"Directory containing datacenter_*.csv (default: {DATA_DIR})",
    )
    parser.add_argument(
        "--base-path",
        type=str,
        default=None,
        help="Project root (default: script parent parent)",
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

    print("Building zip_table_num_dc from datacenter_*.csv...")
    df = build_zip_table_num_dc(base_path, data_dir=args.data_dir)
    # Ensure zip_code is string before write
    if ZIP_COLUMN_OUTPUT in df.columns:
        df[ZIP_COLUMN_OUTPUT] = df[ZIP_COLUMN_OUTPUT].astype(str).str.strip().str.zfill(5).astype("string")
    df.to_csv(output_path, index=False)
    print(f"Saved {len(df)} rows to {output_path}")


if __name__ == "__main__":
    main()
    sys.exit(0)
