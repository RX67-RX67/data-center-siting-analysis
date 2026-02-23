"""
Pipeline: Build a unified county-level working table from 01_tables.

Inputs (under data_revealed/01_tables/):
- county_table.csv
- county_fips_table.csv
- county_from_zip_table_elec_price.csv
- county_from_zip_table_num_dc.csv
- county_policy_signal.csv

All tables are merged with an outer join on (state, county). Some inputs use
different key column names (e.g., county_name, mentioned_state), which are
standardized to `state` and `county` before merging.

Output:
- data_revealed/02_tables/county_working_table.csv
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
DEFAULT_OUTPUT = "data_revealed/02_tables/county_working_table.csv"

# Per-file key column configuration
TABLE_SPECS: dict[str, dict[str, str]] = {
    "county_table.csv": {
        "state_col": "state",
        "county_col": "county",
    },
    "county_fips_table.csv": {
        "state_col": "state",
        "county_col": "county",
    },
    "county_from_zip_table_elec_price.csv": {
        "state_col": "state",
        "county_col": "county_name",
    },
    "county_from_zip_table_num_dc.csv": {
        "state_col": "state",
        "county_col": "county_name",
    },
    "county_policy_signal.csv": {
        "state_col": "mentioned_state",
        "county_col": "mentioned_county",
    },
}


def _standardize_keys(df: pd.DataFrame, file_name: str) -> pd.DataFrame:
    """Rename key columns to `state` and `county` based on TABLE_SPECS."""
    spec = TABLE_SPECS.get(file_name, {})
    state_col = spec.get("state_col")
    county_col = spec.get("county_col")

    df = df.copy()
    if state_col and state_col in df.columns and state_col != "state":
        df = df.rename(columns={state_col: "state"})
    if county_col and county_col in df.columns and county_col != "county":
        df = df.rename(columns={county_col: "county"})

    # If not specified, fall back to common patterns
    if "state" not in df.columns:
        for cand in ("mentioned_state",):
            if cand in df.columns:
                df = df.rename(columns={cand: "state"})
                break
    if "county" not in df.columns:
        for cand in ("county_name", "mentioned_county"):
            if cand in df.columns:
                df = df.rename(columns={cand: "county"})
                break

    if "state" not in df.columns or "county" not in df.columns:
        raise ValueError(
            f"File '{file_name}' does not contain recognizable state/county key columns."
        )

    # Normalize key values
    df["state"] = df["state"].astype(str).str.strip()
    df["county"] = df["county"].astype(str).str.strip()
    return df


def build_county_working_table(base_path: Path) -> pd.DataFrame:
    input_dir = base_path / INPUT_DIR
    if not input_dir.exists():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")

    csv_files = sorted(p for p in input_dir.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {input_dir}")

    tables: list[tuple[str, pd.DataFrame]] = []
    for path in csv_files:
        name = path.name
        logger.info(f"Reading {name}")
        df = pd.read_csv(path)
        df = _standardize_keys(df, name)
        logger.info(f"{name}: {len(df)} rows, columns: {list(df.columns)}")
        tables.append((Path(name).stem, df))

    # Sequential outer merges on (state, county)
    key_cols = ["state", "county"]
    base_name, out = tables[0]
    logger.info(f"Starting from table '{base_name}' with {len(out)} rows")

    for name, df in tables[1:]:
        logger.info(f"Merging table '{name}' ({len(df)} rows)")
        # Avoid column name collisions for non-key columns
        overlap = [c for c in df.columns if c not in key_cols and c in out.columns]
        if overlap:
            df = df.rename(columns={c: f"{c}_{name}" for c in overlap})

        out = out.merge(df, on=key_cols, how="outer")
        logger.info(f"After merging '{name}': {len(out)} rows, {len(out.columns)} columns")

    # Drop all FIPS code columns
    fips_cols = [c for c in out.columns if "fips" in c.lower()]
    if fips_cols:
        out = out.drop(columns=fips_cols)
        logger.info(f"Dropped FIPS columns: {fips_cols}")

    logger.info(
        f"Final county working table: {len(out)} rows, columns: {list(out.columns)}"
    )
    return out


def main():
    parser = argparse.ArgumentParser(
        description="Build unified county-level working table from data_revealed/01_tables."
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

    print("Building county working table from data_revealed/01_tables...")
    df = build_county_working_table(base_path)
    df.to_csv(output_path, index=False)
    print(f"Saved {len(df)} rows to {output_path}")


if __name__ == "__main__":
    main()
    sys.exit(0)

