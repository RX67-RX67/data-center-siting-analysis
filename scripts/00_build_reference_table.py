import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from src.configs.sources_reference import SOURCES_REFERENCE

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# Set by main() when --quiet; _read_table and build_reference_table print head only when not quiet
_verbose = True


def _print_missing_counts(df: pd.DataFrame, table_name: str) -> None:
    """Print missing value count per column for the input table (when not quiet)."""
    if not _verbose:
        return
    n = len(df)
    missing = df.isna().sum()
    lines = [f"Missing values in input table '{table_name}' (n={n} rows):"]
    for col in df.columns:
        cnt = int(missing[col])
        pct = (100.0 * cnt / n) if n else 0.0
        lines.append(f"  {col}: {cnt} ({pct:.2f}%)")
    print("\n" + "\n".join(lines) + "\n")


# US state and territory abbreviation -> full name (for state_cap column)
STATE_ABBR_TO_FULL = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas", "CA": "California",
    "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware", "DC": "District of Columbia",
    "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho", "IL": "Illinois",
    "IN": "Indiana", "IA": "Iowa", "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana",
    "ME": "Maine", "MD": "Maryland", "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota",
    "MS": "Mississippi", "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
    "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
    "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma", "OR": "Oregon",
    "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina", "SD": "South Dakota",
    "TN": "Tennessee", "TX": "Texas", "UT": "Utah", "VT": "Vermont", "VA": "Virginia",
    "WA": "Washington", "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming",
    "AS": "American Samoa", "FM": "Federated States of Micronesia", "GU": "Guam",
    "MH": "Marshall Islands", "MP": "Northern Mariana Islands", "PW": "Palau", "PR": "Puerto Rico",
    "VI": "U.S. Virgin Islands",
}


def _map_state_cap_to_full(df: pd.DataFrame) -> pd.DataFrame:
    """Map state_cap (abbreviation) to state (full name), then drop state_cap."""
    if "state_cap" not in df.columns:
        return df
    cap = df["state_cap"].astype(str).str.strip().str.upper()
    df = df.copy()
    df["state"] = cap.map(lambda x: STATE_ABBR_TO_FULL.get(x, x) if pd.notna(x) and x else "")
    df = df.drop(columns=["state_cap"])
    return df


def _resolve_path(path_str: str, base_path: Path) -> Path:
    p = Path(path_str)
    return base_path / p if not p.is_absolute() else p


def _parse_read_dtypes(read_dtypes: dict) -> dict:
    """Convert schema dtype names to types usable by read_excel/read_csv."""
    type_map = {"string": str, "str": str, "float64": float, "float": float, "int64": int, "int": int}
    out = {}
    for col, dtype in read_dtypes.items():
        if isinstance(dtype, type):
            out[col] = dtype
        else:
            out[col] = type_map.get(dtype, dtype)
    return out


def _read_table(name: str, base_path: Path) -> pd.DataFrame:
    """Load a single table from SOURCES_REFERENCE into a DataFrame."""
    if name not in SOURCES_REFERENCE:
        raise KeyError(f"Unknown table: {name}. Available: {list(SOURCES_REFERENCE)}")
    spec = SOURCES_REFERENCE[name]

    path = _resolve_path(spec["path"], base_path)
    if not path.exists():
        raise FileNotFoundError(f"Data not found: {path}")

    # Build read kwargs; apply read_dtypes at read time so no later step alters values
    if spec.get("format", "xlsx").lower() == "xlsx":
        read_kw = {"engine": "openpyxl"}
        if "sheet" in spec:
            read_kw["sheet_name"] = spec["sheet"]
        if "skiprows" in spec:
            read_kw["skiprows"] = spec["skiprows"]
        if "read_dtypes" in spec:
            read_kw["dtype"] = _parse_read_dtypes(spec["read_dtypes"])
        df = pd.read_excel(path, **read_kw)
    else:
        read_kw = {}
        if "read_dtypes" in spec:
            read_kw["dtype"] = _parse_read_dtypes(spec["read_dtypes"])
        df = pd.read_csv(path, **read_kw)
    logger.info(f"Read {name}: {len(df)} rows from {path.name}")
    if _verbose:
        print(f"\n--- {name} (after read) ---\n{df.head()}\n")

    # Filters: match column by stripped name; compare value as string (50 == "050")
    if "filters" in spec:
        logger.info(f"Filtering {name} by {spec['filters']}")
        col_map = {c.strip(): c for c in df.columns}
        for col, val in spec["filters"].items():
            col_actual = col_map.get(col.strip()) or (col if col in df.columns else None)
            if col_actual is not None:
                val_str = str(val).strip().zfill(3)
                mask = df[col_actual].astype(str).str.strip().str.zfill(3) == val_str
                df = df[mask]
        logger.info(f"After filter: {len(df)} rows")
        if _verbose:
            print(f"\n--- {name} (after filter) ---\n{df.head()}\n")

    # Combine columns (e.g. FIPS)
    if "combine_columns" in spec:
        for out_col, cfg in spec["combine_columns"].items():
            from_cols = cfg["from"]
            zfill_list = cfg.get("zfill", [2, 3])
            parts = [
                df[c].astype(str).str.zfill(zfill_list[i] if i < len(zfill_list) else 0)
                for i, c in enumerate(from_cols)
            ]
            df[out_col] = parts[0]
            for i in range(1, len(parts)):
                df[out_col] = df[out_col].astype(str) + parts[i].astype(str)
            for c in from_cols:
                if c in df.columns:
                    df = df.drop(columns=[c])

    # Rename and keep canonical columns
    keys = spec.get("keys", {})
    value_columns = spec.get("value_columns", {})
    rename = {v: k for k, v in {**keys, **value_columns}.items()}
    df = df.rename(columns=rename)
    keep = [c for c in list(keys.keys()) + list(value_columns.keys()) if c in df.columns]
    df = df[keep].copy()

    # Apply schema dtypes for consistent joins (string, float64, etc.)
    # Use pandas StringDtype ("string") so dtypes display as string, not object
    if "dtypes" in spec:
        for col, dtype in spec["dtypes"].items():
            if col not in df.columns:
                continue
            if dtype in ("string", "str"):
                df[col] = df[col].astype(str).str.strip()
            elif isinstance(dtype, str) and dtype.startswith("float"):
                df[col] = pd.to_numeric(df[col], errors="coerce")
            else:
                try:
                    df[col] = df[col].astype(dtype)
                except (TypeError, ValueError):
                    pass
        logger.info(f"Applied dtypes: {list(spec['dtypes'].keys())}")
        if _verbose:
            print(f"\n--- {name} (after dtypes) ---\n{df.dtypes}\n")

    logger.info(f"After rename/keep: {len(df)} rows, columns: {list(df.columns)}")
    if _verbose:
        print(f"\n--- {name} (after rename/keep) ---\n{df.head()}\n")

    # Post-filters
    if "post_filters" in spec:
        logger.info(f"Post-filtering {name} by {spec['post_filters']}")
        for key, value in spec["post_filters"].items():
            if key.endswith("_not_ending_with"):
                col = key.replace("_not_ending_with", "")
                ser = df[col].iloc[:, 0] if isinstance(df[col], pd.DataFrame) else df[col]
                df = df[~ser.astype(str).str.endswith(str(value))]
        logger.info(f"After post-filter: {len(df)} rows")
        if _verbose:
            print(f"\n--- {name} (after post_filter) ---\n{df.head()}\n")

    return df


def build_reference_table(base_path: Path) -> pd.DataFrame:
    """Load zip_to_fips and fips_to_county, join on county_fips."""
    zip_fips = _read_table("zip_to_fips", base_path)
    _print_missing_counts(zip_fips, "zip_to_fips")
    logger.info(f"zip_to_fips: {len(zip_fips)} rows")
    if _verbose:
        print(f"\n--- zip_to_fips (final) ---\n{zip_fips.head()}\n")

    fips_county = _read_table("fips_to_county", base_path)
    _print_missing_counts(fips_county, "fips_to_county")
    logger.info(f"fips_to_county: {len(fips_county)} rows")
    if _verbose:
        print(f"\n--- fips_to_county (final) ---\n{fips_county.head()}\n")

    # Normalize join keys to 5-digit string for reliable merges
    zip_fips["county_fips"] = zip_fips["county_fips"].astype(str).str.strip().str.zfill(5)
    zip_fips["zip_code"] = zip_fips["zip_code"].astype(str).str.strip().str.zfill(5)
    fips_county["county_fips"] = fips_county["county_fips"].astype(str).str.strip().str.zfill(5)
    ref = zip_fips.merge(fips_county, on="county_fips", how="outer")
    logger.info(f"Reference table (after join): {len(ref)} rows, columns: {list(ref.columns)}")
    if _verbose:
        print(f"\n--- reference_table (after join) ---\n{ref.head()}\n")

    ref = _map_state_cap_to_full(ref)
    logger.info("Mapped state_cap -> state (full name), dropped state_cap")
    if _verbose:
        print(f"\n--- reference_table (final) ---\n{ref.head()}\n")
    return ref


def main():
    global _verbose
    parser = argparse.ArgumentParser(description="Build reference table from SOURCES_REFERENCE")
    parser.add_argument(
        "--output",
        type=str,
        default="data/processed_data/data_build/reference_table.csv",
        help="Output CSV path",
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

    print("Building reference table from SOURCES_REFERENCE...")
    df = build_reference_table(base_path)
    df.to_csv(output_path, index=False)
    print(f"Saved {len(df)} rows to {output_path}")


if __name__ == "__main__":
    main()
    sys.exit(0)
