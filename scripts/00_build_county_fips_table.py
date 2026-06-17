"""
Pipeline: Build county-FIPS-granularity table from SOURCES_COUNTY_FIPS.

Loads tables (grid_infrastructure, high_speed_internet, land_price) from
src/configs/sources_county_fips.py: read with read_dtypes, filter, special_values,
pivot, rename, dtypes, proxies. Merges on county_fips. Writes county_fips_table.csv.

Note: county-name-grain tables live in sources_county.py and are built by a separate pipeline.
"""

import argparse
import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from src.configs.sources_county_fips import SOURCES_COUNTY_FIPS

# US state and territory abbreviation -> full name (for state column in output)
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

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

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


def _resolve_path(path_str: str, base_path: Path) -> Path:
    p = Path(path_str)
    return base_path / p if not p.is_absolute() else p


def _parse_read_dtypes(read_dtypes: dict) -> dict:
    """Convert schema dtype names to types usable by read_csv/read_excel."""
    type_map = {"string": str, "str": str, "float64": float, "float": float, "int64": int, "int": int}
    out = {}
    for col, dtype in read_dtypes.items():
        if isinstance(dtype, type):
            out[col] = dtype
        else:
            out[col] = type_map.get(dtype, dtype)
    return out


def _read_csv(path: Path, sep: str | None = None, dtype: dict | None = None) -> pd.DataFrame:
    """Read CSV with encoding/delimiter fallback (utf-16 BOM, utf-8, latin-1; tab or comma).
    
    Handles comma-separated thousands in numeric columns via thousands=',' parameter.
    """
    with open(path, "rb") as f:
        head = f.read(4)
    kwargs = {"low_memory": False, "thousands": ","}
    if dtype is not None:
        kwargs["dtype"] = dtype
    if head[:2] in (b"\xff\xfe", b"\xfe\xff"):
        delim = sep if sep is not None else "\t"
        return pd.read_csv(path, encoding="utf-16", sep=delim, **kwargs)
    for enc in ("utf-8", "latin-1", "cp1252"):
        for delim in ([sep] if sep is not None else [None, "\t"]):
            try:
                kw = {"encoding": enc, **kwargs}
                if delim is not None:
                    kw["sep"] = delim
                return pd.read_csv(path, **kw)
            except (UnicodeDecodeError, pd.errors.ParserError):
                continue
    return pd.read_csv(path, encoding="utf-8", sep=sep or ",", **kwargs)


def _read_table(name: str, base_path: Path) -> pd.DataFrame:
    """Load a single table from SOURCES_COUNTY_FIPS into a DataFrame."""
    if name not in SOURCES_COUNTY_FIPS:
        raise KeyError(f"Unknown table: {name}. Available: {list(SOURCES_COUNTY_FIPS)}")
    spec = SOURCES_COUNTY_FIPS[name]

    path = _resolve_path(spec["path"], base_path)
    if not path.exists():
        raise FileNotFoundError(f"Data not found: {path}")

    read_dtype_arg = _parse_read_dtypes(spec["read_dtypes"]) if spec.get("read_dtypes") else None
    fmt = spec.get("format", "csv").lower()
    if fmt == "csv":
        df = _read_csv(path, dtype=read_dtype_arg)
    else:
        read_kw = {"engine": "openpyxl"}
        if "sheet" in spec:
            read_kw["sheet_name"] = spec["sheet"]
        if "skiprows" in spec:
            read_kw["skiprows"] = spec["skiprows"]
        if read_dtype_arg:
            read_kw["dtype"] = read_dtype_arg
        df = pd.read_excel(path, **read_kw)

    # Normalize column names: collapse newlines/multi-space to single space, then strip
    def _norm_col(c):
        if not isinstance(c, str):
            return c
        return " ".join(c.replace("\n", " ").replace("\r", " ").split()).strip()
    df.columns = [_norm_col(c) for c in df.columns]
    logger.info(f"Read {name}: {len(df)} rows from {path.name}")
    if _verbose:
        print(f"\n--- {name} (after read) ---\n{df.head()}\n")

    # Filter
    if "filter" in spec:
        filt = spec["filter"]
        for col, val in list(filt.items()):
            col_actual = col if col in df.columns else next((c for c in df.columns if c.strip() == col.strip()), None)
            if col_actual is None:
                logger.warning(f"Filter column '{col}' not in {name}; skipping")
                continue
            if isinstance(val, list):
                df = df[df[col_actual].isin(val)]
            else:
                df = df[df[col_actual] == val]
        logger.info(f"After filter: {len(df)} rows")
        if _verbose:
            print(f"\n--- {name} (after filter) ---\n{df.head()}\n")

    # Special values handling (e.g. grid_infrastructure: "<10" -> 5)
    if "special_values" in spec:
        for special_val, cfg in spec["special_values"].items():
            replace_with = cfg.get("replace_with")
            if replace_with is not None:
                # Find columns that might contain this special value (value_columns)
                value_cols = spec.get("value_columns", {})
                for canonical_name, raw_name in value_cols.items():
                    if raw_name in df.columns:
                        # Replace special value string with numeric value
                        mask = df[raw_name].astype(str).str.strip() == str(special_val).strip()
                        if mask.any():
                            df.loc[mask, raw_name] = replace_with
                            logger.info(f"Replaced {mask.sum()} occurrences of '{special_val}' with {replace_with} in {raw_name}")
        if _verbose:
            print(f"\n--- {name} (after special_values) ---\n{df.head()}\n")

    # Pivot (e.g. high_speed_internet)
    if "pivot" in spec:
        pv = spec["pivot"]
        index = pv.get("index", [])
        columns = pv.get("columns")
        values = pv.get("values", [])
        flatten_names = pv.get("flatten_names", False)
        
        # Handle multiple values in pivot
        if isinstance(values, str):
            values = [values]
        
        df = df.pivot_table(index=index, columns=columns, values=values, aggfunc="first").reset_index()
        
        if isinstance(df.columns, pd.MultiIndex):
            if flatten_names:
                # Flatten: reverse order to get "Fiber_speed_100_20" format (column_value)
                # MultiIndex is (value, column), but we want (column, value)
                new_cols = []
                for c in df.columns:
                    if isinstance(c, tuple) and len(c) == 2:
                        # (value, column) -> reverse to (column, value)
                        new_cols.append("_".join(str(x) for x in reversed(c)).strip())
                    else:
                        # Index column (single string) or other
                        new_cols.append(str(c).strip())
                df.columns = new_cols
            else:
                df.columns = ["_".join(str(x) for x in c).strip() if isinstance(c, tuple) else str(c).strip() for c in df.columns]
        else:
            df.columns = [str(c).strip() if isinstance(c, str) else c for c in df.columns]
        
        # Clean up any trailing underscores/spaces from column names
        df.columns = [c.rstrip("_ ").strip() for c in df.columns]
        
        logger.info(f"After pivot: {len(df)} rows, columns: {list(df.columns)}")
        if _verbose:
            print(f"\n--- {name} (after pivot) ---\n{df.head()}\n")

    # Rename to canonical keys + value_columns
    keys = spec.get("keys", {})
    value_columns = spec.get("value_columns", {})
    rename_map = {v: k for k, v in {**keys, **value_columns}.items()}
    df = df.rename(columns=rename_map)
    keep_set = set(keys.keys()) | set(value_columns.keys())
    keep = [c for c in df.columns if c in keep_set]
    df = df[keep].copy()

    # Apply schema dtypes for consistent joins (string -> pandas "string", float64)
    if "dtypes" in spec:
        for col, dtype in spec["dtypes"].items():
            if col not in df.columns:
                continue
            if dtype in ("string", "str"):
                df[col] = df[col].astype(str).str.strip().astype("string")
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

    # Proxies
    if "proxies" in spec:
        for proxy_name, proxy_cfg in spec["proxies"].items():
            # Handle proxies with "columns" (sum of multiple columns)
            if "columns" in proxy_cfg:
                cols = proxy_cfg["columns"]
                cols_in_df = [c for c in cols if c in df.columns]
                if cols_in_df:
                    df[proxy_name] = df[cols_in_df].sum(axis=1)
                    logger.info(f"Built proxy {proxy_name} from columns: {cols_in_df}")
            # Handle proxies with "column" (single column, just keep/rename)
            elif "column" in proxy_cfg:
                col = proxy_cfg["column"]
                if col in df.columns:
                    df[proxy_name] = df[col]
                    logger.info(f"Built proxy {proxy_name} from column: {col}")
        
        logger.info(f"After proxies: columns include {list(spec['proxies'].keys())}")
        
        # Drop value columns that participate in proxy construction
        value_cols = spec.get("value_columns", {})
        keys_to_keep = list(spec.get("keys", {}).keys())
        proxy_cols_to_keep = list(spec.get("proxies", {}).keys())
        
        # Find columns used in proxies
        cols_used_in_proxies = set()
        for proxy_name, proxy_cfg in spec["proxies"].items():
            if "columns" in proxy_cfg:
                cols_used_in_proxies.update(proxy_cfg["columns"])
            elif "column" in proxy_cfg:
                cols_used_in_proxies.add(proxy_cfg["column"])
        
        # Drop value columns that are used in proxies (but keep keys and proxies themselves)
        cols_to_drop = [
            c for c in value_cols.keys() 
            if c in df.columns 
            and c not in keys_to_keep 
            and c not in proxy_cols_to_keep
            and c in cols_used_in_proxies
        ]
        
        if cols_to_drop:
            df = df.drop(columns=cols_to_drop)
            logger.info(f"Dropped value columns used in proxies: {cols_to_drop}")
            if _verbose:
                print(f"\n--- {name} (after dropping proxy columns) ---\n{df.head()}\n")

    return df


def build_county_fips_table(base_path: Path, table_names: list[str] | None = None) -> pd.DataFrame:
    """Load county-FIPS-grain table(s) from SOURCES_COUNTY_FIPS. Merge on county_fips."""
    names = table_names or list(SOURCES_COUNTY_FIPS)
    if not names:
        raise ValueError("SOURCES_COUNTY_FIPS is empty")
    dfs = []
    for name in names:
        df = _read_table(name, base_path)
        _print_missing_counts(df, name)
        # Normalize county_fips to string for consistent joins
        if "county_fips" in df.columns:
            df["county_fips"] = df["county_fips"].astype(str).str.strip().str.zfill(5)
        logger.info(f"{name}: {len(df)} rows")
        dfs.append((name, df))

    out = dfs[0][1]
    join_key = "county_fips"
    for name, df in dfs[1:]:
        if join_key in out.columns and join_key in df.columns:
            suffix_cols = [c for c in df.columns if c != join_key and c in out.columns]
            if suffix_cols:
                df = df.rename(columns={c: f"{c}_{name}" for c in suffix_cols})
            out = out.merge(df, on=join_key, how="outer")
        else:
            out = pd.concat([out, df], axis=1)
    # Normalize state column from abbreviation to full name with capital first letter
    if "state" in out.columns:
        s = out["state"].astype(str).str.strip()
        # Map 2-letter abbreviations; keep other values but title-case them
        mapped = s.str.upper().map(STATE_ABBR_TO_FULL)
        # Where mapping fails, fall back to title-cased original
        fallback = s.str.title()
        out["state"] = mapped.fillna(fallback).astype("string")

    logger.info(f"County FIPS table: {len(out)} rows, columns: {list(out.columns)}")
    if _verbose:
        print(f"\n--- county_fips_table (final) ---\n{out.head()}\n")
    return out


def main():
    global _verbose
    parser = argparse.ArgumentParser(description="Build county-FIPS-granularity table from SOURCES_COUNTY_FIPS")
    parser.add_argument(
        "--output",
        type=str,
        default="data/processed_data/data_build/county_fips_table.csv",
        help="Output CSV path",
    )
    parser.add_argument(
        "--base-path",
        type=str,
        default=None,
        help="Project root (default: script parent)",
    )
    parser.add_argument(
        "--tables",
        type=str,
        default=None,
        help="Comma-separated table names (default: all in SOURCES_COUNTY_FIPS)",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress table head prints (only show INFO logs)",
    )
    args = parser.parse_args()
    _verbose = not args.quiet

    base_path = Path(args.base_path) if args.base_path else project_root
    table_names = [t.strip() for t in args.tables.split(",")] if args.tables else None
    output_path = base_path / args.output
    output_path.parent.mkdir(parents=True, exist_ok=True)

    print("Building county FIPS table from SOURCES_COUNTY_FIPS...")
    df = build_county_fips_table(base_path, table_names=table_names)
    df.to_csv(output_path, index=False)
    print(f"Saved {len(df)} rows to {output_path}")


if __name__ == "__main__":
    main()
    sys.exit(0)
