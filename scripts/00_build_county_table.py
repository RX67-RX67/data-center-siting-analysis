import argparse
import logging
import sys
from pathlib import Path
import re

import numpy as np
import pandas as pd

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from src.configs.sources_county import SOURCES_COUNTY

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


def _normalize_county(series: pd.Series) -> pd.Series:
    """Remove anything after ' County' suffix and strip whitespace."""
    def _norm(s):
        if pd.isna(s):
            return s
        s = str(s).strip()
        s = re.sub(r",\s*[^,]+$", "", s)
        return s

    return series.map(_norm)


def _read_table(name: str, base_path: Path) -> pd.DataFrame:
    """Load a single table from SOURCES_COUNTY into a DataFrame."""
    if name not in SOURCES_COUNTY:
        raise KeyError(f"Unknown table: {name}. Available: {list(SOURCES_COUNTY)}")
    spec = SOURCES_COUNTY[name]

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

    # Combine columns (e.g. environment_risk: county_name from County Name + County Type)
    if "combine_columns" in spec:
        for out_col, cfg in spec["combine_columns"].items():
            from_cols = cfg["from"]
            if cfg.get("method") == "concat":
                sep = cfg.get("separator", " ")
                parts = [df[c].astype(str).str.strip() for c in from_cols if c in df.columns]
                if len(parts) >= 2:
                    df[out_col] = parts[0]
                    for p in parts[1:]:
                        df[out_col] = df[out_col] + sep + p
                    for c in from_cols:
                        if c in df.columns:
                            df = df.drop(columns=[c])
        logger.info(f"After combine_columns: {len(df)} rows")
        if _verbose:
            print(f"\n--- {name} (after combine_columns) ---\n{df.head()}\n")

    # Normalize key columns (e.g. county name)
    if "normalize" in spec:
        for key, key_cfg in spec["normalize"].items():
            key_col = spec["keys"].get(key)
            if key_col and key_col in df.columns and "method" in key_cfg:
                if "county" in key.lower() and "remove anything after" in key_cfg["method"].lower():
                    df[key_col] = _normalize_county(df[key_col])
        logger.info(f"After normalize: {len(df)} rows")
        if _verbose:
            print(f"\n--- {name} (after normalize) ---\n{df.head()}\n")

    # Pivot (e.g. labor_price)
    if "pivot" in spec:
        pv = spec["pivot"]
        index = pv.get("index", [])
        columns = pv.get("columns")
        values = pv.get("values")
        rename = pv.get("rename", {})
        df = df.pivot_table(index=index, columns=columns, values=values, aggfunc="first").reset_index()
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = ["_".join(str(x) for x in c).strip() for c in df.columns]
        else:
            df.columns = [str(c).strip() if isinstance(c, str) else c for c in df.columns]
        strip_rename = {k.strip(): v for k, v in rename.items()}
        df = df.rename(columns=strip_rename)
        logger.info(f"After pivot: {len(df)} rows, columns: {list(df.columns)}")
        if _verbose:
            print(f"\n--- {name} (after pivot) ---\n{df.head()}\n")

    # Rename to canonical keys + value_columns (and pivot renames)
    keys = spec.get("keys", {})
    value_columns = spec.get("value_columns", {})
    rename_map = {v: k for k, v in {**keys, **value_columns}.items()}
    pv_rename = spec.get("pivot", {}).get("rename", {})
    if pv_rename:
        rename_map.update(pv_rename)
    df = df.rename(columns=rename_map)
    keep_set = set(keys.keys()) | set(value_columns.keys())
    if pv_rename:
        keep_set |= set(pv_rename.values())
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

    # Column-specific handling (e.g. county_fips: normalize to 5-digit string with leading zeros)
    if "column_handling" in spec:
        for col, desc in spec["column_handling"].items():
            if col not in df.columns:
                continue
            if "zfill(5)" in desc or "5-digit" in desc.lower():
                df[col] = df[col].astype(str).str.strip().str.zfill(5).astype("string")
                logger.info(f"Normalized {col} to 5-digit string (zfill)")

    logger.info(f"After rename/keep: {len(df)} rows, columns: {list(df.columns)}")
    if _verbose:
        print(f"\n--- {name} (after rename/keep) ---\n{df.head()}\n")

    # Aggregation (no tables in current SOURCES_COUNTY use it; kept for future)
    if "aggregation" in spec:
        agg = spec["aggregation"]
        groupby = agg.get("groupby", [])
        method = agg.get("method", "median")
        value_cols_cfg = agg.get("value_columns", {})
        groupby_in_df = [c for c in groupby if c in df.columns]
        if groupby_in_df:
            value_cols = [c for c in value_cols_cfg.keys() if c in df.columns]
            if not value_cols:
                value_cols = [c for c in df.select_dtypes(include=["number"]).columns if c in df.columns]
            if value_cols:
                df = df.groupby(groupby_in_df, as_index=False)[value_cols].agg(method)
                logger.info(f"After aggregation ({method} by {groupby_in_df}): {len(df)} rows")
                if _verbose:
                    print(f"\n--- {name} (after aggregation) ---\n{df.head()}\n")

    # Proxies (e.g. transportation)
    if "proxies" in spec:
        for proxy_name, proxy_cfg in spec["proxies"].items():
            if proxy_name == "air_connectivity":
                weights = {
                    "primary_large_airport_count": 5,
                    "primary_medium_airport_count": 4,
                    "primary_small_airport_count": 3,
                    "non_hub_primary_airport_count": 2,
                    "national_non_primary_airport_count": 1.5,
                    "regional_non_primary_airport_count": 1.0,
                    "local_non_primary_airport_count": 0.7,
                    "basic_non_primary_airport_count": 0.4,
                    "unclassified_non_primary_airport_count": 0.2,
                }
                cols = [c for c in weights if c in df.columns]
                if cols:
                    w = np.array([weights[c] for c in cols])
                    raw = np.column_stack([pd.to_numeric(df[c], errors="coerce").fillna(0).values for c in cols])
                    df[proxy_name] = np.log1p((raw * w).sum(axis=1))
            elif proxy_name == "rail_intensity":
                if "rail_track_count" in df.columns:
                    df[proxy_name] = np.log1p(pd.to_numeric(df["rail_track_count"], errors="coerce").fillna(0))
            elif proxy_name == "infrastructure_quality":
                g, f, p = "infra_good_count", "infra_fair_count", "infra_poor_count"
                if all(c in df.columns for c in (g, f, p)):
                    gg = pd.to_numeric(df[g], errors="coerce").fillna(0)
                    ff = pd.to_numeric(df[f], errors="coerce").fillna(0)
                    pp = pd.to_numeric(df[p], errors="coerce").fillna(0)
                    total = gg + ff + pp
                    df[proxy_name] = np.where(total > 0, (gg + 0.5 * ff) / total, np.nan)
            elif proxy_name == "dock_presence":
                if "docks_count" in df.columns:
                    df[proxy_name] = (pd.to_numeric(df["docks_count"], errors="coerce").fillna(0) > 0).astype(int)
        logger.info(f"After proxies: columns include {list(spec['proxies'].keys())}")
        
        # Drop value columns after proxies are built (they're redundant)
        value_cols_to_drop = list(spec.get("value_columns", {}).keys())
        keys_to_keep = list(spec.get("keys", {}).keys())
        proxy_cols_to_keep = list(spec.get("proxies", {}).keys())
        cols_to_drop = [c for c in value_cols_to_drop if c in df.columns and c not in keys_to_keep and c not in proxy_cols_to_keep]
        if cols_to_drop:
            df = df.drop(columns=cols_to_drop)
            logger.info(f"Dropped value columns after proxies: {cols_to_drop}")
            if _verbose:
                print(f"\n--- {name} (after dropping value columns) ---\n{df.head()}\n")

    return df


def build_county_table(base_path: Path, table_names: list[str] | None = None) -> pd.DataFrame:
    """Load county-grain table(s) from SOURCES_COUNTY. Merge on (state, county)."""
    names = table_names or list(SOURCES_COUNTY)
    if not names:
        raise ValueError("SOURCES_COUNTY is empty")
    dfs = []
    for name in names:
        df = _read_table(name, base_path)
        _print_missing_counts(df, name)
        for k in ("state", "county"):
            if k in df.columns:
                df[k] = df[k].astype(str).str.strip().astype("string")
        logger.info(f"{name}: {len(df)} rows")
        dfs.append((name, df))

    out = dfs[0][1]
    join_keys = ["state", "county"]
    for name, df in dfs[1:]:
        on_cols = [k for k in join_keys if k in out.columns and k in df.columns]
        if on_cols:
            suffix_cols = [c for c in df.columns if c not in on_cols and c in out.columns]
            if suffix_cols:
                df = df.rename(columns={c: f"{c}_{name}" for c in suffix_cols})
            out = out.merge(df, on=on_cols, how="outer")
        else:
            out = pd.concat([out, df], axis=1)
    logger.info(f"County table: {len(out)} rows, columns: {list(out.columns)}")
    if _verbose:
        print(f"\n--- county_table (final) ---\n{out.head()}\n")
    return out


def main():
    global _verbose
    parser = argparse.ArgumentParser(description="Build county-granularity table from SOURCES_COUNTY")
    parser.add_argument(
        "--output",
        type=str,
        default="data/processed_data/data_build/county_table.csv",
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
        help="Comma-separated table names (default: all in SOURCES_COUNTY)",
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

    print("Building county table from SOURCES_COUNTY...")
    df = build_county_table(base_path, table_names=table_names)
    df.to_csv(output_path, index=False)
    print(f"Saved {len(df)} rows to {output_path}")


if __name__ == "__main__":
    main()
    sys.exit(0)
