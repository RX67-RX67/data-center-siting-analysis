import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from src.configs.sources_zip import SOURCES_ZIP

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

_verbose = True


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


def _read_table(name: str, base_path: Path) -> pd.DataFrame:
    """Load a single table from SOURCES_ZIP into a DataFrame."""
    if name not in SOURCES_ZIP:
        raise KeyError(f"Unknown table: {name}. Available: {list(SOURCES_ZIP)}")
    spec = SOURCES_ZIP[name]

    # Read: single path or multiple sources (concat); apply read_dtypes at read time
    read_dtype_arg = _parse_read_dtypes(spec["read_dtypes"]) if spec.get("read_dtypes") else None
    if "sources" in spec:
        dfs = []
        for s in spec["sources"]:
            path = _resolve_path(s["path"], base_path)
            if not path.exists():
                raise FileNotFoundError(f"Data not found: {path}")
            fmt = s.get("format", "csv").lower()
            if fmt == "csv":
                df_part = pd.read_csv(path, dtype=read_dtype_arg) if read_dtype_arg else pd.read_csv(path)
            else:
                df_part = pd.read_excel(path, engine="openpyxl", dtype=read_dtype_arg) if read_dtype_arg else pd.read_excel(path, engine="openpyxl")
            dfs.append(df_part)
        df = pd.concat(dfs, ignore_index=True)
        logger.info(f"Read {name}: concatenated {len(spec['sources'])} sources, {len(df)} rows")
    else:
        path = _resolve_path(spec["path"], base_path)
        if not path.exists():
            raise FileNotFoundError(f"Data not found: {path}")
        fmt = spec.get("format", "csv").lower()
        if fmt == "csv":
            df = pd.read_csv(path, dtype=read_dtype_arg) if read_dtype_arg else pd.read_csv(path)
        else:
            df = pd.read_excel(path, engine="openpyxl", dtype=read_dtype_arg) if read_dtype_arg else pd.read_excel(path, engine="openpyxl")
        logger.info(f"Read {name}: {len(df)} rows from {path.name}")

    if _verbose:
        print(f"\n--- {name} (after read) ---\n{df.head()}\n")

    # Rename and keep canonical columns
    keys = spec.get("keys", {})
    value_columns = spec.get("value_columns", {})
    rename = {v: k for k, v in {**keys, **value_columns}.items()}
    df = df.rename(columns=rename)
    keep = [c for c in list(keys.keys()) + list(value_columns.keys()) if c in df.columns]
    df = df[keep].copy()

    # Apply schema dtypes for consistent joins (string, float64); use pandas StringDtype so dtypes show as string
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

    # Aggregation (e.g. mean per zip_code)
    if "aggregation" in spec:
        agg = spec["aggregation"]
        groupby = agg.get("groupby", [])
        method = agg.get("method", "mean")
        groupby_in_df = [c for c in groupby if c in df.columns]
        if groupby_in_df:
            numeric_cols = [c for c in df.select_dtypes(include=["number"]).columns if c in df.columns]
            if numeric_cols:
                df = df.groupby(groupby_in_df, as_index=False)[numeric_cols].agg(method)
            # Re-apply dtypes after aggregation (groupby can change types); keep zip_code and other non-numeric as-is from groupby index
            if groupby_in_df and "dtypes" in spec:
                for col in groupby_in_df:
                    if col in spec["dtypes"] and spec["dtypes"][col] in ("string", "str"):
                        df[col] = df[col].astype(str).str.strip().str.zfill(5).astype("string")
            logger.info(f"After aggregation ({method} by {groupby_in_df}): {len(df)} rows")
            if _verbose:
                print(f"\n--- {name} (after aggregation) ---\n{df.head()}\n")

    # Normalize zip_code to 5-digit string for reliable joins
    if "zip_code" in df.columns:
        df["zip_code"] = df["zip_code"].astype(str).str.strip().str.zfill(5).astype("string")

    return df


def build_zip_table(base_path: Path, table_names: list[str] | None = None) -> pd.DataFrame:
    """Load ZIP-grain table(s) from SOURCES_ZIP. If multiple, join on zip_code."""
    names = table_names or list(SOURCES_ZIP)
    if not names:
        raise ValueError("SOURCES_ZIP is empty")
    dfs = []
    for name in names:
        df = _read_table(name, base_path)
        logger.info(f"{name}: {len(df)} rows")
        dfs.append((name, df))

    out = dfs[0][1]
    for name, df in dfs[1:]:
        on_col = "zip_code" if "zip_code" in out.columns and "zip_code" in df.columns else None
        if on_col:
            out = out.merge(df, on=on_col, how="outer", suffixes=("", f"_{name}"))
        else:
            out = pd.concat([out, df], axis=1)
    logger.info(f"ZIP table: {len(out)} rows, columns: {list(out.columns)}")
    if _verbose:
        print(f"\n--- zip_table (final) ---\n{out.head()}\n")
    return out


def main():
    global _verbose
    parser = argparse.ArgumentParser(description="Build ZIP-granularity table from SOURCES_ZIP")
    parser.add_argument(
        "--output",
        type=str,
        default="data/processed_data/zip_table.csv",
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
        help="Comma-separated table names (default: all in SOURCES_ZIP)",
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

    print("Building ZIP table from SOURCES_ZIP...")
    df = build_zip_table(base_path, table_names=table_names)
    df.to_csv(output_path, index=False)
    print(f"Saved {len(df)} rows to {output_path}")


if __name__ == "__main__":
    main()
    sys.exit(0)
