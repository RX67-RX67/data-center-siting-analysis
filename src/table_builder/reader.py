"""
Generic reader: load any table from SOURCES config into a DataFrame.

Single entry point for all source types. Handles format (csv/xlsx),
filters, combine_columns, post_filters, normalize, aggregation, pivot, special_values.
"""

from pathlib import Path

import pandas as pd

from src.configs.sources import SOURCES


def _resolve_path(path_str: str, base_path: Path | None) -> Path:
    p = Path(path_str)
    if not p.is_absolute() and base_path is not None:
        return base_path / p
    return p


def _read_file(path: Path, spec: dict) -> pd.DataFrame:
    fmt = spec.get("format", "csv").lower()
    if fmt == "xlsx":
        read_kw: dict = {"engine": "openpyxl"}
        if "sheet" in spec:
            read_kw["sheet_name"] = spec["sheet"]
        if "skiprows" in spec:
            read_kw["skiprows"] = spec["skiprows"]
        return pd.read_excel(path, **read_kw)
    if fmt == "csv":
        return pd.read_csv(path)
    raise ValueError(f"Unsupported format: {fmt}")


def _apply_filters(df: pd.DataFrame, filters: dict) -> pd.DataFrame:
    for col, val in filters.items():
        if col not in df.columns:
            continue
        if isinstance(val, list):
            df = df[df[col].isin(val)]
        else:
            df = df[df[col] == val]
    return df


def _apply_combine_columns(df: pd.DataFrame, combine_config: dict) -> pd.DataFrame:
    df = df.copy()
    for out_col, spec in combine_config.items():
        from_cols = spec["from"]
        method = spec.get("method", "concat")
        zfill_list = spec.get("zfill", [2, 3])
        if method == "concat_zfill":
            parts = [
                df[c].astype(str).str.zfill(zfill_list[i] if i < len(zfill_list) else 0)
                for i, c in enumerate(from_cols)
            ]
            df[out_col] = parts[0]
            for i in range(1, len(parts)):
                df[out_col] = df[out_col].astype(str) + parts[i].astype(str)
        else:
            df[out_col] = df[from_cols[0]].astype(str)
            for c in from_cols[1:]:
                df[out_col] = df[out_col] + df[c].astype(str)
        if spec.get("dtype"):
            df[out_col] = df[out_col].astype(spec["dtype"])
    return df


def _apply_post_filters(df: pd.DataFrame, post_filters: dict) -> pd.DataFrame:
    for key, value in post_filters.items():
        if key.endswith("_not_ending_with"):
            col = key.replace("_not_ending_with", "")
            df = df[~df[col].astype(str).str.endswith(str(value))]
        else:
            raise ValueError(f"Unknown post_filter: {key}")
    return df


def _apply_normalize(df: pd.DataFrame, normalize_config: dict, keys: dict) -> pd.DataFrame:
    """Normalize key columns (e.g. strip ' County' suffix from county_name)."""
    df = df.copy()
    for out_key, spec in normalize_config.items():
        src_col = keys.get(out_key, out_key)
        if src_col not in df.columns:
            continue
        method = spec.get("method", "")
        if "remove anything after the county name including the word 'county'" in method:
            # Strip trailing " County", " Parish", etc.
            df[src_col] = df[src_col].astype(str).str.replace(
                r"\s+(County|Parish|Municipio)\s*$", "", case=False, regex=True
            )
    return df


def _apply_special_values(df: pd.DataFrame, special_values: dict) -> pd.DataFrame:
    for raw_val, spec in special_values.items():
        replace_with = spec.get("replace_with")
        for col in df.select_dtypes(include=["object", "number"]).columns:
            df[col] = df[col].replace(raw_val, replace_with)
        # Also handle if stored as string
        for col in df.columns:
            if df[col].dtype == object and df[col].astype(str).str.contains(raw_val, regex=False).any():
                df[col] = df[col].replace(raw_val, replace_with)
    return df


def _rename_and_select(df: pd.DataFrame, keys: dict, value_columns: dict) -> pd.DataFrame:
    rename = {}
    if keys:
        rename.update({v: k for k, v in keys.items()})
    if value_columns:
        rename.update({v: k for k, v in value_columns.items()})
    df = df.rename(columns=rename)
    keep = list((keys or {}).keys()) + list((value_columns or {}).keys())
    keep = [c for c in keep if c in df.columns]
    return df[keep].copy()


def _apply_aggregation(df: pd.DataFrame, agg_spec: dict, keys: dict, value_columns: dict) -> pd.DataFrame:
    groupby = agg_spec.get("groupby", [])
    method = agg_spec.get("method", "mean")
    agg_value_cols = agg_spec.get("value_columns")
    if agg_value_cols:
        # agg_value_cols: {output_name: raw_column_name}; after rename we use output names
        cols_to_agg = [c for c in agg_value_cols.keys() if c in df.columns]
        if not cols_to_agg:
            cols_to_agg = [c for c in agg_value_cols.values() if c in df.columns]
        groupby_in_df = [c for c in groupby if c in df.columns]
        if cols_to_agg and groupby_in_df:
            df = df.groupby(groupby_in_df, as_index=False)[cols_to_agg].agg(method)
    elif groupby:
        numeric_cols = [c for c in df.select_dtypes(include=["number"]).columns if c in df.columns]
        groupby_in_df = [c for c in groupby if c in df.columns]
        if groupby_in_df and numeric_cols:
            df = df.groupby(groupby_in_df, as_index=False)[numeric_cols].agg(method)
    return df


def _apply_pivot(df: pd.DataFrame, pivot_spec: dict, keys: dict) -> pd.DataFrame:
    index = pivot_spec.get("index", [])
    columns = pivot_spec.get("columns")
    values = pivot_spec.get("values")
    rename = pivot_spec.get("rename", {})
    flatten = pivot_spec.get("flatten_names", False)
    if isinstance(values, str):
        values = [values]
    index_in_df = [c for c in index if c in df.columns]
    if not index_in_df or columns not in df.columns or not values:
        return df
    values_in_df = [v for v in values if v in df.columns]
    if not values_in_df:
        return df
    pivoted = df.pivot_table(
        index=index_in_df,
        columns=columns,
        values=values_in_df[0] if len(values_in_df) == 1 else values_in_df,
        aggfunc="first",
    )
    if flatten and isinstance(pivoted.columns, pd.MultiIndex):
        pivoted.columns = ["_".join(str(c) for c in col).strip("_") for col in pivoted.columns]
    elif isinstance(pivoted.columns, pd.MultiIndex):
        pivoted.columns = [f"{col[1]}_{col[0]}" if len(col) == 2 else str(col) for col in pivoted.columns]
    pivoted = pivoted.reset_index()
    if rename:
        pivoted = pivoted.rename(columns=rename)
    return pivoted


def read(table_name: str, base_path: Path | None = None) -> pd.DataFrame:
    """Load a single table from SOURCES into a DataFrame.

    Args:
        table_name: Key in SOURCES (e.g. 'zip_to_fips', 'transportation', 'electricity_price').
        base_path: Project root for resolving relative paths.

    Returns:
        DataFrame with standardized column names.
    """
    if table_name not in SOURCES:
        raise KeyError(f"Unknown table '{table_name}'. Available: {list(SOURCES)}")
    spec = SOURCES[table_name]

    # Resolve path(s)
    if "sources" in spec:
        dfs = []
        for s in spec["sources"]:
            path = _resolve_path(s["path"], base_path)
            if not path.exists():
                raise FileNotFoundError(f"Data not found: {path}")
            dfs.append(_read_file(path, s))
        df = pd.concat(dfs, ignore_index=True)
    else:
        path = _resolve_path(spec["path"], base_path)
        if not path.exists():
            raise FileNotFoundError(f"Data not found: {path}")
        df = _read_file(path, spec)

    keys = spec.get("keys", {})
    value_columns = spec.get("value_columns", {})

    # Filters (before rename)
    if "filters" in spec:
        df = _apply_filters(df, spec["filters"])
    if "filter" in spec:
        df = _apply_filters(df, spec["filter"])

    # Combine columns (e.g. FIPS)
    if "combine_columns" in spec:
        df = _apply_combine_columns(df, spec["combine_columns"])

    # Normalize (before rename, uses raw col names via keys)
    if "normalize" in spec:
        df = _apply_normalize(df, spec["normalize"], keys)

    # Special values (before pivot/aggregation, while cols are raw)
    if "special_values" in spec:
        df = _apply_special_values(df, spec["special_values"])

    # Pivot (before rename, uses raw col names)
    if "pivot" in spec:
        df = _apply_pivot(df, spec["pivot"], keys)

    # Merge aggregation value_columns into rename (e.g. land_price)
    value_cols = dict(value_columns)
    if "aggregation" in spec and "value_columns" in spec["aggregation"]:
        value_cols = {**value_cols, **spec["aggregation"]["value_columns"]}
    # Pivot creates columns via rename; add those to keep list (e.g. labor_price)
    if "pivot" in spec and "rename" in spec["pivot"]:
        for v in spec["pivot"]["rename"].values():
            if v not in value_cols:
                value_cols[v] = v  # keep as-is

    # Rename and select
    df = _rename_and_select(df, keys, value_cols)

    # Aggregation (after rename; groupby uses output names)
    if "aggregation" in spec:
        df = _apply_aggregation(df, spec["aggregation"], keys, value_cols)

    # Post-filters
    if "post_filters" in spec:
        df = _apply_post_filters(df, spec["post_filters"])

    return df


def read_many(table_names: list[str], base_path: Path | None = None) -> dict[str, pd.DataFrame]:
    """Load multiple tables. Returns dict mapping table name to DataFrame."""
    return {name: read(name, base_path) for name in table_names}


def list_tables() -> list[str]:
    """Return all available table names from SOURCES."""
    return list(SOURCES)
