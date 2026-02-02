"""
Composable builders for constructing tables from SOURCES.

Supports three pipeline patterns:
- Reference table: zip_to_fips + fips_to_county joined
- ZIP table: zip-grain tables joined on zip_code (optionally with reference)
- County / All-in-one table: county-grain tables joined on county_fips
"""

from pathlib import Path

import pandas as pd

from src.configs.sources import SOURCES
from src.table_builder.reader import read, read_many


def build_reference(base_path: Path | None = None) -> pd.DataFrame:
    """Build the reference table: ZIP ↔ county mapping with county names.

    Joins zip_to_fips and fips_to_county. Result has: zip_code, county_fips,
    county_name, state_cap, res_ratio, business_ratio, other_ratio, total_ratio.

    Args:
        base_path: Project root for resolving data paths.

    Returns:
        DataFrame at ZIP–county grain (one row per zip–county pair with ratios).
    """
    zip_fips = read("zip_to_fips", base_path)
    fips_county = read("fips_to_county", base_path)
    ref = zip_fips.merge(fips_county, on="county_fips", how="left")
    return ref


def build_zip_table(
    table_names: list[str] | None = None,
    base_path: Path | None = None,
    include_reference: bool = False,
) -> pd.DataFrame:
    """Build a ZIP-based table by joining zip-grain tables on zip_code.

    Args:
        table_names: Tables to join (must have grain="zip"). Default: all zip tables.
        base_path: Project root.
        include_reference: If True, merge in reference (zip_code, county_fips, county_name, ratios).

    Returns:
        DataFrame at ZIP grain.
    """
    zip_tables = [n for n, s in SOURCES.items() if s.get("grain") == "zip" and n != "zip_to_fips"]
    to_load = table_names if table_names is not None else zip_tables
    for n in to_load:
        if SOURCES.get(n, {}).get("grain") != "zip":
            raise ValueError(f"Table '{n}' is not zip-grain. Use build_county_table for county tables.")

    dfs = read_many(to_load, base_path)
    out = None
    for name, df in dfs.items():
        if out is None:
            out = df
        else:
            out = out.merge(df, on="zip_code", how="outer")

    if include_reference:
        ref = build_reference(base_path)
        # Keep one row per zip (take max ratio or first); or keep all zip-county pairs
        ref_zip = ref.drop_duplicates(subset=["zip_code"], keep="first")
        out = out.merge(
            ref_zip[["zip_code", "county_fips", "county_name", "state_cap"]],
            on="zip_code",
            how="left",
        )
    return out


def build_county_table(
    table_names: list[str] | None = None,
    base_path: Path | None = None,
    reference: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build a county-based table by joining county-grain tables on county_fips.

    Tables with grain="county_state_name" are resolved to county_fips via reference
    (state + county_name -> county_fips from ref). Tables with grain="county_fips" join directly.

    Args:
        table_names: Tables to join. Default: all county tables.
        base_path: Project root.
        reference: Pre-built reference (zip_to_fips + fips_to_county). If None, built from sources.

    Returns:
        DataFrame at county_fips grain.
    """
    county_tables = [
        n for n, s in SOURCES.items()
        if s.get("grain") in ("county_fips", "county_state_name")
    ]
    to_load = table_names if table_names is not None else county_tables

    if reference is None:
        ref = build_reference(base_path)
    else:
        ref = reference

    # state+county_name -> county_fips (from ref: state_cap + county_name)
    state_county_to_fips = ref[["state_cap", "county_name", "county_fips"]].drop_duplicates()
    state_county_to_fips = state_county_to_fips.rename(columns={"state_cap": "state"})

    out = None
    for name in to_load:
        spec = SOURCES.get(name, {})
        grain = spec.get("grain")
        df = read(name, base_path)

        if grain == "county_state_name":
            df = df.merge(
                state_county_to_fips,
                on=["state", "county_name"],
                how="left",
            )
            if df["county_fips"].isna().all():
                raise ValueError(
                    f"Could not resolve county_fips for '{name}'. "
                    "Check state/county_name alignment with reference."
                )
        elif grain != "county_fips":
            raise ValueError(f"Table '{name}' has unexpected grain: {grain}")

        if out is None:
            out = df
        else:
            drop_cols = [c for c in df.columns if c in out.columns and c != "county_fips"]
            df_join = df.drop(columns=drop_cols, errors="ignore")
            out = out.merge(df_join, on="county_fips", how="outer")
    return out


def build_all_in_one(
    base_path: Path | None = None,
    county_tables: list[str] | None = None,
    zip_tables: list[str] | None = None,
    output_grain: str = "county",
) -> pd.DataFrame:
    """Build an all-in-one table: county features + ZIP features aggregated to county.

    County tables are joined on county_fips. ZIP tables are aggregated to county
    via reference (zip -> county_fips), using mean across ZIPs in each county.

    Args:
        base_path: Project root.
        county_tables: County tables to include. Default: all county tables.
        zip_tables: ZIP tables to include. Default: all zip tables (excl. zip_to_fips).
        output_grain: "county" (one row per county) or "zip" (one row per zip with county cols).

    Returns:
        DataFrame at county or zip grain.
    """
    ref = build_reference(base_path)

    county_list = county_tables or [
        n for n, s in SOURCES.items()
        if s.get("grain") in ("county_fips", "county_state_name")
    ]
    county_df = build_county_table(table_names=county_list, base_path=base_path, reference=ref)

    zip_list = zip_tables or [
        n for n, s in SOURCES.items()
        if s.get("grain") == "zip" and n != "zip_to_fips"
    ]
    zip_df = build_zip_table(table_names=zip_list, base_path=base_path, include_reference=False)

    ref_zip = ref[["zip_code", "county_fips", "county_name", "state_cap"]].drop_duplicates()

    if output_grain == "zip":
        merged = ref_zip.merge(zip_df, on="zip_code", how="left")
        merged = merged.merge(county_df, on="county_fips", how="left", suffixes=("", "_cty"))
        return merged

    # output_grain == "county": aggregate ZIP features to county (mean)
    merged = ref_zip.merge(zip_df, on="zip_code", how="inner")
    value_cols = [c for c in merged.columns if c not in ["zip_code", "county_fips", "county_name", "state_cap"]]
    if value_cols:
        zip_agg = merged.groupby("county_fips")[value_cols].mean().reset_index()
        county_df = county_df.merge(zip_agg, on="county_fips", how="left")
    return county_df
