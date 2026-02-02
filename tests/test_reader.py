"""Tests for src.table_builder.reader."""

import sys
from pathlib import Path

import pandas as pd
import pytest

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from src.table_builder.reader import (
    read,
    read_many,
    list_tables,
    _resolve_path,
    _read_file,
    _apply_filters,
    _apply_combine_columns,
    _apply_post_filters,
    _apply_normalize,
    _apply_special_values,
    _rename_and_select,
    _apply_aggregation,
    _apply_pivot,
)


# --- list_tables ---


def test_list_tables_returns_list():
    out = list_tables()
    assert isinstance(out, list)
    assert len(out) > 0


def test_list_tables_contains_expected():
    out = list_tables()
    assert "zip_to_fips" in out
    assert "fips_to_county" in out
    assert "transportation" in out
    assert "electricity_price" in out


# --- read (errors) ---


def test_read_unknown_table_raises():
    with pytest.raises(KeyError, match="Unknown table"):
        read("nonexistent_table")


def test_read_missing_file_raises():
    base = Path("/nonexistent/base")
    with pytest.raises(FileNotFoundError, match="Data not found|not found"):
        read("zip_to_fips", base_path=base)


# --- read_many ---


def test_read_many_unknown_table_raises():
    with pytest.raises(KeyError):
        read_many(["zip_to_fips", "nonexistent"])


def test_read_many_returns_dict():
    # Will raise FileNotFoundError if data missing; just check it returns dict when given valid names
    result = read_many([])
    assert result == {}


# --- _resolve_path ---


def test_resolve_path_relative_with_base():
    base = Path("/project")
    out = _resolve_path("data/raw/foo.csv", base)
    assert out == Path("/project/data/raw/foo.csv")


def test_resolve_path_relative_no_base():
    out = _resolve_path("data/foo.csv", None)
    assert out == Path("data/foo.csv")


def test_resolve_path_absolute_unchanged():
    base = Path("/project")
    out = _resolve_path("/absolute/path.csv", base)
    assert out == Path("/absolute/path.csv")


# --- _apply_filters ---


def test_apply_filters_scalar():
    df = pd.DataFrame({"a": [1, 2, 3], "b": [10, 20, 20]})
    out = _apply_filters(df, {"b": 20})
    assert len(out) == 2
    assert out["b"].tolist() == [20, 20]


def test_apply_filters_list():
    df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    out = _apply_filters(df, {"b": ["x", "z"]})
    assert len(out) == 2
    assert set(out["b"]) == {"x", "z"}


def test_apply_filters_missing_column_skipped():
    df = pd.DataFrame({"a": [1, 2, 3]})
    out = _apply_filters(df, {"nonexistent": 1})
    assert len(out) == 3


# --- _apply_combine_columns ---


def test_apply_combine_columns_concat_zfill():
    df = pd.DataFrame({
        "State FIPS Code": [1, 48],
        "County FIPS Code": [1, 201],
    })
    config = {
        "county_fips": {
            "from": ["State FIPS Code", "County FIPS Code"],
            "method": "concat_zfill",
            "zfill": [2, 3],
            "dtype": "string",
        }
    }
    out = _apply_combine_columns(df, config)
    assert "county_fips" in out.columns
    assert out["county_fips"].tolist() == ["01001", "48201"]


# --- _apply_post_filters ---


def test_apply_post_filters_not_ending_with():
    df = pd.DataFrame({"county_fips": ["01001", "01000", "48201"]})
    out = _apply_post_filters(df, {"county_fips_not_ending_with": "000"})
    assert len(out) == 2
    assert "01000" not in out["county_fips"].values


def test_apply_post_filters_unknown_raises():
    df = pd.DataFrame({"x": [1]})
    with pytest.raises(ValueError, match="Unknown post_filter"):
        _apply_post_filters(df, {"unknown_key": "x"})


# --- _apply_normalize ---


def test_apply_normalize_county_suffix():
    df = pd.DataFrame({"County": ["Fairfax County", "Prince William County", "St. Louis"]})
    keys = {"county_name": "County"}
    config = {"county_name": {"method": "remove anything after the county name including the word 'county'"}}
    out = _apply_normalize(df, config, keys)
    assert out["County"].tolist() == ["Fairfax", "Prince William", "St. Louis"]


# --- _apply_special_values ---


def test_apply_special_values_replace():
    df = pd.DataFrame({"jobs": ["<10", 50, 100], "name": ["A", "B", "C"]})
    config = {"<10": {"replace_with": 5}}
    out = _apply_special_values(df, config)
    assert out["jobs"].tolist() == [5, 50, 100]


# --- _rename_and_select ---


def test_rename_and_select():
    df = pd.DataFrame({"State": ["VA", "TX"], "County Name": ["Fairfax", "Dallas"], "extra": [1, 2]})
    keys = {"state": "State", "county_name": "County Name"}
    value_columns = {}
    out = _rename_and_select(df, keys, value_columns)
    assert list(out.columns) == ["state", "county_name"]
    assert out["state"].tolist() == ["VA", "TX"]


def test_rename_and_select_with_values():
    df = pd.DataFrame({"k": [1, 2], "v": [10, 20]})
    out = _rename_and_select(df, {"key": "k"}, {"val": "v"})
    assert list(out.columns) == ["key", "val"]
    assert out["val"].tolist() == [10, 20]


# --- _apply_aggregation ---


def test_apply_aggregation_groupby_mean():
    df = pd.DataFrame({
        "zip_code": ["001", "001", "002"],
        "price": [10.0, 20.0, 15.0],
    })
    agg_spec = {"groupby": ["zip_code"], "method": "mean"}
    out = _apply_aggregation(df, agg_spec, {}, {})
    assert len(out) == 2
    assert set(out["zip_code"]) == {"001", "002"}
    assert out.set_index("zip_code").loc["001", "price"] == 15.0


def test_apply_aggregation_with_value_columns():
    df = pd.DataFrame({
        "state": ["VA", "VA"],
        "county_name": ["A", "A"],
        "land_value_1_4_acre_standardized": [100, 200],
    })
    agg_spec = {
        "groupby": ["state", "county_name"],
        "method": "median",
        "value_columns": {"land_value_1_4_acre_standardized": "Land Value (1/4 Acre Lot, Standardized)"},
    }
    out = _apply_aggregation(df, agg_spec, {}, {})
    assert len(out) == 1
    assert out["land_value_1_4_acre_standardized"].iloc[0] == 150.0


# --- _apply_pivot ---


def test_apply_pivot_simple():
    df = pd.DataFrame({
        "id": [1, 1, 2],
        "tech": ["Fiber", "Cable", "Fiber"],
        "speed": [100, 50, 100],
    })
    pivot_spec = {"index": ["id"], "columns": "tech", "values": "speed", "rename": {"Fiber": "fiber", "Cable": "cable"}}
    out = _apply_pivot(df, pivot_spec, {})
    assert "id" in out.columns
    assert out.shape[0] == 2


# --- _read_file (CSV) ---


def test_read_file_csv(tmp_path):
    csv_path = tmp_path / "test.csv"
    csv_path.write_text("a,b\n1,2\n3,4")
    spec = {"format": "csv"}
    out = _read_file(csv_path, spec)
    assert list(out.columns) == ["a", "b"]
    assert out.shape == (2, 2)


def test_read_file_unsupported_format_raises(tmp_path):
    p = tmp_path / "x.parquet"
    p.touch()
    with pytest.raises(ValueError, match="Unsupported format"):
        _read_file(p, {"format": "parquet"})
