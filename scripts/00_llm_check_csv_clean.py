"""
Pipeline: Clean county_candidates_llm_check.csv and output county-level policy signal table.

- Drop rows missing mentioned_state or mentioned_county
- Normalize state to full name with capital first letter (e.g. Alabama)
- Normalize county to "Name County" form
- Map support_data_center_siting: True -> 1, False -> -1, neutral -> 0
- Keep only rows where is_data_center_policy is True; treat as has_policy_signal
- Aggregate to one row per (state, county) with has_policy_signal=1 and
  policy_direction_score = mean(support_data_center_siting) over mentions
- Output CSV: mentioned_state, mentioned_county, has_policy_signal, policy_direction_score
"""

import argparse
import sys
from pathlib import Path

import pandas as pd

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

# US state/territory abbreviation -> full name (capital first letter)
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

STATE_COL = "mentioned_state"
COUNTY_COL = "mentioned_county"
IS_POLICY_COL = "is_data_center_policy"
SUPPORT_SITING_COL = "support_data_center_siting"

DEFAULT_INPUT = "data/processed_data/county_candidates_llm_check.csv"
DEFAULT_OUTPUT = "data/processed_data/county_policy_signal.csv"


def _normalize_state(s: str) -> str:
    """Return full state name with capital first letter. Handles abbr (e.g. IA) or full name."""
    if pd.isna(s) or not str(s).strip():
        return ""
    s = str(s).strip()
    upper = s.upper()
    if len(s) == 2 and upper in STATE_ABBR_TO_FULL:
        return STATE_ABBR_TO_FULL[upper]
    return s.title()


def _normalize_county(s: str) -> str:
    """Return 'Name County' form (title case, ensure ends with ' County')."""
    if pd.isna(s) or not str(s).strip():
        return ""
    s = str(s).strip().title()
    if not s.endswith(" County"):
        s = f"{s} County"
    return s


def _is_true(val) -> bool:
    if pd.isna(val):
        return False
    return str(val).strip().lower() in ("true", "1", "yes")


def main():
    parser = argparse.ArgumentParser(
        description="Clean LLM-check CSV and output county-level table: has_policy_signal, policy_direction_score."
    )
    parser.add_argument(
        "--input",
        type=str,
        default=DEFAULT_INPUT,
        help=f"Input CSV (default: {DEFAULT_INPUT})",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=DEFAULT_OUTPUT,
        help=f"Output county-level CSV (default: {DEFAULT_OUTPUT})",
    )
    parser.add_argument(
        "--base-path",
        type=str,
        default=None,
        help="Project root (default: script parent)",
    )
    args = parser.parse_args()

    base = Path(args.base_path) if args.base_path else project_root
    input_path = base / args.input
    output_path = base / args.output

    if not input_path.exists():
        print(f"Error: input not found: {input_path}", file=sys.stderr)
        sys.exit(1)

    df = pd.read_csv(input_path)

    # 1) Drop rows without mentioned_state or mentioned_county
    for col in (STATE_COL, COUNTY_COL):
        if col not in df.columns:
            print(f"Warning: column '{col}' not found", file=sys.stderr)
            sys.exit(1)
    df = df.dropna(subset=[STATE_COL, COUNTY_COL])
    df = df[df[STATE_COL].astype(str).str.strip() != ""]
    df = df[df[COUNTY_COL].astype(str).str.strip() != ""]

    # 2) Unify state to full name, capital first letter
    df[STATE_COL] = df[STATE_COL].astype(str).str.strip().apply(_normalize_state)

    # 3) Unify county to "Name County" form
    df[COUNTY_COL] = df[COUNTY_COL].astype(str).apply(_normalize_county)

    # 4) Keep only is_data_center_policy == True (drop False rows); conceptually becomes has_policy_signal
    if IS_POLICY_COL not in df.columns:
        print(f"Warning: column '{IS_POLICY_COL}' not found", file=sys.stderr)
    else:
        df = df[df[IS_POLICY_COL].apply(_is_true)]

    # 5) support_data_center_siting: True -> 1, False -> -1, neutral -> 0
    if SUPPORT_SITING_COL in df.columns:
        def map_siting(val):
            if pd.isna(val):
                return 0
            s = str(val).strip().lower()
            if s in ("true", "1", "yes"):
                return 1
            if s in ("false", "0", "no"):
                return -1
            if s == "neutral":
                return 0
            return 0
        df[SUPPORT_SITING_COL] = df[SUPPORT_SITING_COL].apply(map_siting)

    # 6) Aggregate to one row per county: has_policy_signal=1, policy_direction_score = mean(support_data_center_siting)
    if SUPPORT_SITING_COL not in df.columns:
        df[SUPPORT_SITING_COL] = 0
    out = df.groupby([STATE_COL, COUNTY_COL], as_index=False).agg(
        policy_direction_score=(SUPPORT_SITING_COL, "mean"),
    )
    out["has_policy_signal"] = 1
    out = out[[STATE_COL, COUNTY_COL, "has_policy_signal", "policy_direction_score"]]

    output_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(output_path, index=False, encoding="utf-8")
    print(f"Wrote {len(out)} rows to {output_path}")


if __name__ == "__main__":
    main()
    sys.exit(0)
