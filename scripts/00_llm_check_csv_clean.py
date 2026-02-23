"""
Pipeline: Clean county_candidates_llm_check.csv for downstream use.

- Drop rows missing mentioned_state or mentioned_county
- Normalize state to full name with capital first letter (e.g. Alabama)
- Normalize county to "Name County" form
- Keep only rows where is_data_center_policy is True
- Map support_data_center_siting: True -> 1, False -> -1
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
DEFAULT_OUTPUT = "data/processed_data/county_candidates_llm_check_clean.csv"
DEFAULT_REVIEW_OUTPUT = "data/processed_data/policy_county_human_review.csv"


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
        description="Clean LLM-check CSV: drop missing state/county, normalize names, filter policy=True, map support_siting to 1/-1."
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
        help=f"Output CSV (default: {DEFAULT_OUTPUT})",
    )
    parser.add_argument(
        "--output-review",
        type=str,
        default=DEFAULT_REVIEW_OUTPUT,
        help=f"Output CSV for county support/oppose counts (default: {DEFAULT_REVIEW_OUTPUT})",
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
    review_output_path = base / args.output_review

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

    # 4) Keep only is_data_center_policy == True
    if IS_POLICY_COL not in df.columns:
        print(f"Warning: column '{IS_POLICY_COL}' not found", file=sys.stderr)
    else:
        df = df[df[IS_POLICY_COL].apply(_is_true)]

    # 5) support_data_center_siting: True -> 1, False -> -1
    if SUPPORT_SITING_COL in df.columns:
        def map_siting(val):
            if pd.isna(val):
                return -1
            return 1 if _is_true(val) else -1
        df[SUPPORT_SITING_COL] = df[SUPPORT_SITING_COL].apply(map_siting)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False, encoding="utf-8")
    print(f"Wrote {len(df)} rows to {output_path}")

    # 6) County-level summary: list each (state, county) with counts of 1 and -1 for support_data_center_siting
    #    Filter to rows that have more than one support_count OR more than one oppose_count
    if SUPPORT_SITING_COL in df.columns:
        agg = df.groupby([STATE_COL, COUNTY_COL], as_index=False).agg(
            support_count=(SUPPORT_SITING_COL, lambda s: (s == 1).sum()),
            oppose_count=(SUPPORT_SITING_COL, lambda s: (s == -1).sum()),
        )
        agg = agg[(agg["support_count"] > 1) | (agg["oppose_count"] > 1)]
        review_output_path.parent.mkdir(parents=True, exist_ok=True)
        agg.to_csv(review_output_path, index=False, encoding="utf-8")
        print(f"Wrote {len(agg)} rows to {review_output_path}")


if __name__ == "__main__":
    main()
    sys.exit(0)
