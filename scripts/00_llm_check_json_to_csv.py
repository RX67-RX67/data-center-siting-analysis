"""
Pipeline: Transform county_candidates_llm_check.json to CSV.

Reads the JSON object (url -> {mentioned_state, mentioned_county, is_data_center_policy, ...}),
emits one row per entry with all keys as columns. Writes to CSV under data/processed_data/
by default.
"""

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

DEFAULT_INPUT = "data/processed_data/county_candidates_llm_check.json"
DEFAULT_OUTPUT = "data/processed_data/county_candidates_llm_check.csv"


def main():
    parser = argparse.ArgumentParser(
        description="Convert county_candidates_llm_check.json to CSV (all keys kept)."
    )
    parser.add_argument(
        "--input",
        type=str,
        default=DEFAULT_INPUT,
        help=f"Input JSON path (default: {DEFAULT_INPUT})",
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

    base = Path(args.base_path) if args.base_path else project_root
    input_path = base / args.input
    output_path = base / args.output

    if not input_path.exists():
        print(f"Error: input not found: {input_path}", file=sys.stderr)
        sys.exit(1)

    with open(input_path, encoding="utf-8") as f:
        data = json.load(f)

    # JSON is url -> dict; one row per dict, keep all keys
    rows = []
    for _url, obj in data.items():
        row = dict(obj)
        rows.append(row)

    df = pd.DataFrame(rows)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False, encoding="utf-8")
    print(f"Wrote {len(df)} rows to {output_path}")


if __name__ == "__main__":
    main()
    sys.exit(0)
