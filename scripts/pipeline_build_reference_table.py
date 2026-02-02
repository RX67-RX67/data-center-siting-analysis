"""
Pipeline: Build the reference table from sources.py.

Constructs the reference table by joining zip_to_fips and fips_to_county
(defined in src/configs/sources.py) via table_builder.build_reference.
Output: zip_code, county_fips, county_name, state_cap, res_ratio, business_ratio,
other_ratio, total_ratio.
"""

import argparse
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from src.table_builder.builder import build_reference


def main():
    parser = argparse.ArgumentParser(
        description="Build reference table (ZIP ↔ county mapping) from sources.py"
    )
    parser.add_argument(
        "--output",
        type=str,
        default="data/processed_data/reference_table.csv",
        help="Output CSV path",
    )
    parser.add_argument(
        "--base-path",
        type=str,
        default=None,
        help="Project root for data paths (default: project root)",
    )
    args = parser.parse_args()

    base = Path(args.base_path) if args.base_path else project_root
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    print("Building reference table (zip_to_fips + fips_to_county)...")
    df = build_reference(base_path=base)
    df.to_csv(out_path, index=False)
    print(f"Saved {len(df)} rows to {out_path}")


if __name__ == "__main__":
    main()
    sys.exit(0)
