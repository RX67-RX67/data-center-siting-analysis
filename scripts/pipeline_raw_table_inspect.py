import argparse
import json
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.configs.sources_county_fips import SOURCES_COUNTY_FIPS
from src.configs.sources_county import SOURCES_COUNTY
from src.configs.sources_zip import SOURCES_ZIP
from src.raw_table_inspector.inspector import parse_config, inspect_dtypes

def inspect_all_sources(sources: dict, base_path: Path | None = None) -> dict:
    results = {}
    for name, cfg in sources.items():
        try:
            df = parse_config(cfg, base_path=base_path)
            dtype_table = inspect_dtypes(df)
            results[name] = dtype_table
        except Exception as e:
            results[name] = type(e).__name__ + ": " + str(e)
    return results

def _dataframe_to_markdown(df) -> str:
    """Format a DataFrame as a markdown table without requiring tabulate."""
    rows = [list(df.columns)]
    for _, r in df.iterrows():
        rows.append([str(x) for x in r])
    if not rows:
        return ""
    ncols = len(rows[0])
    widths = [max(len(str(rows[i][j])) for i in range(len(rows))) for j in range(ncols)]
    lines = []
    for i, row in enumerate(rows):
        line = "| " + " | ".join(str(x).ljust(widths[j]) for j, x in enumerate(row)) + " |"
        lines.append(line)
        if i == 0:
            sep = "| " + " | ".join(":" + "-" * max(2, w) for w in widths) + " |"
            lines.append(sep)
    return "\n".join(lines)


def inspect_to_markdown(results: dict) -> str:
    blocks = []
    for name, value in results.items():
        blocks.append(f"## {name}")
        if isinstance(value, str):
            blocks.append(f"`{value}`")
        else:
            blocks.append(_dataframe_to_markdown(value))
    return "\n\n".join(blocks)


def results_to_json(results: dict) -> dict:
    """Convert inspection results to a JSON-serializable dict."""
    out = {}
    for name, value in results.items():
        if isinstance(value, str):
            out[name] = {"error": value}
        else:
            out[name] = value.to_dict(orient="records")  # list of {"column": ..., "dtype": ...}
    return out

def main():
    parser = argparse.ArgumentParser(
        description="Inspect raw tables defined in source configs and report dtypes."
    )
    parser.add_argument(
        "--which",
        choices=["county_fips", "county", "zip", "all"],
        default="all",
        help="Which source dictionary to inspect.",
    )
    parser.add_argument(
        "--out",
        type=str,
        default="",
        help="Output file path. Use .json for JSON or .md for markdown; if empty, print to stdout.",
    )
    parser.add_argument(
        "--base-path",
        type=str,
        default=None,
        help="Project root for resolving relative paths (default: script's parent parent).",
    )
    args = parser.parse_args()

    base_path = Path(args.base_path) if args.base_path else project_root

    # Select sources
    selected = []
    if args.which in ("county_fips", "all"):
        selected.append(("county_fips", SOURCES_COUNTY_FIPS))
    if args.which in ("county", "all"):
        selected.append(("county", SOURCES_COUNTY))
    if args.which in ("zip", "all"):
        selected.append(("zip", SOURCES_ZIP))

    # Merge sources with namespace prefixes to avoid name collisions
    merged_sources = {}
    for prefix, src in selected:
        for name, cfg in src.items():
            key = f"{prefix}.{name}"
            if key in merged_sources:
                raise ValueError(f"Duplicate source key after prefixing: {key}")
            merged_sources[key] = cfg

    # Run inspection
    results = inspect_all_sources(merged_sources, base_path=base_path)
    as_json = args.out.lower().endswith(".json") if args.out else False

    # Output
    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        if as_json:
            payload = results_to_json(results)
            out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        else:
            out_path.write_text(inspect_to_markdown(results), encoding="utf-8")
        print(f"Wrote dtype report to: {out_path}")
    else:
        if as_json:
            print(json.dumps(results_to_json(results), indent=2))
        else:
            print(inspect_to_markdown(results))

if __name__ == "__main__":
    main()
