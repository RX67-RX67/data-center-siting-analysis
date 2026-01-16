import argparse
import sys
from pathlib import Path
import time
import random
import csv

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.policy_finder import finder as fd


def write_counts_csv(counts, path: str):
    """Save county frequency table to CSV."""
    out_path = Path(path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["county", "count"])
        for county, cnt in counts.most_common():
            w.writerow([county, cnt])


def dedup_items_by_url(items: list[dict]) -> list[dict]:
    """Deduplicate organic results by URL."""
    seen = set()
    out = []
    for it in items:
        url = (it.get("url") or "").strip()
        if not url or url in seen:
            continue
        seen.add(url)
        out.append(it)
    return out


def main():
    parser = argparse.ArgumentParser(
        description="Search for counties related to data center policies (snippet-based filtering)."
    )

    # Output files
    parser.add_argument(
        "--output",
        type=str,
        default="data/processed_data/county_candidates.csv",
        help="Output CSV for evidence rows (default: data/processed_data/county_candidates.csv)",
    )
    parser.add_argument(
        "--output_counts",
        type=str,
        default="data/processed_data/county_frequency.csv",
        help="Output CSV for county frequency table (default: data/processed_data/county_frequency.csv)",
    )

    # Query control
    parser.add_argument(
        "--query",
        type=str,
        default=None,
        help="Single search query (e.g. 'data center county moratorium')",
    )
    parser.add_argument(
        "--queries_file",
        type=str,
        default=None,
        help="Path to a text file containing queries (one per line).",
    )
    parser.add_argument(
        "--topk",
        type=int,
        default=10,
        help="Top results per query (default: 10)",
    )

    # Rate limiting (good practice)
    parser.add_argument(
        "--sleep",
        type=float,
        default=1.5,
        help="Base sleep seconds between queries (default: 1.5)",
    )
    parser.add_argument(
        "--jitter",
        type=float,
        default=0.8,
        help="Random jitter seconds added to sleep (default: 0.8)",
    )

    args = parser.parse_args()

    # --------- 1) Build query list ----------
    queries = []
    if args.queries_file:
        qpath = Path(args.queries_file)
        if not qpath.exists():
            raise FileNotFoundError(f"queries_file not found: {qpath}")
        queries = [line.strip() for line in qpath.read_text(encoding="utf-8").splitlines() if line.strip()]
    elif args.query:
        queries = [args.query]
    else:
        # default query set (reasonable starter pack)
        queries = [
            "data center county zoning ordinance",
            "data center county moratorium",
            "data center county conditional use permit",
            'data center "County" ordinance',
            'data center "County" zoning amendment',
        ]

    # --------- 2) Run pipeline ----------
    all_items = []
    for q in queries:
        results = fd.search_policies(q, args.topk)
        items = fd.extract_organic_results(results)

        # attach query for traceability (optional but recommended)
        for it in items:
            it["query"] = q

        all_items.extend(items)

        # rate limit
        time.sleep(args.sleep + random.random() * args.jitter)

    # --------- 3) Deduplicate ----------
    unique_items = dedup_items_by_url(all_items)

    # --------- 4) Extract counties (evidence rows) ----------
    rows = fd.build_county_candidates(unique_items)

    # --------- 5) County frequency ----------
    counts = fd.count_counties(rows)

    # --------- 6) Save outputs ----------
    fd.save_to_csv(rows, args.output)
    write_counts_csv(counts, args.output_counts)

    print(f"[OK] queries={len(queries)}")
    print(f"[OK] items(total)={len(all_items)} items(unique)={len(unique_items)}")
    print(f"[OK] rows(evidence)={len(rows)} counties(unique)={len(counts)}")
    print(f"[OK] saved rows -> {args.output}")
    print(f"[OK] saved counts -> {args.output_counts}")


if __name__ == "__main__":
    main()
