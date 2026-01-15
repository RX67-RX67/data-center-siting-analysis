import argparse
import sys
import csv
from pathlib import Path
import time
import random

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.scraper import scraper as scrp

# Base URL for the scraper
BASE_URL = "https://www.datacentermap.com/usa"


def main():
    """Main pipeline function to scrape all datacenters from datacentermap.com"""
    parser = argparse.ArgumentParser(
        description="Scrape datacenter locations from datacentermap.com"
    )
    parser.add_argument(
        "--output",
        type=str,
        default="data/processed_data/datacenter_list.csv",
        help="Output CSV file path (default: data/processed_data/datacenter_list.csv)"
    )
    parser.add_argument(
        "--base-url",
        type=str,
        default=BASE_URL,
        help=f"Base URL to scrape from (default: {BASE_URL})"
    )
    parser.add_argument(
        "--states",
        type=str,
        default="",
        help="Comma-separated state slugs to scrape (e.g., 'texas,virginia'). Empty means all."
    )
    args = parser.parse_args()

    # Create output directory if it doesn't exist
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    print("=" * 80)
    print("Data Center Scraping Pipeline")
    print("=" * 80)
    print(f"Base URL: {args.base_url}")
    print(f"Output file: {args.output}")
    print("=" * 80)
    print()

    all_datacenters = []

    try:
        # Step 1: Get all states
        print("Step 1: Fetching all states...")
        states = scrp.get_states(args.base_url)
        print(f"Found {len(states)} states")
        print()

        if args.states.strip():
            wanted = {s.strip().lower() for s in args.states.split(",") if s.strip()}
            states = [(name, url) for (name, url) in states if name in wanted]
            print(f"Filtered to {len(states)} states: {sorted(wanted)}")

            if not states:
                print("ERROR: None of the requested states were found. Exiting.")
                return 1

        # Step 2: For each state, get markets
        total_states = len(states)
        for state_idx, (state_name, state_url) in enumerate(states, 1):
            print(f"[{state_idx}/{total_states}] Processing state: {state_name}")
            
            try:
                markets = scrp.get_markets(state_url)
                print(f"  Found {len(markets)} markets in {state_name}")
                
                if not markets:
                    print(f"  Warning: No markets found for {state_name}")
                    continue

                # Step 3: For each market, get datacenters
                for market_idx, (market_name, market_url) in enumerate(markets, 1):
                    print(f"    [{market_idx}/{len(markets)}] Processing market: {market_name}")
                    
                    try:
                        datacenters = scrp.get_datacenters(state_name, market_name, market_url)
                        print(f"      Found {len(datacenters)} datacenters")
                        all_datacenters.extend(datacenters)
                    except Exception as e:
                        print(f"      ERROR: Failed to get datacenters for {market_name}: {e}")
                        continue
                        
            except Exception as e:
                print(f"  ERROR: Failed to get markets for {state_name}: {e}")
                continue
            
            print()

        # Step 4: Save results to CSV
        print("=" * 80)
        print(f"Step 4: Saving {len(all_datacenters)} datacenters to {args.output}")
        print("=" * 80)
        
        if not all_datacenters:
            print("ERROR: No datacenters found. Nothing to save.")
            return 1

        # Define CSV columns
        fieldnames = ["state", "market", "facility", "company", "street", "zip", "city", "source_url"]
        
        with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_datacenters)

        print(f"Successfully saved {len(all_datacenters)} datacenters to {args.output}")
        print("=" * 80)
        return 0

    except KeyboardInterrupt:
        print("\n\nPipeline interrupted by user.")
        if all_datacenters:
            print(f"Saving {len(all_datacenters)} datacenters collected so far...")
            fieldnames = ["state", "market", "facility", "company", "street", "zip", "city", "source_url"]
            with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(all_datacenters)
            print(f"Partial results saved to {args.output}")
        return 1
    except Exception as e:
        print(f"\nERROR: Pipeline failed with error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)