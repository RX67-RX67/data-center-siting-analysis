import argparse
import sys
import csv
import json
from pathlib import Path
import time
import random

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.scraper import scraper as scrp

# Base URL for the scraper
BASE_URL = "https://www.datacentermap.com/usa"

# Checkpoint file for resumable scraping
CHECKPOINT_DIR = Path("data/checkpoints")


def load_checkpoint(state: str) -> dict:
    """Load checkpoint for a state. Returns dict with completed markets and datacenters."""
    cp_path = CHECKPOINT_DIR / f"checkpoint_{state}.json"
    if cp_path.exists():
        with open(cp_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"completed_markets": [], "datacenters": [], "rate_limited": False}


def save_checkpoint(state: str, checkpoint: dict):
    """Save checkpoint for a state."""
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    cp_path = CHECKPOINT_DIR / f"checkpoint_{state}.json"
    with open(cp_path, "w", encoding="utf-8") as f:
        json.dump(checkpoint, f, indent=2)


def clear_checkpoint(state: str):
    """Remove checkpoint file after successful completion."""
    cp_path = CHECKPOINT_DIR / f"checkpoint_{state}.json"
    if cp_path.exists():
        cp_path.unlink()


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
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from checkpoint (skip already-scraped markets)"
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Clear checkpoint and start fresh"
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
    print(f"Resume mode: {args.resume}")
    print("=" * 80)
    print()

    # Determine state name for checkpoint (use first state if single)
    state_key = args.states.strip().replace(",", "_") if args.states.strip() else "all"

    # Handle --reset
    if args.reset:
        clear_checkpoint(state_key)
        print(f"Cleared checkpoint for '{state_key}'")

    # Load checkpoint if resuming
    checkpoint = load_checkpoint(state_key) if args.resume else {"completed_markets": [], "datacenters": [], "rate_limited": False}
    completed_markets = set(checkpoint.get("completed_markets", []))
    all_datacenters = list(checkpoint.get("datacenters", []))

    if args.resume and completed_markets:
        print(f"Resuming: {len(completed_markets)} markets already completed, {len(all_datacenters)} datacenters loaded")

    rate_limited = False

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
                    market_key = f"{state_name}:{market_name}"

                    # Skip if already completed
                    if market_key in completed_markets:
                        print(f"    [{market_idx}/{len(markets)}] Skipping market (already done): {market_name}")
                        continue

                    print(f"    [{market_idx}/{len(markets)}] Processing market: {market_name}")

                    try:
                        datacenters = scrp.get_datacenters(state_name, market_name, market_url)

                        # Check for rate limiting (0 datacenters + title contains "Page View Limit")
                        if len(datacenters) == 0:
                            # Fetch page to check title (scraper already printed debug)
                            html = scrp.fetch(market_url)
                            if html and "Page View Limit" in html:
                                print(f"      RATE LIMITED: Page View Limit Reached. Stopping.")
                                rate_limited = True
                                break

                        print(f"      Found {len(datacenters)} datacenters")
                        all_datacenters.extend(datacenters)

                        # Mark market as completed and save checkpoint
                        completed_markets.add(market_key)
                        checkpoint = {
                            "completed_markets": list(completed_markets),
                            "datacenters": all_datacenters,
                            "rate_limited": False,
                        }
                        save_checkpoint(state_key, checkpoint)

                    except Exception as e:
                        print(f"      ERROR: Failed to get datacenters for {market_name}: {e}")
                        continue

                if rate_limited:
                    break

            except Exception as e:
                print(f"  ERROR: Failed to get markets for {state_name}: {e}")
                continue

            print()

            if rate_limited:
                break

        # Step 4: Save results to CSV
        print("=" * 80)
        print(f"Step 4: Saving {len(all_datacenters)} datacenters to {args.output}")
        print("=" * 80)

        if not all_datacenters:
            print("WARNING: No datacenters found. Nothing to save.")
            if rate_limited:
                checkpoint["rate_limited"] = True
                save_checkpoint(state_key, checkpoint)
                print(f"Rate limited. Re-run with --resume later to continue.")
                return 2
            return 1

        # Define CSV columns
        fieldnames = ["state", "market", "facility", "company", "street", "zip", "city", "source_url"]

        with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_datacenters)

        print(f"Successfully saved {len(all_datacenters)} datacenters to {args.output}")

        if rate_limited:
            checkpoint["rate_limited"] = True
            save_checkpoint(state_key, checkpoint)
            print(f"Rate limited. Re-run with --resume later to continue.")
            print("=" * 80)
            return 2

        # Clear checkpoint on successful completion
        clear_checkpoint(state_key)
        print("Checkpoint cleared (scrape complete).")
        print("=" * 80)
        return 0

    except KeyboardInterrupt:
        print("\n\nPipeline interrupted by user.")
        # Save checkpoint
        checkpoint = {
            "completed_markets": list(completed_markets),
            "datacenters": all_datacenters,
            "rate_limited": False,
        }
        save_checkpoint(state_key, checkpoint)
        print(f"Checkpoint saved. Re-run with --resume to continue.")

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
        # Save checkpoint on error
        checkpoint = {
            "completed_markets": list(completed_markets),
            "datacenters": all_datacenters,
            "rate_limited": False,
        }
        save_checkpoint(state_key, checkpoint)
        print(f"Checkpoint saved. Re-run with --resume to continue.")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)