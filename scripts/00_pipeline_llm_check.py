import argparse
import sys
from pathlib import Path
import json

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.policy_finder import llm_checker as lch

# environment variables
CSV_PATH = "data/processed_data/county_candidates.csv"

# main function
def main():
    parser = argparse.ArgumentParser(
        description="LLM check pipeline"
    )
    parser.add_argument(
        "--output",
        type=str,
        default="data/processed_data/county_candidates_llm_check.json",
        help="Output json file path (default: data/processed_data/county_candidates_llm_check.json)"
    )
    parser.add_argument(
        "--max_chars",
        type=int,
        default=8000,
        help="Maximum number of characters to process each url(default: 8000)"
    )
    args = parser.parse_args()
    
    # pipeline
    urls = lch.get_url(CSV_PATH)

    results = {}

    for i, url in enumerate(urls):

        print(f"Processing URL {i+1} of {len(urls)}")

        try:
            text = lch.fetch_page_text(url, max_chars=args.max_chars)

            result = lch.llm_checker(text)
            
            # Add URL to the result for reference
            result["url"] = url
            
            # Store result in dictionary with URL as key
            results[url] = result

            print(f"Result: {result}")

        except Exception as e:
            # Handle errors gracefully
            error_result = {
                "url": url,
                "error": str(e),
                "mentioned_state": None,
                "mentioned_county": None,
                "is_data_center_policy": False,
                "policy_type": None,
                "summary": "",
                "llm_confidence": 0.0
            }
            results[url] = error_result
            print(f"Error processing {url}: {e}")

    # Write results to JSON file
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)  # Create directory if it doesn't exist
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    print(f"\nResults saved to: {output_path}")
    print(f"Total URLs processed: {len(results)}")


if __name__ == "__main__":
    main()

