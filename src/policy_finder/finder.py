import os
from dotenv import load_dotenv
from serpapi import GoogleSearch
from collections import Counter
import pandas as pd
import re

load_dotenv()

# environment variables
API_KEY = os.getenv("SERPAPI_KEY")
TOKEN = r"(?:St\.|[A-Z][a-z]+)"
COUNTY_RE = re.compile(
    rf"""\b(
        (?:(?:New|North|South|East|West|Upper|Lower)\s+)?   # optional prefix
        {TOKEN}                                            # first token
        (?:                                                # following tokens
            (?:[-'\s]{TOKEN})                              # token joined by space/hyphen/apostrophe
          | (?:\s(?:San|Santa|Los|De|Del|La|Le|Du|Van|Von)) # particles
        )*
        \sCounty
    )\b""",
    re.VERBOSE
)


# function to search policies
def search_policies(query, topk=10):
    params = {
        "engine": "google",
        "q": query,
        "num": topk,
        "hl": "en",
        "gl": "us",
        "api_key": API_KEY,
    }

    search = GoogleSearch(params)
    results = search.get_dict()
    return results

# function to extract organic results
def extract_organic_results(results):
    items = []
    for r in results.get("organic_results", []):
        items.append({
            "title": r.get("title", ""),
            "snippet": r.get("snippet", ""),
            "url": r.get("link", ""),
        })
    return items

# function to extract counties from text
def extract_counties(text: str) -> set[str]:
    return set(m.strip() for m in COUNTY_RE.findall(text))

# function to build county candidates
def build_county_candidates(items):
    rows = []
    for item in items:
        text = f"{item['title']} {item['snippet']}"
        counties = extract_counties(text)

        for c in counties:
            rows.append({
                "county": c,
                "query": item.get("query", ""),
                "title": item["title"],
                "snippet": item["snippet"],
                "url": item["url"],
            })
    return rows

# function to count counties
def count_counties(rows):
    return Counter(r["county"] for r in rows)

# function to save to csv
def save_to_csv(rows, path="county_candidates.csv"):
    df = pd.DataFrame(rows)
    df.to_csv(path, index=False)

