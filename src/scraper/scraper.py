import time
import csv
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import re

# global variables
BASE_URL = "https://www.datacentermap.com/usa" 
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1"
}

REQUEST_DELAY = 3  
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds to wait after a 429 error

# functionality
def fetch(url, retries=MAX_RETRIES):
    """fetch url content with retry logic for rate limiting"""

    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            
            # Handle rate limiting (429) with exponential backoff
            if r.status_code == 429:
                if attempt < retries - 1:
                    wait_time = RETRY_DELAY * (2 ** attempt)  # exponential backoff
                    time.sleep(wait_time)
                    continue
                else:
                    r.raise_for_status()
            
            r.raise_for_status()
            time.sleep(REQUEST_DELAY)
            return r.text

        except requests.exceptions.RequestException as e:
            if attempt == retries - 1:
                raise
            time.sleep(RETRY_DELAY * (2 ** attempt))
    
    raise requests.exceptions.RequestException(f"Failed to fetch {url} after {retries} attempts")

def get_states(url):
    """get all states from the USA page"""

    html = fetch(url)
    soup = BeautifulSoup(html, "html.parser")

    states = []

    # Find the table containing states (more specific targeting)
    # Try to find links in the table tbody first
    table = soup.find("table", class_="ui sortable striped very basic very compact table")
    
    if table:
        # Look for links in table body
        tbody = table.find("tbody")
        if tbody:
            links = tbody.find_all("a", href=True)
        else:
            # Fallback to all links in table
            links = table.find_all("a", href=True)
    else:
        # Fallback: find all links that start with /usa/
        links = soup.find_all("a", href=lambda h: h and h.startswith("/usa/"))

    for a in links:
        href = a.get("href", "").strip()
        if not href:
            continue
        
        # Normalize href - handle both absolute and relative URLs
        if href.startswith("http"):
            # Full URL, extract the path
            parsed = urlparse(href)
            path = parsed.path
        else:
            path = href
        
        # Clean up path and split
        path = path.strip("/")
        parts = [p for p in path.split("/") if p]  # Remove empty strings
        
        # Check if it's a state link: /usa/{state} or /usa/{state}/
        if len(parts) == 2 and parts[0] == "usa":
            state = parts[1]
            # Build full URL
            state_url = urljoin(BASE_URL, f"/usa/{state}/")
            states.append((state, state_url))

    # Remove duplicates and sort
    return sorted(list(set(states)))

def get_markets(state_url):
    """fetch all the markets (cities) from the state page"""
    html = fetch(state_url)
    soup = BeautifulSoup(html, "html.parser")

    markets = []
    
    # Extract state from URL to match links
    parsed = urlparse(state_url)
    state_path = parsed.path.strip("/")
    state_parts = [p for p in state_path.split("/") if p]
    
    if len(state_parts) < 2 or state_parts[0] != "usa":
        return []
    
    state_name = state_parts[1]

    table = soup.find("table", class_="ui sortable striped very basic very compact table")

    if table and table.find("tbody"):
        links = table.find("tbody").find_all("a", href=True)
    else:
        links = soup.find_all("a", href=True)
    
    # Find all links that contain /usa/{state}/ in href
    for a in links:
        href = a.get("href", "").strip()
        if not href or f"/usa/{state_name}/" not in href:
            continue
        
        # Normalize href
        if href.startswith("http"):
            parsed_href = urlparse(href)
            path = parsed_href.path
        else:
            path = href
        
        # Clean up path and split
        path = path.strip("/")
        parts = [p for p in path.split("/") if p]
        
        # Check if it's a market link: /usa/{state}/{market} or /usa/{state}/{market}/
        BAD = {"quote", "contact", "about", "privacy", "terms"}
        if len(parts) >= 3 and parts[0] == "usa" and parts[1] == state_name and len(parts) == 3 and parts[2] not in BAD:
            market = parts[2]
            # Build full URL
            market_url_full = urljoin(BASE_URL, f"/usa/{state_name}/{market}/")
            markets.append((market, market_url_full))
        else:
            continue

    # Remove duplicates and sort
    return sorted(list(set(markets)))


def get_datacenters(state, market, market_url):
    """fetch data center locations"""
    html = fetch(market_url)
    soup = BeautifulSoup(html, "html.parser")

    results = []
    seen = set()

    cards = soup.select(".ui.cards a.card, .ui.cards a.ui.card, a.ui.card")

    if not cards:
        print("DEBUG 0 cards:", market_url)
        print("len(html)=", len(html))
        print("title=", soup.title.string if soup.title else None)
        print("doctype=", html.lstrip()[:15])
        return []

    for card in cards:

        href = card.get("href", "").strip()
        if not href:
            continue

        parts = [p for p in href.strip("/").split("/") if p]
        if len(parts) != 4 or parts[0] != "usa" or parts[1] != state or parts[2] != market:
            continue

        if href in seen:
            continue
        seen.add(href)

        header = card.select_one(".header")
        desc = card.select_one(".description")

        if not header or not desc:
            continue

        lines = [line.strip() for line in desc.get_text("\n").split("\n") if line.strip()]

        if len(lines) < 2:
            continue
    
        facility = header.get_text(strip=True)
        company = lines[0]
        street = lines[1]
        zip_code = None
        city = None

        # find zip code and city
        zip_idx = None
        for i in range(2, len(lines)):
            if re.match(r"^\d{5}(?:-\d{4})?$", lines[i]):
                zip_code = lines[i][:5]
                zip_idx = i
                break

        # if ZIP is on a separate line, city is likely on the next line
        if zip_idx is not None:
            # find the first text after zip that is not TX / USA / noise as city
            for j in range(zip_idx + 1, len(lines)):
                candidate = lines[j]
                # avoid treating "Suite 200" as city (can be expanded as needed)
                if re.match(r"^(suite|ste|unit|floor|fl)\b", candidate, flags=re.I):
                    continue
                city = candidate
                break
                    
        results.append({
            "state": state,
            "market": market,
            "facility": facility,
            "company": company,
            "street": street,
            "zip": zip_code,
            "city": city,
            "source_url": market_url
        })

    return results

