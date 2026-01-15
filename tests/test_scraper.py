import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# import scraper
from src.scraper import scraper as scrp

def test_url():
    url = "https://www.datacentermap.com/usa"
    content = scrp.fetch(url)
    assert content is not None

def test_get_states():
    states = scrp.get_states("https://www.datacentermap.com/usa")
    assert len(states) > 0

def test_get_markets():
    markets = scrp.get_markets("https://www.datacentermap.com/usa/texas")
    assert len(markets) > 0

def test_get_datacenters():
    datacenters = scrp.get_datacenters("texas", "dallas", "https://www.datacentermap.com/usa/texas/dallas")
    assert len(datacenters) > 0