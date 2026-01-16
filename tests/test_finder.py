import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# import policy finder
from src.policy_finder import finder as fd

def test_search_policies():
    results = fd.search_policies("data center county zoning ordinance")
    assert len(results) > 0

def test_extract_organic_results():
    results = fd.search_policies("data center county zoning ordinance")
    items = fd.extract_organic_results(results)
    assert len(items) > 0

