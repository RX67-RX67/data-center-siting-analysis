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

def test_extract_counties_basic():
    text = "Loudoun County approved a new data center ordinance."
    result = fd.extract_counties(text)

    assert result == {"Loudoun County"}

def test_extract_counties_multiple():
    text = (
        "Data centers are expanding in Loudoun County and "
        "Prince William County according to recent reports."
    )
    result = fd.extract_counties(text)

    assert result == {"Loudoun County", "Prince William County"}

def test_extract_counties_complex_names():
    text = (
        "Projects were approved in Los Angeles County, "
        "San Bernardino County, and St. Louis County."
    )
    result = fd.extract_counties(text)

    assert "Los Angeles County" in result
    assert "San Bernardino County" in result
    assert "St. Louis County" in result
    assert "Louis County" not in result

def test_extract_counties_no_false_positive():
    text = "The county-level policy discussion is ongoing."
    result = fd.extract_counties(text)

    assert result == set()

def test_build_county_candidates():

    mock_items = [
        {
            "title": "Prince William County considers data center moratorium",
            "snippet": "County officials discussed a temporary pause.",
            "url": "https://example.com/pw",
            "query": "data center moratorium",
        },
        {
            "title": "Fairfax County updates comprehensive plan",
            "snippet": "Fairfax County planning commission reviews data center impacts.",
            "url": "https://example.com/fairfax",
            "query": "fairfax county data center policy",
        },
    ]

    result = fd.build_county_candidates(mock_items)
    assert len(result) == 2

def test_count_counties():
    mock_rows = [
        {"county": "Prince William County"},
        {"county": "Fairfax County"},
    ]
    result = fd.count_counties(mock_rows)
    assert result == {"Prince William County": 1, "Fairfax County": 1}



