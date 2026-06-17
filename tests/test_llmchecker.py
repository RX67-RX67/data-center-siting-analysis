import sys
from pathlib import Path
import pandas as pd
import tempfile
import os
import json
from unittest.mock import patch, Mock

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# import llm checker
from src.policy_finder.llm_checker import get_url, fetch_html, html_to_text, llm_checker

# test get_url function
def test_get_url():
    # Create a temporary CSV file with test data
    test_data = {
        "url": [
            "https://example.com/page1",
            "https://example.com/page2",
            "",  # empty string
            "  https://example.com/page3  ",  # with whitespace
            "https://example.com/page1",  # duplicate
            None,  # NA value
            "https://example.com/page4"
        ],
        "other_column": ["a", "b", "c", "d", "e", "f", "g"]
    }
    
    # Create temporary CSV file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        df = pd.DataFrame(test_data)
        df.to_csv(f.name, index=False)
        temp_path = f.name
    
    try:
        # Test the function
        urls = get_url(temp_path)
        
        # Assertions
        assert isinstance(urls, list)
        assert len(urls) == 4  # Should have 4 unique, non-empty URLs
        assert "https://example.com/page1" in urls
        assert "https://example.com/page2" in urls
        assert "https://example.com/page3" in urls  # whitespace should be stripped
        assert "https://example.com/page4" in urls
        assert "" not in urls  # empty strings should be removed
        assert urls.count("https://example.com/page1") == 1  # duplicates should be removed
    
    finally:
        # Clean up temporary file
        if os.path.exists(temp_path):
            os.unlink(temp_path)

# test fetch_html function
def test_fetch_html():
    # Mock the requests.get call
    mock_response = Mock()
    mock_response.text = "<html><body>Test HTML content</body></html>"
    mock_response.raise_for_status = Mock()  # Mock the raise_for_status method
    
    with patch('src.policy_finder.llm_checker.requests.get', return_value=mock_response) as mock_get:
        result = fetch_html("https://example.com/test")
        
        # Assertions
        assert result == "<html><body>Test HTML content</body></html>"
        assert mock_get.called
        assert mock_get.call_count == 1
        
        # Verify the call was made with correct arguments
        call_args = mock_get.call_args
        assert call_args[0][0] == "https://example.com/test"  # URL
        assert "headers" in call_args[1]  # Headers were passed
        assert "timeout" in call_args[1]  # Timeout was passed
        assert call_args[1]["timeout"] == 10  # Default timeout
        
        # Verify raise_for_status was called
        mock_response.raise_for_status.assert_called_once()

def test_fetch_html_custom_timeout():
    # Test with custom timeout
    mock_response = Mock()
    mock_response.text = "<html><body>Test</body></html>"
    mock_response.raise_for_status = Mock()
    
    with patch('src.policy_finder.llm_checker.requests.get', return_value=mock_response) as mock_get:
        result = fetch_html("https://example.com/test", timeout=30)
        
        assert result == "<html><body>Test</body></html>"
        call_args = mock_get.call_args
        assert call_args[1]["timeout"] == 30  # Custom timeout

# test html_to_text function
def test_html_to_text():
    html = "<html><body>Test HTML content</body></html>"
    result = html_to_text(html)
    assert result == "Test HTML content"

def test_html_to_text_removes_script_style():
    # Test that script, style, and noscript tags are removed
    html = """
    <html>
        <head>
            <script>alert('test');</script>
            <style>body { color: red; }</style>
        </head>
        <body>
            <noscript>Please enable JavaScript</noscript>
            <p>Visible content</p>
        </body>
    </html>
    """
    result = html_to_text(html)
    assert "alert" not in result
    assert "color: red" not in result
    assert "Please enable JavaScript" not in result
    assert "Visible content" in result

def test_html_to_text_collapses_newlines():
    # Test that multiple newlines are collapsed to double newlines
    html = """
    <html>
        <body>
            <p>Paragraph 1</p>
            
            
            <p>Paragraph 2</p>
        </body>
    </html>
    """
    result = html_to_text(html)
    # Should not have more than 2 consecutive newlines
    assert "\n\n\n" not in result
    assert "Paragraph 1" in result
    assert "Paragraph 2" in result

def test_html_to_text_collapses_spaces():
    # Test that multiple spaces/tabs are collapsed
    html = "<html><body><p>Text    with     multiple    spaces</p></body></html>"
    result = html_to_text(html)
    # Should not have more than one consecutive space
    assert "    " not in result
    assert "Text with multiple spaces" in result or "Text with multiple spaces" in result.replace("\n", " ")

def test_html_to_text_strips_whitespace():
    # Test that leading/trailing whitespace is stripped
    html = "   <html><body><p>Content</p></body></html>   "
    result = html_to_text(html)
    assert result == result.strip()  # Should be already stripped
    assert "Content" in result

# test llm_checker function
def test_llm_checker_success():
    # Mock the OpenAI API response
    mock_response = Mock()
    mock_response.choices = [Mock()]
    mock_response.choices[0].message = Mock()
    mock_response.choices[0].message.content = json.dumps({
        "mentioned_state": "Virginia",
        "mentioned_county": "Loudoun County",
        "is_data_center_policy": True,
        "policy_type": "zoning ordinance",
        "summary": "Loudoun County approved new data center zoning regulations",
        "llm_confidence": 0.95
    })
    
    with patch('src.policy_finder.llm_checker.CLIENT.chat.completions.create', return_value=mock_response):
        result = llm_checker("Loudoun County approved new data center zoning regulations in Virginia.")
        
        # Assertions
        assert isinstance(result, dict)
        assert result["mentioned_state"] == "Virginia"
        assert result["mentioned_county"] == "Loudoun County"
        assert result["is_data_center_policy"] is True
        assert result["policy_type"] == "zoning ordinance"
        assert "summary" in result
        assert "llm_confidence" in result
        assert result["llm_confidence"] == 0.95
        assert "error" not in result

def test_llm_checker_no_policy():
    # Test case where text is not about data center policy
    mock_response = Mock()
    mock_response.choices = [Mock()]
    mock_response.choices[0].message = Mock()
    mock_response.choices[0].message.content = json.dumps({
        "mentioned_state": None,
        "mentioned_county": None,
        "is_data_center_policy": False,
        "policy_type": None,
        "summary": "Text is about general technology news",
        "llm_confidence": 0.1
    })
    
    with patch('src.policy_finder.llm_checker.CLIENT.chat.completions.create', return_value=mock_response):
        result = llm_checker("General technology news article about cloud computing.")
        
        assert isinstance(result, dict)
        assert result["is_data_center_policy"] is False
        assert result["mentioned_state"] is None
        assert result["mentioned_county"] is None

def test_llm_checker_exception_handling():
    # Test that exceptions are caught and return fallback response
    with patch('src.policy_finder.llm_checker.CLIENT.chat.completions.create', side_effect=Exception("API Error")):
        result = llm_checker("Some text")
        
        # Should return fallback response
        assert isinstance(result, dict)
        assert result["mentioned_state"] is None
        assert result["mentioned_county"] is None
        assert result["is_data_center_policy"] is False
        assert result["policy_type"] is None
        assert result["summary"] == ""
        assert result["llm_confidence"] == 0.0
        assert "error" in result
        assert "API Error" in result["error"]