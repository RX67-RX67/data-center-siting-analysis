from dotenv import load_dotenv
import os
from openai import OpenAI
import requests
from bs4 import BeautifulSoup
import re
import json
from typing import Dict, Any
import pandas as pd

load_dotenv()

# environment variables
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
UA = "Mozilla/5.0 (compatible; DataCenterPolicyBot/1.0)"
DEFAULT_MAX_CHARS = 8000
CLIENT = OpenAI(api_key=OPENAI_API_KEY)

# get url from csv file
def get_url(path: str) -> list[str]:
    df = pd.read_csv(path)
    urls = (
        df["url"]
        .dropna()
        .astype(str)
        .str.strip()
    )
    urls = urls[urls != ""].drop_duplicates()
    return urls.tolist()

# get html through url
def fetch_html(url: str, timeout: int = 10):
    headers = {
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1"
    }
    response = requests.get(url, headers=headers, timeout=timeout)
    response.raise_for_status()
    return response.text

# make html content text
def html_to_text(html: str):
    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    text = soup.get_text(separator="\n")
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]{2,}", " ", text)
    return text.strip()

# fetch page text with max characters
def fetch_page_text(url: str, max_chars: int = DEFAULT_MAX_CHARS) -> str:
    html = fetch_html(url)
    text = html_to_text(html)
    return text[:max_chars]

# llm wrapper
def llm_checker(text:str) -> Dict[str, Any]:

    system_prompt = """
    You are an information extraction assistant.

    Your task:
    - Decide whether the input text is about data center related public policy.
    - Extract mentioned US county and state if present.

    Return ONLY a valid JSON object.
    Do NOT include markdown or explanations.
    """

    user_prompt = f"""
    Analyze the following text and return a JSON object with EXACTLY these keys:

    {{
    "mentioned_state": string or null,   
    "mentioned_county": string or null, 
    "is_data_center_policy": boolean,
    "policy_type": string or null,       
    "summary": string,                   
    "llm_confidence": number             
    }}

    Text:
    \"\"\"{text}\"\"\"
    """
    try:
        response = CLIENT.chat.completions.create(
            model="gpt-4o-mini",
            temperature=0.2,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            response_format={"type": "json_object"}
        )

        content = response.choices[0].message.content
        return json.loads(content)

    except Exception as e:
        # pipeline-safe fallback
        return {
            "mentioned_state": None,
            "mentioned_county": None,
            "is_data_center_policy": False,
            "policy_type": None,
            "summary": "",
            "llm_confidence": 0.0,
            "error": str(e)
        }




