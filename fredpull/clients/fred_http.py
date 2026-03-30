import requests
import os
from dotenv import load_dotenv

load_dotenv()

BASE = "https://api.stlouisfed.org/fred"
API_KEY = os.getenv("FRED_API_KEY")

if API_KEY is None:
    raise RuntimeError("FRED_API_KEY missing")


def fred_get(endpoint, params=None):
    if params is None:
        params = {}

    params.update({
        "api_key": API_KEY,
        "file_type": "json"
    })

    r = requests.get(f"{BASE}/{endpoint}", params=params)
    r.raise_for_status()
    return r.json()