import os
from fredapi import Fred
from dotenv import load_dotenv

load_dotenv()


def get_fred():
    key = os.getenv("FRED_API_KEY")

    if key is None:
        raise RuntimeError("FRED_API_KEY missing")

    return Fred(api_key=key)