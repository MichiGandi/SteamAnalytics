import pandas as pd
import requests
from dotenv import load_dotenv
import json
import os
from src import paths

load_dotenv()

#Settings
OUTPUT_PATH = paths.TAG_LIST_PATH


def main():
    download_tag_list()


def download_tag_list():
    url = "https://api.steampowered.com/IStoreService/GetTagList/v1/"
    params = {
        "language": "english"
    }

    response = requests.get(url, params=params)
    response.raise_for_status()

    os.makedirs("data", exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(response.json(), f, indent=2)


if __name__ == "__main__":
    main()