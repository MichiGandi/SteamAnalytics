import pandas as pd
import requests
from dotenv import load_dotenv
import json
import os

load_dotenv()

# Settings
ALL_APPS_PATH = "data/all_apps.csv"
OUTPUT_DIR = "data/storebrowse_items"
BATCH_SIZE = 100
URL = "https://api.steampowered.com/IStoreBrowseService/GetItems/v1"
params = {
    "ids": [],
    "context": {
        "language": "english",
        "country_code": "US",
        "steam_realm": 1
    },
    "data_request": {
        "include_assets": True,
        "include_release": True,
        "include_platforms": True,
        "include_all_purchase_options": True,
        "include_screenshots": True,
        "include_trailers": True,
        "include_ratings": True,
        "include_tag_count": 10000,
        "include_reviews": True,
        "include_basic_info": True,
        "include_supported_languages": True,
        "include_full_description": True,
        "include_included_items": True,
        "include_assets_without_overrides": True,
        "include_links": True
    }
}


def main():
    print(f"Fetching appdetails from: {URL}")

    all_apps = pd.read_csv(ALL_APPS_PATH)
    print(f"Loaded {len(all_apps)} apps.")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    for i in range(0, len(all_apps), BATCH_SIZE):
        appids = [int(appid) for appid in all_apps["appid"][i:i+BATCH_SIZE]]
        print(f"({i / len(all_apps) * 100:.2f} %) Fetched batch {appids[0]}...{appids[-1]}.")
        fetch_and_save(appids)


def fetch_and_save(appids):
    params["ids"].clear()
    params["ids"].extend([{"appid": a} for a in appids])
    for attempt in range(3):
        try:
            response = requests.get(URL, params={"input_json": json.dumps(params)}, timeout=4)
            response.raise_for_status()
            break
        except requests.exceptions.RequestException as e:
            print(f"Received unexpected status code ({appids[0]}...{appids[-1]}): {e}.")
    else:
        return

    data = pd.DataFrame(response.json())
    #data.to_json(f"{OUTPUT_DIR}/{appids[0]}-{appids[-1]}.json", orient="records", indent=2)
    with open(f"{OUTPUT_DIR}/{appids[0]}-{appids[-1]}.json", "w") as f:
        json.dump(response.json(), f, indent=2)


if __name__ == "__main__":
    main()