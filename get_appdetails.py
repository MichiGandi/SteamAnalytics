import pandas as pd
import requests
from dotenv import load_dotenv
import os

load_dotenv()

# Settings
ALL_APPS_PATH = "data/all_apps.csv"
OUTPUT_DIR = "data/appdetails"
URL = "https://store.steampowered.com/api/appdetails/APPID"
params = {}


def main():
    print(f"Fetching appdetails from: {URL}")

    all_apps = pd.read_csv(ALL_APPS_PATH)
    print(f"Loaded {len(all_apps)} apps.")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    for appid in all_apps["appid"]:
        fetch_and_save(appid)


def fetch_and_save(appid):
    params["appids"] = appid
    try:
        response = requests.get(URL, params=params, timeout=4)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Received unexpected status code for app {appid}: {e}.")
        return

    data = pd.DataFrame(response.json())
    data.to_json(f"{OUTPUT_DIR}/{appid}.json", orient="records", indent=2)
    print(f"Fetched app {appid}.")


if __name__ == "__main__":
    main()