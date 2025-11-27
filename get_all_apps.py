import pandas as pd
import requests
from dotenv import load_dotenv
import os

load_dotenv()

url = "https://api.steampowered.com/IStoreService/GetAppList/v1/"
params = {
    "key": os.getenv("STEAM_WEB_API_KEY"),
    "include_games": True,
    "include_dlc": False,
    "include_software": False,
    "include_videos": False,
    "include_hardware": False,
    "max_results": 50000
}

params["last_appid"] = 0
all_apps = []

max_requests = 20
request_count = 0
while True:
    request_count += 1
    print(f"request: {request_count}...")
    
    response = requests.get(url, params=params)
    response.raise_for_status()

    data = response.json()

    apps = data.get("response", {}).get("apps", [])
    all_apps.extend(apps)
    params["last_appid"] = data.get("response", {}).get("last_appid")

    have_more_results = data.get("response", {}).get("have_more_results")
    if not have_more_results:
        break

print(f"fetched {len(all_apps)} apps.")

df = pd.DataFrame(all_apps)

os.makedirs("data", exist_ok=True)
df.to_csv("data/all_apps.csv", index=False)