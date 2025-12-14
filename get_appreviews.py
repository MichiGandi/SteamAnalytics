import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import psycopg2
import requests
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

# Settings
ALL_APPS_PATH = "data/all_apps.csv"
OUTPUT_DIR = "data/appreviews"
MAX_REQUESTS = 15
APPIDS = None
URL = "https://store.steampowered.com/appreviews/"
PARAMS = {
        "json": 1,
        "filter": "recent",
        "language": "all",
        "cursor": "*",
        "num_per_page": 100,
    }
MAX_REQUEST_ATTEMPTS = 100

# globals
stop_event = threading.Event()
progress_bar = None
failed_appids = []


def main():
    print(f"Fetching appreviews from: {URL}")

    if not APPIDS:
        all_apps = pd.read_csv(ALL_APPS_PATH)
        total_reviews = get_total_reviews()
    else:
        all_apps = {"appid": APPIDS}
        total_reviews = get_total_reviews(all_apps["appid"])
        print(f"all_apps: {all_apps}")
    print(f"Loaded {len(all_apps)} apps.")
    print(f"Total Reviews to load: {total_reviews}.")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    executor = ThreadPoolExecutor(max_workers=MAX_REQUESTS)
    futures = []

    global progress_bar
    progress_bar = tqdm(
        total=total_reviews,
        desc="Reviews loaded",
        unit="reviews",
        ncols=100,
        bar_format='{desc}: {percentage:3.2f}%|{bar}{r_bar}'
    )

    try:
        for appid in all_apps["appid"]:
            output_file = os.path.join(OUTPUT_DIR, f"{appid}.json")

            if os.path.exists(output_file):
                try:
                    with open(output_file, "r", encoding="utf-8") as f:
                        existing_reviews = json.load(f)
                        review_count = len(existing_reviews)

                    progress_bar.update(review_count)

                except Exception as e:
                    tqdm.write(f"[App {appid}] Failed to read existing file, refetching: {e}")
                    futures.append(executor.submit(fetch_and_save, appid))
                continue

            futures.append(executor.submit(fetch_and_save, appid))

        for future in futures:
            while not future.done():
                time.sleep(0.5)
            try:
                future.result()
            except Exception as e:
                tqdm.write(f"Error in thread: {e}")

    except KeyboardInterrupt:
        stop_event.set()
        executor.shutdown(wait=False, cancel_futures=True)
        progress_bar.close()
        print("KeyboardInterrupt received, stopping immediately!")
        sys.exit(1)

    executor.shutdown()
    progress_bar.close()
    print(f"failed appids: {failed_appids}.")
    print("All tasks completed.")


def fetch_and_save(appid):
    app_url = URL + str(appid)
    app_params = PARAMS.copy()
    all_reviews = []
    total_reviews = None
    failed_requests = 0
    
    while True:
        if stop_event.is_set():
            return

        try:
            response = requests.get(app_url, params=app_params, timeout=20)
            response.raise_for_status()
            failed_requests = 0
        except requests.exceptions.RequestException as e:
            failed_requests += 1
            tqdm.write(f"  [App {appid}] Received unexpected status code (failed_requests = {failed_requests}): {e}.")
            if (failed_requests > MAX_REQUEST_ATTEMPTS):
                failed_appids.append(appid)

                with open(os.path.join(OUTPUT_DIR, f"{appid}-failed.json"), "w", encoding="utf-8") as f:
                    json.dump(all_reviews, f, indent=2)
                    f.write(f"\n\n{app_params["cursor"]}")

                tqdm.write(f"[App {appid}] Giving up after {MAX_REQUEST_ATTEMPTS} failed requests............................")
                return
            continue

        data = response.json()
        reviews = data.get("reviews", [])   
        all_reviews.extend(reviews)

        if not total_reviews:
            total_reviews = data['query_summary']['total_reviews']

        if app_params["cursor"] == data["cursor"]:
            break
        app_params["cursor"] = data["cursor"]

        global progress_bar
        progress_bar.update(len(reviews))

    with open(os.path.join(OUTPUT_DIR, f"{appid}.json"), "w", encoding="utf-8") as f:
        json.dump(all_reviews, f, indent=2)
    tqdm.write(f"  [App {appid}] Saved {len(all_reviews)} reviews.")


def get_total_reviews(appids: list[int] | None = None):
    conn = connect_to_db()
    with conn.cursor() as cur:
        if appids is None:
            cur.execute("""
                SELECT SUM((reviews).total_reviews)
                FROM apps;
            """)
        else:
            cur.execute("""
                SELECT SUM((reviews).total_reviews)
                FROM apps
                WHERE appid = ANY(%s);
            """, (appids,))
        total_reviews = cur.fetchone()[0]
    conn.close()
    return total_reviews


def connect_to_db():
    try:
        conn = psycopg2.connect(
            dbname="SteamAnalytics",
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host="localhost",
            port=5432
        )
        return conn
    except:
        print("failed to connect to DB.")
        raise


if __name__ == "__main__":
    main()