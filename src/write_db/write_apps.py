import json
from decimal import Decimal
from datetime import datetime
from dotenv import load_dotenv
import os
from tqdm import tqdm
from src import db_utility
from src import paths


def main():
    load_dotenv()
    conn, cur = db_utility.connect_to_db()
    
    write_apps(cur, conn)

    conn.commit()
    cur.close()
    conn.close()
    print("All apps written successfully!")


def write_apps(cur, conn):
    json_files = [f for f in os.listdir(paths.STOREBROWSE_ITEMS_DIRECTORY) if f.endswith(".json")]
    print(len(json_files))
    for filename in tqdm(json_files, desc="Processing Apps"):
        filepath = os.path.join(paths.STOREBROWSE_ITEMS_DIRECTORY, filename)
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)

        for item in data["response"]["store_items"]:
            write_app(item, cur)
            conn.commit()


def write_app(item, cur):
    appid = item["appid"]
    name = item["name"]
    # Reviews (taking summary_filtered)
    reviews_summary = item.get("reviews", {}).get("summary_filtered", {})
    total_reviews = reviews_summary.get("review_count", 0)
    percent_positive = reviews_summary.get("percent_positive", 0)
    review_score = reviews_summary.get("review_score", 0)
    
    # Release date
    release_timestamp = item.get("release", {}).get("steam_release_date")
    release_date = datetime.fromtimestamp(release_timestamp) if release_timestamp else None
    
    # Tags as weighted_tagid[]
    tags = item.get("tags", [])
    tag_array_str = assemble_list([f'({t["tagid"]},{t["weight"]})' for t in tags], True)
    
    # Publishers & developers as TEXT[]
    publishers = item.get("basic_info", {}).get("publishers", [])
    publishers_array_str = assemble_list([f'"{escape_text(p["name"])}"' for p in publishers])
    developers = item.get("basic_info", {}).get("developers", [])
    developers_array_str = assemble_list([f'"{escape_text(d["name"])}"' for d in developers])

    price = item.get("best_purchase_option", {}).get("final_price_in_cents", 0)
    if price is not None:
        price = Decimal(price) / Decimal(100)

    # Insert into PostgreSQL
    SQL = """
INSERT INTO apps (appid, name, reviews, release_date, tagids, publishers, developers, price)
VALUES (
    %s,
    %s,
    ROW(%s,%s,%s)::review_summary,
    %s,
    %s::weighted_tagid[],
    %s::text[],
    %s::text[],
    %s
)
ON CONFLICT (appid) DO UPDATE
SET
    name = EXCLUDED.name,
    reviews = EXCLUDED.reviews,
    release_date = EXCLUDED.release_date,
    tagids = EXCLUDED.tagids,
    publishers = EXCLUDED.publishers,
    developers = EXCLUDED.developers,
    price = EXCLUDED.price;
"""
    cur.execute(SQL, (
        appid,
        name,
        total_reviews,
        percent_positive,
        review_score,
        release_date,
        tag_array_str,
        publishers_array_str,
        developers_array_str,
        price
    ))


def assemble_list(items, composite=False):
    """
    Convert a list into a PostgreSQL array string.
    
    - composite=True: each item is a composite, wrap in quotes: "(x,y)"
    - composite=False: plain array elements
    
    Examples:
    assemble_list(['"Valve"', '"Epic"']) -> '{"Valve","Epic"}'
    assemble_list(['(19,1623)','(1663,1454)'], composite=True) -> {"(19,1623)","(1663,1454)"}
    """
    if not items:
        return "{}"
    if composite:
        items = [f'"{item}"' for item in items]
    return "{" + ",".join(items) + "}"


def escape_text(s):
    return s.replace('"', '\\"')


if __name__ == "__main__":
    main()