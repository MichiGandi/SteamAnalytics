import json
import psycopg2
from datetime import datetime
from dotenv import load_dotenv
import os
from tqdm import tqdm

load_dotenv()

# Settings
DIRECTORY = "data/storebrowse_items"


def connect_to_db():
    try:
        conn = psycopg2.connect(
            dbname="SteamAnalytics",
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host="localhost",
            port=5432
        )
        cur = conn.cursor()
        return conn, cur
    except:
        print("failed to connect to DB.")
        raise
        
def escape_text(s):
    return s.replace('"', '\\"')

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


def process_app(item, cur):
    appid = item["appid"]
    name = item["name"]
    test = "A"
    # Reviews (taking summary_filtered)
    reviews_summary = item.get("reviews", {}).get("summary_filtered", {})
    total_reviews = reviews_summary.get("review_count", 0)
    percent_positive = reviews_summary.get("percent_positive", 0)
    review_score = reviews_summary.get("review_score", 0)
    
    # Release date
    release_timestamp = item.get("release", {}).get("steam_release_date")
    release_date = datetime.fromtimestamp(release_timestamp) if release_timestamp else None
    
    # Tags as weighted_tag[]
    tags = item.get("tags", [])
    tag_array_str = assemble_list([f'({t["tagid"]},{t["weight"]})' for t in tags], True)
    
    # Publishers & developers as TEXT[]
    publishers = item.get("basic_info", {}).get("publishers", [])
    publishers_array_str = assemble_list([f'"{escape_text(p["name"])}"' for p in publishers])

    developers = item.get("basic_info", {}).get("developers", [])
    developers_array_str = assemble_list([f'"{escape_text(d["name"])}"' for d in developers])
    
    # Insert into PostgreSQL
    sql = """
    INSERT INTO apps (appid, name, reviews, release_date, tags, publishers, developers)
    VALUES (
        %s,
        %s,
        ROW(%s,%s,%s)::review_summary,
        %s,
        %s::weighted_tag[],
        %s::text[],
        %s::text[]
    )
    ON CONFLICT (appid) DO NOTHING
    """
    cur.execute(sql, (
        appid,
        name,
        total_reviews,
        percent_positive,
        review_score,
        release_date,
        tag_array_str,
        publishers_array_str,
        developers_array_str
    ))


def process_file(data, cur):
    for item in data["response"]["store_items"]:
        process_app(item, cur)


def main():
    conn, cur = connect_to_db()

    json_files = [f for f in os.listdir(DIRECTORY) if f.endswith(".json")]

    for filename in tqdm(json_files, desc="Processing JSON files"):
        filepath = os.path.join(DIRECTORY, filename)
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
        process_file(data, cur)
            
            

    conn.commit()
    cur.close()
    conn.close()
    print("All JSON files imported successfully!")


main()