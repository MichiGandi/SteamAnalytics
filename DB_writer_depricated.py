import json
import psycopg2
from decimal import Decimal
from datetime import datetime
from dotenv import load_dotenv
import os
import pandas as pd
from tqdm import tqdm


load_dotenv()

# Settings
STOREBROWSE_ITEMS_DIRECTORY = "data/storebrowse_items"
REVIEWS_DIRECTORY = "data/appreviews"
TAGS_FILE = "data/steam_tags.json"

def main():
    conn, cur = connect_to_db()

    store_apps(cur)
    store_tags(cur)
    store_reviews(cur, conn)
        
    conn.commit()
    cur.close()
    conn.close()
    print("All JSON files imported successfully!")


def store_apps(cur):
    json_files = [f for f in os.listdir(STOREBROWSE_ITEMS_DIRECTORY) if f.endswith(".json")]

    for filename in tqdm(json_files, desc="Processing Apps"):
        filepath = os.path.join(STOREBROWSE_ITEMS_DIRECTORY, filename)
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)

        for item in data["response"]["store_items"]:
            process_app(item, cur)


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


def store_tags(cur):
    with open(TAGS_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    tags = data.get("response", {}).get("tags", [])

    SQL = """
INSERT INTO tags (tagid, tagname)
VALUES (%s, %s)
ON CONFLICT (tagid) DO UPDATE
SET
    tagname = EXCLUDED.tagname;
"""

    for tag in tags:
        tagid = int(tag["tagid"])
        tagname = tag["name"]

        cur.execute(SQL, (tagid, tagname))


def store_reviews(cur, conn):
    json_files = [f for f in os.listdir(REVIEWS_DIRECTORY) if f.endswith(".json")]

    for filename in tqdm(json_files, desc="Processing Reviews"):
        appid = int(os.path.splitext(filename)[0])
        filepath = os.path.join(REVIEWS_DIRECTORY, filename)
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)

        for item in data:
            process_review(item, appid, cur)
        conn.commit()


def process_review(item, appid, cur):
    #tqdm.write(item["recommendationid"])
    author = item.get("author", {})

    cur.execute(
        """
        INSERT INTO reviews (
            recommendationid,
            appid,
            author,
            review,
            timestamp_created,
            timestamp_updated,
            voted_up,
            votes_funny,
            weighted_vote_score,
            comment_count,
            steam_purchase,
            received_for_free,
            written_during_early_access,
            primarily_steam_deck
        )
        VALUES (
            %(recommendationid)s,
            %(appid)s,
            ROW(
                %(steamid)s,
                %(num_games_owned)s,
                %(num_reviews)s,
                %(playtime_forever)s,
                %(playtime_last_two_weeks)s,
                %(playtime_at_review)s,
                to_timestamp(%(timestamp_created)s)
            )::review_author,
            %(review)s,
            to_timestamp(%(timestamp_created)s),
            to_timestamp(%(timestamp_updated)s),
            %(voted_up)s,
            %(votes_funny)s,
            %(weighted_vote_score)s,
            %(comment_count)s,
            %(steam_purchase)s,
            %(received_for_free)s,
            %(written_during_early_access)s,
            %(primarily_steam_deck)s
        )
        ON CONFLICT (recommendationid) DO NOTHING;
        """,
        {
            "recommendationid": int(item["recommendationid"]),
            "appid": appid,
            "steamid": int(author.get("steamid", 0)),
            "num_games_owned": author.get("num_games_owned"),
            "num_reviews": author.get("num_reviews"),
            "playtime_forever": author.get("playtime_forever"),
            "playtime_last_two_weeks": author.get("playtime_last_two_weeks"),
            "playtime_at_review": author.get("playtime_at_review"),
            "last_played": author.get("last_played"),
            "review": item.get("review"),
            "timestamp_created": item.get("timestamp_created"),
            "timestamp_updated": item.get("timestamp_updated"),
            "voted_up": item.get("voted_up"),
            "votes_funny": item.get("votes_funny"),
            "weighted_vote_score": item.get("weighted_vote_score"),
            "comment_count": item.get("comment_count"),
            "steam_purchase": item.get("steam_purchase"),
            "received_for_free": item.get("received_for_free"),
            "written_during_early_access": item.get("written_during_early_access"),
            "primarily_steam_deck": item.get("primarily_steam_deck"),
        },
    )


def store_game_distance_graph(conn, cur):
    query_condition = "WHERE appid < 100"
    query1 = f"""
WITH app_authors AS (
    SELECT
        appid,
        (author).steamid AS steamid
    FROM reviews
    {query_condition}
)
SELECT
    a1.appid AS appid_1,
    a2.appid AS appid_2,
    COUNT(*) AS weight
FROM app_authors a1
JOIN app_authors a2
  ON a1.steamid = a2.steamid
 AND a1.appid < a2.appid
GROUP BY a1.appid, a2.appid
HAVING COUNT(*) >= 5
ORDER BY weight DESC;
"""

    print("read DB...")
    df_weights = pd.read_sql(query1, conn)


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