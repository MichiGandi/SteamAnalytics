import json
from dotenv import load_dotenv
import os
from tqdm import tqdm
from src import db_utility
from src import paths


def main():
    load_dotenv()
    conn, cur = db_utility.connect_to_db()

    write_reviews(cur, conn)
        
    conn.commit()
    cur.close()
    conn.close()
    print("All store reviews written successfully!")


def write_reviews(cur, conn):
    json_files = [f for f in os.listdir(paths.REVIEWS_DIRECTORY) if f.endswith(".json")]

    for filename in tqdm(json_files, desc="Processing Reviews"):
        appid = int(os.path.splitext(filename)[0])
        filepath = os.path.join(paths.REVIEWS_DIRECTORY, filename)
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)

        for item in data:
            write_review(item, appid, cur)
        conn.commit()


def write_review(item, appid, cur):
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


main()