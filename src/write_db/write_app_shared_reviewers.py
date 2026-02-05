import json
import time

from dotenv import load_dotenv
from tqdm import tqdm

from src import db_utility, paths

load_dotenv()


def main():
    conn, cur = db_utility.connect_to_db()

    print("get authorid bins...")
    authorid_bins = get_authorid_bins(conn, 1000)
    print(f"authorid_bins: {authorid_bins}")
    
    write_apps_shared_reviewers(conn, authorid_bins)
    
    conn.commit()
    cur.close()
    conn.close()
    print("Finished.")


def create_processing_state_table(conn):
    SQL = """
CREATE TABLE IF NOT EXISTS app_shared_reviewers_processing_state (
    authorid BIGINT PRIMARY KEY
);
"""
    with conn.cursor() as cur:
        cur.execute(SQL)
        conn.commit()


def write_apps_shared_reviewers(conn, authorid_bins):
    with tqdm(
        total=len(authorid_bins),
        desc="Processing authorid bins",
        ncols=100,
        bar_format='{desc}{percentage:3.2f}%|{bar}{r_bar}'
        ) as progress_bar:
        for authorid_bin in authorid_bins:
            progress_bar.set_description(f"Processing {authorid_bin}")
            progress_bar.refresh()
            try:
                write_apps_shared_reviewers_range(conn, authorid_bin)
            except RuntimeError as e:
                print(e)
                break
            progress_bar.update(1)
            

def write_apps_shared_reviewers_range(conn, authorid_bin):
    SQL = """
INSERT INTO app_shared_reviewers (appid1, appid2, shared_review_count)
WITH a AS (
    SELECT DISTINCT
        (author).steamid AS steamid,
        appid
    FROM reviews
    WHERE (author).steamid > %(bin_start)s
      AND (author).steamid <= %(bin_end)s
)
SELECT
    LEAST(a1.appid, a2.appid) AS appid1,
    GREATEST(a1.appid, a2.appid) AS appid2,
    COUNT(*) AS shared_review_count
FROM a a1
JOIN a a2
  ON a1.steamid = a2.steamid
 AND a1.appid < a2.appid
GROUP BY appid1, appid2
ON CONFLICT (appid1, appid2)
DO UPDATE SET shared_review_count = app_shared_reviewers.shared_review_count + EXCLUDED.shared_review_count;
    """

    try:
        with conn.cursor() as cur:
            cur.execute(SQL, {"bin_start": authorid_bin[0], "bin_end": authorid_bin[1]})
        conn.commit()
        return True

    except Exception as e:
        conn.rollback()
        print(f"Error processing authorid bin {authorid_bin}: {e}")
        raise


def get_authorid_bins(conn, bin_count):
    SQL_BINS = """
WITH numbered AS (
    SELECT (author).steamid AS authorid,
            NTILE(%s) OVER (ORDER BY (author).steamid) AS bin_number
    FROM reviews r
    WHERE NOT EXISTS (
        SELECT 1
        FROM app_shared_reviewers_processing_state p
        WHERE p.authorid = (r.author).steamid
    )
)
SELECT
    MAX(authorid) AS range_end,
    COUNT(*) AS review_count
FROM numbered
GROUP BY bin_number
ORDER BY bin_number;
"""

    with conn.cursor() as cur:
        cur.execute(SQL_BINS, (bin_count,))
        bins = [row[0] for row in cur.fetchall()]

    bins = [0] + bins
    bins = [(bins[i], bins[i + 1]) for i in range(len(bins) - 1)]
    
    return bins


if __name__ == "__main__":
    main()
