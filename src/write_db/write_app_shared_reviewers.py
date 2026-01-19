import json
import time

from dotenv import load_dotenv
from tqdm import tqdm

from src import db_utility, paths

load_dotenv()


def main():
    conn, cur = db_utility.connect_to_db()

    appids, skipped_apps = get_appids(conn)
    
    if skipped_apps > 0:
        print(f"skipped {skipped_apps} apps, because they already got executed.")
    
    create_processing_state_table(conn)
    write_apps_shared_reviewers(conn, appids)
    
    conn.commit()
    cur.close()
    conn.close()
    print("All JSON files imported successfully!")


def create_processing_state_table(conn):
    SQL = """
CREATE TABLE IF NOT EXISTS app_shared_reviewers_processing_state (
    processed_appid INT PRIMARY KEY
);
"""
    with conn.cursor() as cur:
        cur.execute(SQL)
        conn.commit()


def write_apps_shared_reviewers(conn, appids):
     
    
    with tqdm(total=len(appids), desc="Processing apps") as progress_bar:
        for appid in appids:
            progress_bar.set_description(f"Processing app {appid}")
            progress_bar.refresh()
            try:
                write_app_shared_reviewers(conn, appid)
            except RuntimeError as e:
                print(e)
                break
            progress_bar.update(1)
            

def write_app_shared_reviewers(conn, appid):
    SQL = """
WITH target_authors AS (
    SELECT DISTINCT (author).steamid AS authorid
    FROM reviews
    WHERE appid = %s
)
INSERT INTO app_shared_reviewers (appid1, appid2, shared_review_count)
SELECT
    LEAST(%s, r.appid) AS appid1,
    GREATEST(%s, r.appid) AS appid2,
    COUNT(DISTINCT (r.author).steamid) AS shared_review_count
FROM reviews r
JOIN target_authors t
  ON (r.author).steamid = t.authorid
WHERE r.appid <> %s
GROUP BY r.appid
ON CONFLICT (appid1, appid2)
DO UPDATE SET shared_review_count = EXCLUDED.shared_review_count;
"""

    SQL_CHECK = """
SELECT 1
FROM app_shared_reviewers_processing_state
WHERE processed_appid = %s;
"""

    SQL_PROGRESS_STATE = """
INSERT INTO app_shared_reviewers_processing_state(processed_appid)
VALUES (%s)
ON CONFLICT DO NOTHING;
"""

    try:
        with conn.cursor() as cur:
            cur.execute(SQL_CHECK, (appid,))
            if cur.fetchone() is not None:
                return False
            
            cur.execute(SQL, (appid, appid, appid, appid))
            cur.execute(SQL_PROGRESS_STATE, (appid,))
        conn.commit()
        return True
    
    except Exception as e:
        conn.rollback()
        print(f"Error processing app {appid}: {e}")
        raise


def get_appids(conn):
    SQL = """
SELECT a.appid
FROM apps a
WHERE NOT EXISTS (
    SELECT 1
    FROM app_shared_reviewers_processing_state p
    WHERE p.processed_appid = a.appid
)
ORDER BY a.appid;
"""
    SQL_SKIPPED = """
SELECT COUNT(*) AS processed_apps
FROM app_shared_reviewers_processing_state;
"""
    with conn.cursor() as cur:
        cur.execute(SQL)
        appids = [row[0] for row in cur.fetchall()]
        
        cur.execute(SQL_SKIPPED)
        skipped_apps = cur.fetchone()[0]
    
    return (appids, skipped_apps)
    

if __name__ == "__main__":
    main()