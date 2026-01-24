import json
import time

from dotenv import load_dotenv
from tqdm import tqdm

from src import db_utility, paths

load_dotenv()


def main():
    conn, cur = db_utility.connect_to_db()

    authorids, processed_authors = get_authorids(conn)
    
    if processed_authors > 0:
        print(f"skipped {processed_authors} apps, because they already got executed.")
    
    create_processing_state_table(conn)
    write_apps_shared_reviewers(conn, authorids)
    
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
            

#def write_app_shared_reviewers(conn, appid):
def write_app_shared_reviewers(conn, authorid):
    SQL_CHECK = """
    SELECT 1
    FROM author_processing_state
    WHERE authorid = %s;
    """

    SQL_AUTHOR_APPS = """
    WITH author_apps AS (
        SELECT DISTINCT appid
        FROM reviews
        WHERE (author).steamid = %s
    )
    INSERT INTO app_shared_reviewers (appid1, appid2, shared_review_count)
    SELECT
        LEAST(a.appid, b.appid) AS appid1,
        GREATEST(a.appid, b.appid) AS appid2,
        1 AS shared_review_count
    FROM author_apps a
    JOIN author_apps b
      ON a.appid < b.appid
    ON CONFLICT (appid1, appid2)
    DO UPDATE
    SET shared_review_count = app_shared_reviewers.shared_review_count + 1;
    """

    SQL_MARK_DONE = """
    INSERT INTO author_processing_state (authorid)
    VALUES (%s)
    ON CONFLICT DO NOTHING;
    """

    try:
        with conn.cursor() as cur:
            cur.execute(SQL_CHECK, (authorid,))
            if cur.fetchone() is not None:
                return False

            cur.execute(SQL_AUTHOR_APPS, (authorid,))
            cur.execute(SQL_MARK_DONE, (authorid,))
        conn.commit()
        return True

    except Exception as e:
        conn.rollback()
        print(f"Error processing author {authorid}: {e}")
        raise


def get_authorids(conn):
    SQL = """
    SELECT DISTINCT (r.author).steamid
    FROM reviews r
    WHERE NOT EXISTS (
        SELECT 1
        FROM author_processing_state p
        WHERE p.authorid = (r.author).steamid
    )
    ORDER BY (r.author).steamid;
    """

    SQL_PROCESSED = """
    SELECT COUNT(*) 
    FROM author_processing_state;
    """

    with conn.cursor() as cur:
        cur.execute(SQL)
        authorids = [row[0] for row in cur.fetchall()]

        cur.execute(SQL_PROCESSED)
        processed_authors = cur.fetchone()[0]

    return authorids, processed_authors



if __name__ == "__main__":
    main()