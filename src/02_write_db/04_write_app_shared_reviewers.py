from tqdm import tqdm

from src import db_utility


def main():
    try:
        with db_utility.connect_to_db() as conn:
            with tqdm(total=1, desc="1/3: Creating stage table", unit="step") as pbar:
                create_app_shared_reviewers_stage(conn)
                pbar.update(1)
            
            with tqdm(total=1, desc="2/3: Copying to main table", unit="step") as pbar:
                copy_to_app_shared_reviewers(conn)
                pbar.update(1)
            
            with tqdm(total=1, desc="3/3: Dropping stage table", unit="step") as pbar:
                drop_app_shared_reviewers_stage(conn)
                pbar.update(1)
    
    except ValueError as e:
        print(f"{e}")
    
    else:
        print("Calculated app shared reviewers successfully.")


def create_app_shared_reviewers_stage(conn):
    SQL = """
CREATE UNLOGGED TABLE app_shared_reviewers_stage AS
WITH a AS (
    SELECT DISTINCT
        (author).steamid AS steamid,
        appid
    FROM reviews
),
rc AS (
    SELECT appid, COUNT(*) AS review_count
    FROM reviews
    GROUP BY appid
)
SELECT
    LEAST(a1.appid, a2.appid)    AS appid1,
    GREATEST(a1.appid, a2.appid) AS appid2,
    rc1.review_count             AS reviews1,
    rc2.review_count             AS reviews2,
    COUNT(*)                     AS shared_reviewers
FROM a a1
JOIN a a2
  ON a1.steamid = a2.steamid
 AND a1.appid < a2.appid
JOIN rc rc1 ON rc1.appid = LEAST(a1.appid, a2.appid)
JOIN rc rc2 ON rc2.appid = GREATEST(a1.appid, a2.appid)
GROUP BY appid1, appid2, reviews1, reviews2;
"""
    with conn:
        with conn.cursor() as cur:
            cur.execute(SQL)


def copy_to_app_shared_reviewers(conn):
    SQL = """
CREATE TABLE IF NOT EXISTS app_shared_reviewers (
    appid1 INT,
    appid2 INT,
    reviews1 INT,
    reviews2 INT,
    shared_reviewers INT,
    PRIMARY KEY (appid1, appid2)
);

SET LOCAL synchronous_commit = OFF;

INSERT INTO app_shared_reviewers
SELECT * FROM app_shared_reviewers_stage;
"""
    with conn:
        with conn.cursor() as cur:
            cur.execute(SQL)


def drop_app_shared_reviewers_stage(conn):
    with conn:  # transaction ensures atomicity
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM app_shared_reviewers_stage;")
            stage_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM app_shared_reviewers;")
            main_count = cur.fetchone()[0]

            if stage_count != main_count:
                raise ValueError(
                    f"Won’t drop stage table: row count mismatch "
                    f"(app_shared_reviewers_stage has {stage_count} rows, "
                    f"app_shared_reviewers has {main_count} rows)."
                )
            
            cur.execute("DROP TABLE IF EXISTS app_shared_reviewers_stage;")



if __name__ == "__main__":
    main()
