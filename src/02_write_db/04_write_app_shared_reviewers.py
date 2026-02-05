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
        (author).steamid AS authorid,
        appid
    FROM reviews
)
SELECT
    LEAST(a1.appid, a2.appid) AS appid1,
    GREATEST(a1.appid, a2.appid) AS appid2,
    COUNT(*) AS shared_review_count
FROM a a1
JOIN a a2
  ON a1.authorid = a2.authorid
 AND a1.appid < a2.appid
GROUP BY appid1, appid2;
"""
    with conn:
        with conn.cursor() as cur:
            cur.execute(SQL)


def copy_to_app_shared_reviewers(conn):
    SQL = """
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
