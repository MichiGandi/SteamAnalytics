import json

from src import db_utility, paths


def main():
    #load_dotenv()
    with db_utility.connect_to_db() as conn:
        write_tags(conn)
    print("All JSON files imported successfully!")


def write_tags(conn):
    with open(paths.TAGS_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    tags = data.get("response", {}).get("tags", [])
    with conn:
        for tag in tags:
            tagid = int(tag["tagid"])
            tagname = tag["name"]
            write_tag(conn, tagid, tagname)


def write_tag(conn, tagid, tagname):
    SQL = """
INSERT INTO tags2 (tagid, tagname)
VALUES (%s, %s)
ON CONFLICT (tagid) DO UPDATE
SET
    tagname = EXCLUDED.tagname;
"""
    with conn.cursor() as cur:
        cur.execute(SQL, (tagid, tagname))


if __name__ == "__main__":
    main()
