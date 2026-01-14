import json
from dotenv import load_dotenv
from src import db_utility
from src import paths


def main():
    load_dotenv()
    conn, cur = db_utility.connect_to_db()

    write_tags(cur)
    
    conn.commit()
    cur.close()
    conn.close()
    print("All JSON files imported successfully!")


def write_tags(cur):
    with open(paths.TAGS_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    tags = data.get("response", {}).get("tags", [])
    for tag in tags:
        tagid = int(tag["tagid"])
        tagname = tag["name"]
        write_tag(cur, tagid, tagname)


def write_tag(cur, tagid, tagname):
    SQL = """
INSERT INTO tags2 (tagid, tagname)
VALUES (%s, %s)
ON CONFLICT (tagid) DO UPDATE
SET
    tagname = EXCLUDED.tagname;
"""
    cur.execute(SQL, (tagid, tagname))


if __name__ == "__main__":
    main()