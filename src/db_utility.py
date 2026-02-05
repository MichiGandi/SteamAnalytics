import os
from contextlib import contextmanager

import psycopg2
from dotenv import load_dotenv


@contextmanager
def connect_to_db():
    """Context manager for PostgreSQL connection."""
    load_dotenv()
    try:
        conn = psycopg2.connect(
            dbname="SteamAnalytics",
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host="localhost",
            port=5432)
        try:
            yield conn
        finally:
            conn.close()
    except Exception as e:
        print(f"Failed to connect to DB.\n{e}")
        raise e
    

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
