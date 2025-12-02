from dotenv import load_dotenv
import matplotlib.pyplot as plt
import numpy as np
import os
import psycopg2
import pandas as pd

load_dotenv()

# Settings
OUTPUT_DIR = "data/plots"
OUTPUT_FORMAT = "png"


def main():
    conn = connect_to_db()
    
    review_histogram(conn, "review_histogram")

    conn.close()


def review_histogram(conn, filename):
    query = """
SELECT 
    appid,
    name,
    (reviews).total_reviews AS total_reviews,
    (reviews).percent_positive AS percent_positive,
    release_date
FROM apps;
"""

    df = pd.read_sql(query, conn)

    reviews = df["total_reviews"]

    max = 100000
    bin_width = 500  # for example
    bins = np.arange(0, max + bin_width, bin_width)

    plt.figure()
    plt.hist(reviews, bins=bins)
    plt.xlabel("Total Reviews")
    plt.ylabel("Number of Games")
    plt.xlim(left=1, right=max)
    plt.xscale('linear')
    plt.yscale('log')
    plt.suptitle("Distribution of Review Counts")
    plt.savefig(get_full_filename(filename), dpi=600)


def connect_to_db():
    try:
        conn = psycopg2.connect(
            dbname="SteamAnalytics",
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host="localhost",
            port=5432
        )
        return conn
    except:
        print("failed to connect to DB.")
        raise

def get_full_filename(filename):
    return f"{OUTPUT_DIR}/{filename}.{OUTPUT_FORMAT}"


main()