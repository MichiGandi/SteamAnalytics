import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from src import db_utility, exploration_utility


def review_histogram(filename, max=100000, bin_width = 500):
    SQL = """
SELECT 
    appid,
    name,
    (reviews).total_reviews AS total_reviews,
    (reviews).percent_positive AS percent_positive,
    release_date
FROM apps;
"""
    with db_utility.connect_to_db() as conn:
        df = pd.read_sql(SQL, conn)
    
    reviews = df["total_reviews"]
    bins = np.arange(0, max + bin_width, bin_width)

    plt.figure()
    plt.hist(reviews, bins=bins)
    plt.xlabel("Total Reviews")
    plt.ylabel("Number of Games")
    plt.xlim(left=1, right=max)
    plt.xscale('linear')
    plt.yscale('log')
    plt.suptitle("Distribution of Review Counts")
    plt.savefig(exploration_utility.get_full_filename(filename), dpi=600)



if __name__ == "__main__":
    review_histogram("review_histogram.png")
