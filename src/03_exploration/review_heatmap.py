import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib import colors

from src import db_utility, exploration_utility


def review_heatmap(filename, review_min = 0, review_max = -1, review_scale="linear"):
    SQL = """
SELECT 
    (reviews).total_reviews AS total_reviews,
    (reviews).percent_positive AS percent_positive
FROM apps;
"""
    with db_utility.connect_to_db() as conn:
        df = pd.read_sql(SQL, conn)

    if review_max < 0:
        review_max = df["total_reviews"].max()

    if review_scale=="linear":
        x_bins = np.linspace(0, review_max, 100)
    elif review_scale=="log":
        review_min = max(review_min, 1)
        x_bins = np.logspace(np.log10(review_min), np.log10(review_max), 100)
    else:
        raise ValueError("Unknown review_scale")

    df = df[df["total_reviews"] >= review_min]
    y_bins = np.linspace(0, 100, 101)

    plt.figure()
    plt.hist2d(
        df["total_reviews"],
        df["percent_positive"],
        bins=[x_bins, y_bins],
        cmap="plasma",
        norm=colors.LogNorm(vmin=1)
    )
    plt.colorbar(label="Number of Games")
    plt.xlim(review_min, review_max)
    plt.ylim(0, 100)
    plt.xscale(review_scale)
    plt.yscale("linear")
    plt.xlabel("Review Count")
    plt.ylabel("Percent Positive")
    plt.suptitle(f"Steam Reviews\n(only games with review count ≥ {review_min})")
    plt.savefig(exploration_utility.get_full_filename(filename), dpi=300)
    plt.close()



if __name__ == "__main__":
    review_heatmap("review_heatmap_01.png", review_min=100, review_max=10000)
    review_heatmap("review_heatmap_02.png", review_min=10, review_scale="log")
