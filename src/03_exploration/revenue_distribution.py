import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.ticker import StrMethodFormatter

from src import db_utility, exploration_utility


def revenue_distribution(filename,
                         max_revenue=None,
                         revenue_scale="linear",
                         tag_whitelist=None,
                         tag_blacklist=None,
                         subtitle=None):
    SQL = f"""
SELECT revenue_estimate FROM apps_view
WHERE price IS NOT NULL
AND price > 0
AND tags_filter(tagids, '{db_utility.assemble_list(tag_whitelist)}', '{db_utility.assemble_list(tag_blacklist)}');
"""
    with db_utility.connect_to_db() as conn:
        df = pd.read_sql(SQL, conn)
    
    if df.empty:
        print("No data found for the given filters.")
        return
    
    revenue_sorted = np.sort(df["revenue_estimate"])
    x = np.arange(len(revenue_sorted))

    if max_revenue is None:
        max_revenue = revenue_sorted[-1]

    fig, ax = plt.subplots()
    ax.plot(x, revenue_sorted)
    plt.xscale("linear")
    plt.yscale(revenue_scale)
    ax.set_xticks(np.percentile(x, np.arange(0, 101, 10)))
    ax.axvline(np.percentile(x, 50), color='gray', linestyle='-', linewidth=0.8, alpha=0.7)
    for i in np.linspace(0, 100, 11):
        if (i == 50):
            continue
        ax.axvline(np.percentile(x, i), color='gray', linestyle='--', linewidth=0.8, alpha=0.7)
    plt.xlim(0, len(df) - 1)
    plt.ylim(0, max_revenue)
    ax.set_xticklabels([f"{i}%" for i in range(0, 101, 10)])
    ax.yaxis.set_major_formatter(StrMethodFormatter('${x:,.0f}'))
    ax.set_xlabel("Number of Games")
    ax.set_ylabel("Revenue Estimate")
    plt.suptitle(exploration_utility.combine_lines("Revenue Distribution", subtitle))
    plt.tight_layout()
    plt.savefig(exploration_utility.get_full_filename(filename), dpi=300)



if __name__ == "__main__":
    revenue_distribution("revenue_distribution_01.png", revenue_scale="linear")
    revenue_distribution("revenue_distribution_01_log.png", revenue_scale="log")
    revenue_distribution("revenue_distribution_02.png", revenue_scale="linear", max_revenue=1000000)
    revenue_distribution("revenue_distribution_03.png", revenue_scale="log", tag_whitelist=[1091588], subtitle="Tag = Roguelike Deckbuilder")
    revenue_distribution("revenue_distribution_04.png", revenue_scale="log", tag_whitelist=[5432], subtitle="Tag = Programming")
