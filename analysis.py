import calmap
from dotenv import load_dotenv
import matplotlib.pyplot as plt
from matplotlib import colors
from matplotlib.ticker import StrMethodFormatter
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
    review_heatmap(conn, "review_heatmap_01", review_min=100, review_max=10000)
    review_heatmap(conn, "review_heatmap_02", review_min=10, review_scale="log")
    release_calmap(conn, "release_calmap_01", range(2010, 2025 + 1))
    release_calmap(conn, "release_calmap_02", merge_years=True)
    revenue_distribution(conn, "revenue_distribution_01", revenue_scale="linear")
    revenue_distribution(conn, "revenue_distribution_01_log", revenue_scale="log")
    revenue_distribution(conn, "revenue_distribution_02", revenue_scale="linear", max_revenue=1000000)
    revenue_distribution(conn, "revenue_distribution_03", revenue_scale="log", tag_whitelist=[1091588], subtitle="Tag = Roguelike Deckbuilder")
    revenue_distribution(conn, "revenue_distribution_04", revenue_scale="log", tag_whitelist=[5432], subtitle="Tag = Programming")

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


def review_heatmap(conn, filename, review_min = 0, review_max = -1, review_scale="linear"):
    query = """
SELECT 
    (reviews).total_reviews AS total_reviews,
    (reviews).percent_positive AS percent_positive
FROM apps;
"""

    df = pd.read_sql(query, conn)
    df = df[df["total_reviews"] >= review_min]

    if (review_max < 0):
        review_max = df["total_reviews"].max()

    if review_scale=="linear":
        x_bins = np.linspace(0, review_max, 100)
    elif review_scale=="log":
        x_bins = np.logspace(np.log10(review_min), np.log10(review_max), 100)
    else:
        raise ValueError("Unknown review_scale")

    y_bins = np.linspace(0, 100, 101)

    plt.figure()
    plt.hist2d(
        df["total_reviews"],
        df["percent_positive"],
        bins=[x_bins, y_bins],
        cmap="plasma",
        norm=colors.LogNorm()
    )
    plt.colorbar(label="Number of Games")
    plt.xlim(review_min, review_max)
    plt.ylim(1, 100)
    plt.xscale(review_scale)
    plt.yscale("linear")
    plt.xlabel("Review Count")
    plt.ylabel("Percent Positive")
    plt.suptitle(f"Steam Reviews\n(only games with review count ≥ {review_min})")
    plt.savefig(get_full_filename(filename), dpi=300)


def release_calmap(conn, filename, years=None, merge_years=False, normalize_years=True, shorten_year_labels=False, force_year_labels=None):
    query = """
SELECT release_date FROM apps
WHERE release_date IS NOT NULL;
"""

    df = pd.read_sql(query, conn)

    df["release_date"] = pd.to_datetime(df["release_date"])
    daily_counts = df["release_date"].dt.date.value_counts()
    daily_counts.index = pd.to_datetime(daily_counts.index)
    daily_counts = daily_counts.sort_index()
    
    today = pd.Timestamp.today().normalize()
    daily_counts = daily_counts[daily_counts.index <= today]

    if years is None:
        years = range(daily_counts.index.year.min(), daily_counts.index.year.max() + 1)
    daily_counts = daily_counts[daily_counts.index.year.isin(years)]

    if merge_years:
        daily_counts.index = daily_counts.index.map(lambda d: d.replace(year=2000))
    elif normalize_years:
        daily_counts_normalized = pd.Series(dtype=float)
        for year in years:
            mask = daily_counts.index.year == year
            year_data = daily_counts[mask]
            year_data /= year_data.max()
            daily_counts_normalized = pd.concat([daily_counts_normalized, year_data])
        daily_counts = daily_counts_normalized

    fig, axes = calmap.calendarplot(
        daily_counts,
        fig_kws={"figsize": (plt.rcParams["figure.figsize"][0], 0.5 + 1.2 * daily_counts.index.year.nunique())},
        #yearlabels=force_year_labels or (not merge_years),
        yearlabel_kws={"fontsize": 24},
        dayticks=[0, 6],
        cmap="plasma",
        fillcolor="lightgray",
        #linecolor="purple",
        linewidth=0.25,
    )

    if force_year_labels is not None:
        ax = axes[0].set_ylabel(force_year_labels)
    elif merge_years:
        ax = axes[0].set_ylabel(f"{str(years[0])}-{str(years[-1])[-2:]}")

    if shorten_year_labels:
        for ax in axes.flatten():
            label = ax.get_ylabel()
            if label.isdigit():
                ax.set_ylabel(label[-2:])

    plt.suptitle("Steam Game Releases")
    plt.tight_layout()
    plt.savefig(get_full_filename(filename), dpi=300)


def revenue_distribution(conn, filename,
                         max_revenue=None,
                         revenue_scale="linear",
                         tag_whitelist=None,
                         tag_blacklist=None,
                         subtitle=None):
    query = f"""
SELECT revenue_estimate FROM apps_view
WHERE price IS NOT NULL
AND price > 0
AND tags_filter(tagids, '{assemble_list(tag_whitelist)}', '{assemble_list(tag_blacklist)}');
"""

    df = pd.read_sql(query, conn)
    print(len(df))
    revenue_sorted = np.sort(df["revenue_estimate"])
    x = np.arange(len(revenue_sorted))

    if max_revenue == None:
        max_revenue = revenue_sorted[-1]

    fig, ax = plt.subplots()
    ax.plot(range(len(revenue_sorted)), revenue_sorted)
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
    plt.suptitle(combine_subtitle("Revenue Distribution", subtitle))
    plt.tight_layout()
    plt.savefig(get_full_filename(filename), dpi=300)


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
    else:
        items = [str(item) for item in items]
    return "{" + ",".join(items) + "}"

def combine_subtitle(title, subtitle):
    if not subtitle:
        return title
    else:
        return f"{title}\n({subtitle})"

main()