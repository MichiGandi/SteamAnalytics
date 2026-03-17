import calmap
import matplotlib.pyplot as plt
import pandas as pd

from src import db_utility, exploration_utility


def release_calmap(filename, years=None, merge_years=False, normalize_years=True, shorten_year_labels=False, force_year_labels=None):
    SQL = """
SELECT release_date FROM apps
WHERE release_date IS NOT NULL;
"""
    with db_utility.connect_to_db() as conn:
        df = pd.read_sql(SQL, conn)

    df["release_date"] = pd.to_datetime(df["release_date"])
    daily_counts = df.groupby(df["release_date"].dt.date).size()
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
        label = ax.get_ylabel()
        ax.set_ylabel(label[-2:] if label.isdigit() else label)

    plt.suptitle("Steam Game Releases")
    plt.tight_layout()
    plt.savefig(exploration_utility.get_full_filename(filename), dpi=300)



if __name__ == "__main__":
    release_calmap("release_calmap_01.png", range(2010, 2025 + 1))
    release_calmap("release_calmap_02.png", merge_years=True)
