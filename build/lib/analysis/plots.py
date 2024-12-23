import matplotlib.pyplot as plt
import pandas as pd
from datetime import datetime, timedelta


def plot_weekly_trends(
    df: pd.DataFrame,
    title: str = "Weekly Direct Channel Signups (ONLINE and INSIDE SALES)",
) -> None:
    """
    Plots a line chart of weekly signups for two channels (ONLINE and INSIDE SALES) from a resampled DataFrame.

    Args:
        df (pd.DataFrame): DataFrame containing weekly signup data for "ONLINE" and "INSIDE SALES" channels.
        title (str): The title for the plot. Defaults to "Weekly Direct Channel Signups (ONLINE and INSIDE SALES)".

    Example:
        >>> plot_weekly_trends(weekly_direct_2024)
    """
    # Plot the line chart with markers
    ax = df.plot(
        kind="line",
        marker="o",
        figsize=(14, 6),
        title=title,
        ylabel="Weekly Signups, P90%",
        xlabel="",
    )

    # Set custom x-ticks for readability
    plt.xticks(
        ticks=df.index,
        labels=[x.strftime("%d-%b") for x in df.index],
        rotation=45,
        ha="right",
    )

    # Add legend
    plt.legend(loc="upper left")

    # Annotate points for "ONLINE"
    for x, y in zip(df.index, df["ONLINE"].values):
        plt.annotate(
            f"{y:.0f}",
            (x, y),
            textcoords="offset points",
            xytext=(0, 10),
            ha="center",
            color="C0",
        )

    # Annotate points for "INSIDE SALES"
    for x, z in zip(df.index, df["INSIDE SALES"].values):
        plt.annotate(
            f"{z:.0f}",
            (x, z),
            textcoords="offset points",
            xytext=(0, 10),
            ha="center",
            color="C1",
        )

    # Show the plot
    plt.show()


def plot_100_stacked_bar(
    df: pd.DataFrame, start_date: datetime, title: str = "100% Stacked Weekly Data"
) -> None:
    """
    Plots a 100% stacked bar chart of weekly data for two channels.

    Args:
        df (pd.DataFrame): DataFrame containing weekly counts for two channels (e.g., "ONLINE" and "INSIDE SALES").
        start_date (datetime): Start date for generating x-tick labels.
        title (str): The title for the plot. Defaults to "100% Stacked Weekly Data".

    Example:
        >>> start_date = datetime(2024, 1, 7)
        >>> plot_100_stacked_bar(weekly_direct_2024, start_date)
    """
    # Normalize data for 100% stacking
    df_normalized = df.div(df.sum(axis=1), axis=0)

    # Plot the normalized data as a stacked bar chart
    ax = df_normalized.plot(kind="bar", stacked=True, figsize=(14, 6), width=0.85)

    # Generate x-tick labels
    labels = [
        (start_date + timedelta(weeks=x)).strftime("%d-%b") for x in range(len(df))
    ]

    # Set x-ticks with generated labels
    plt.xticks(ticks=range(len(df.index)), labels=labels, rotation=45, ha="right")

    # Annotate each bar with percentage values
    for i, (index, row) in enumerate(df_normalized.iterrows()):
        cumulative = 0
        for j, (col_name, value) in enumerate(row.items()):
            if value > 0:  # Only annotate if there is a visible bar segment
                ax.text(
                    i,
                    cumulative + value / 2,
                    f"{value * 100:.1f}%",
                    ha="center",
                    va="center",
                    fontsize=8,
                    rotation=90,
                )
                cumulative += value

    # Final adjustments
    plt.legend(loc="upper left")
    plt.ylabel("Percentage")
    plt.title(title)
    plt.show()
