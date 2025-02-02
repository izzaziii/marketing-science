import pandas as pd
import matplotlib.pyplot as plt


def get_csv_data(filepath: str) -> pd.DataFrame:
    df: pd.DataFrame = pd.read_csv(filepath)
    df["p90_date"] = pd.to_datetime(df["p90_date"], format="%Y-%m-%d %H:%M:%S")
    return df


filepath: str = "/Users/izzaziskandar/Documents/1_Projects/Project Submarine - CRUD System for Data/data/raw/20250202_134952_export.csv"
df: pd.DataFrame = get_csv_data(filepath)

(
    df.assign(year=df["p90_date"].dt.year)
    .set_index("p90_date")
    .resample("ME")["Funnel SO No"]
    .count()
).plot(
    kind="line",
    marker="o",
    xlabel="Date",
    ylabel="No. of Signups",
    ylim=(0),
    title="Monthly Signups, All Channels",
    figsize=(10, 6),
)

(
    df.assign(year=df["p90_date"].dt.year, month=df["p90_date"].dt.month)
    .groupby(["year", "month"])["Funnel SO No"]
    .count()
    .unstack()
    .T.fillna(0)
)
