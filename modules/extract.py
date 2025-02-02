import pandas as pd


def clean_rows(
    df: pd.DataFrame,
    rows_to_keep: list[str] = [
        "Funn Status",
        "Funnel SO No",
        "Funnel Bandwidth",
        "Blk Cluster",
        "Blk State",
        "Probability 90% Date",
        " Channel",
        "Nationality",
        "Dob",
        "Billing Method",
    ],
) -> pd.DataFrame:
    """
    Transforms the dataframe generated into more manageable columns by eliminating the non-essential columns.
    Takes in the dataframe of the BO Report and list of rows to keep. Outputs a dataframe.
    """
    try:
        return (
            df[rows_to_keep]
            .astype(
                {
                    "Funn Status": "category",
                    "Funnel Bandwidth": "category",
                    "Blk State": "category",
                    "Probability 90% Date": "datetime64[ns]",
                    " Channel": "category",
                    "Billing Method": "category",
                }
            )
            .assign(
                state=df["Blk State"].str.strip().str.title().astype("category"),
                cluster=df["Blk Cluster"].str.strip().str.title().astype("category"),
                p90_date=pd.to_datetime(df["Probability 90% Date"], format="%Y-%m-%d"),
                channel=df[" Channel"],
            )
        )[
            [
                "Funn Status",
                "Funnel SO No",
                "Funnel Bandwidth",
                "cluster",
                "state",
                "p90_date",
                "channel",
                "Billing Method",
            ]
        ]
    except Exception as e:
        print(f"There was an error: {e}")


def read_and_clean_file(filepath: str) -> pd.DataFrame:
    """
    Directly transforms the excel file into a cleaned dataframe.

    Takes in a filepath as an input, and outputs the transformed dataframe.
    """
    try:
        df: pd.DataFrame = pd.read_excel(filepath)
        if not df.empty:
            return clean_rows(df)
        else:
            print("DataFrame is empty.")
    except Exception as e:
        print(f"There was an error: {e}")


def main(
    filepath: str = "/Users/izzaziskandar/Documents/1_Projects/Project Submarine - CRUD System for Data/BO Report.xlsx",
):
    return read_and_clean_file(filepath)


if __name__ == "__main__":
    main()
