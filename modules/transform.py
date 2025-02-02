from extract import read_and_clean_file
import pandas as pd


def remove_lost_and_p75(df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes LOST and non-P90 submissions.
    """
    if "Funn Status" in df.columns:
        return df.loc[df["Funn Status"] != "LOST"].dropna(subset="p90_date")
    else:
        print("No column Funn Status.")


def main():
    filepath: str = "/Users/izzaziskandar/Documents/1_Projects/Project Submarine - CRUD System for Data/BO Report.xlsx"
    df: pd.DataFrame = read_and_clean_file(filepath)
    return remove_lost_and_p75(df)


if __name__ == "__main__":
    main()
