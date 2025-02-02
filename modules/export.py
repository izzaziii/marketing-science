from extract import read_and_clean_file
from transform import remove_lost_and_p75
from datetime import datetime
import pandas as pd


def export_to_csv(
    df: pd.DataFrame,
    save_path: str = f"{datetime.now().strftime(format='%Y%m%d_%H%M%S')}_export.csv",
):
    try:
        df.to_csv(save_path, index=True, index_label="id")
        print(f"File exported to {save_path}")
    except Exception as e:
        print(e)


def main():
    filepath: str = "/Users/izzaziskandar/Documents/1_Projects/Project Submarine - CRUD System for Data/BO Report.xlsx"
    df: pd.DataFrame = read_and_clean_file(filepath)
    transformed_df: pd.DataFrame = remove_lost_and_p75(df)
    export_to_csv(transformed_df)


if __name__ == "__main__":
    main()
