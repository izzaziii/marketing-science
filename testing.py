from databases.read import generate_dataframe_from_database
from analysis.processing import BOReport
from analysis.plots import plot_weekly_trends, plot_100_stacked_bar
from datetime import datetime

# Read from database
df = generate_dataframe_from_database("deep-diver", "boreport", "Probability 90% Date")

sales = BOReport(df)

sales.prepare_sales_data_direct_channels()

weekly_sales = sales.resample_weekly_sales("2024")

weekly_channel, weekly_product, weekly_state, weekly_contract = (
    sales.resample_weekly_sales_by_columns()
)

plot_weekly_trends(weekly_sales)

plot_100_stacked_bar(weekly_sales, datetime(2024, 1, 1))
