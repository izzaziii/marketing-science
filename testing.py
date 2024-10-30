from databases.connection import import_boreport_to_mongodb  # insert boreport to db
from databases.retrieval import generate_dataframe_from_database  # get data from db
from analysis.processing import BOReport
from analysis.plots import plot_weekly_trends, plot_100_stacked_bar

# Assume I already loaded from excel
# raw = import_boreport_to_mongodb("ftth", "deep-diver", "boreport")

# Read from database
df = generate_dataframe_from_database("deep-diver", "boreport", "Probability 90% Date")

# Process the database contents into information
sales = BOReport(df)  # The class to handle BO Report data

sales.prepare_sales_data_direct_channels()

weekly_sales = sales.resample_weekly_sales(year="2024")
df_channel, df_bandwidth, df_state, df_contract = (
    sales.resample_weekly_sales_by_columns()
)

# Plot

plot_weekly_trends(weekly_sales)
