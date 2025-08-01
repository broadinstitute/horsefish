import pandas as pd

expect_columns_new_header = ["hello_world", "Zenith", "Sojourn", "Intrigue"]

# initialize list of lists
Number_table_data = [[6], [8], [9], [8]]
Number_table_df = pd.DataFrame(Number_table_data, columns=["entity:Number"])
# Create the pandas DataFrame
sojourn_zenith_table_data = [[123.4, "GN0C8lnzi"], [2344,"p0Fk3hr6o6"], [234.098, "LIQ"], [223543.8, "9wrupKH"]]
sojourn_zenith_table_df = pd.DataFrame(sojourn_zenith_table_data, columns=["Sojourn","Zenith"])

Intrigue_table_data = [[""], ["4b"], ["7rv64X"], ["7V"]]
Intrigue_table_df = pd.DataFrame(Intrigue_table_data, columns=["Zenith"])

exp_column_groups = {
    "Example_table": Number_table_df,
    "table" : sojourn_zenith_table_df,
    "Stock": Intrigue_table_df      
}
