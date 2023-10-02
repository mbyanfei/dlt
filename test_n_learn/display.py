#%%
import duckdb
# from google.colab import data_table
# data_table.enable_dataframe_formatter()
#%%
# a database 'chess_pipeline.duckdb' was created in working directory so just connect to it
conn = duckdb.connect(f"{pipeline.pipeline_name}.duckdb")

# this lets us query data without adding schema prefix to table names
conn.sql(f"SET search_path = '{pipeline.dataset_name}'")

# list all tables
display(conn.sql("DESCRIBE"))

# stats_table = conn.sql("SELECT * FROM pokemon").df()
# display(stats_table)