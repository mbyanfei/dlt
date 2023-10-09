#%%
import dlt
import duckdb
import pandas as pd
# from google.colab import data_table
# data_table.enable_dataframe_formatter()
#%%
# data = [
#     {'id': 1, 'name': 'Alice'},
#     {'id': 2, 'name': 'Bob'}
# ]
df = pd.read_csv('users.csv')
data = df.to_dict(orient='records')
#%%
pipeline = dlt.pipeline(
    pipeline_name='quick_start',
    destination='duckdb',
    dataset_name='mydata'
)
load_info = pipeline.run(data, table_name="users",  write_disposition="replace")

print(load_info)
#%%
# a database 'chess_pipeline.duckdb' was created in working directory so just connect to it
conn = duckdb.connect(f"{pipeline.pipeline_name}.duckdb")

# this lets us query data without adding schema prefix to table names
conn.sql(f"SET search_path = '{pipeline.dataset_name}'")
#%%
# list all tables
print(conn.sql("DESCRIBE"))

print(conn.sql("select * from users"))


owid_disasters_csv = "https://raw.githubusercontent.com/owid/owid-datasets/master/datasets/Natural%20disasters%20from%201900%20to%202019%20-%20EMDAT%20(2020)/Natural%20disasters%20from%201900%20to%202019%20-%20EMDAT%20(2020).csv"
df = pd.read_csv(owid_disasters_csv)
data = df.to_dict(orient='records')

pipeline = dlt.pipeline(
    pipeline_name='quick_start',
    destination='duckdb',
    dataset_name='mydata',
)
load_info = pipeline.run(data, table_name="natural_disasters",  write_disposition="replace")

print(load_info)

# list all tables
print(conn.sql("DESCRIBE"))

print(conn.sql("select * from natural_disasters"))