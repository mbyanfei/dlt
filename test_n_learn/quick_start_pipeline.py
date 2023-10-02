import dlt
import duckdb
# from google.colab import data_table
# data_table.enable_dataframe_formatter()

data = [
    {'id': 1, 'name': 'Alice'},
    {'id': 2, 'name': 'Bob'}
]

pipeline = dlt.pipeline(
    pipeline_name='quick_start',
    destination='duckdb',
    dataset_name='mydata'
)
load_info = pipeline.run(data, table_name="users")

print(load_info)

# a database 'chess_pipeline.duckdb' was created in working directory so just connect to it
conn = duckdb.connect(f"{pipeline.pipeline_name}.duckdb")

# this lets us query data without adding schema prefix to table names
conn.sql(f"SET search_path = '{pipeline.dataset_name}'")

# list all tables
print(conn.sql("DESCRIBE"))