from mysql_connector import MySQLConnector
from azure_connector import DataLakeExplorer
import pandasgui
import pyarrow.parquet as pq
from tqdm import tqdm
import pandas as pd
import os

# Constants
DATA_FOLDER = 'data/'


def establish_datalake_connection():
  datalake_connector = DataLakeExplorer()
  datalake_connector.connect()
  return datalake_connector


def establish_sql_connection():
  sql_connector = MySQLConnector()
  sql_connector.connect()
  return sql_connector


def select_and_download_data(datalake_connector: DataLakeExplorer) -> tuple:
  table_name, year, month, days = datalake_connector.select_table_data()
  datalake_connector.download_selected_files(days)
  return table_name, year, month, days


def fetch_mysql_data(sql_connector: MySQLConnector, query: str) -> pd.DataFrame:
  print("Getting data from MySQL.")
  results = sql_connector.execute_query(query)
  print("Fetched data from MySQL.")
  mysql_df = pd.DataFrame(results, columns=[column[0] for column in sql_connector.cursor.description])
  return mysql_df


def convert_unhashable(value):
  if isinstance(value, bytearray):
    return bytes(value)
  return value


def read_parquet_files(days: list, columns: list = None) -> pd.DataFrame:
  datalake_dfs = []
  for day_file_path in tqdm(days, desc="Reading files"):
    if os.path.getsize(DATA_FOLDER + day_file_path) == 0:
      print(f"The file {day_file_path} is empty. Skipping...")
    else:
      parquet_file = pq.ParquetFile(DATA_FOLDER + day_file_path)
      df = parquet_file.read(columns=columns).to_pandas()
      datalake_dfs.append(df)
  datalake_df = pd.concat(datalake_dfs, ignore_index=True)
  datalake_df = datalake_df.map(convert_unhashable)
  print("Data from datalake has been loaded.")
  return datalake_df


def check_subset(df1: pd.DataFrame, df2: pd.DataFrame, comparison_columns: list) -> None:
  """Checks if all rows of df1 are present in df2"""
  is_subset = df1.set_index(comparison_columns).index.isin(df2.set_index(comparison_columns).index).all()
  if is_subset:
    print(f"All rows from {df1.name} are present in {df2.name}")
  else:
    missing_rows = df1[~df1.set_index(comparison_columns).index.isin(df2.set_index(comparison_columns).index)]
    print(f"{len(missing_rows)} rows from {df1.name} are missing in {df2.name}")
    print("Missing rows:")
    print(missing_rows)


def compare_primary_keys_datalake() -> None:
  datalake_connector = establish_datalake_connection()

  table_name, year, month, days = select_and_download_data(datalake_connector)

  sql_connector = establish_sql_connection()

  primary_key_columns = sql_connector.get_primary_key_columns(table_name)
  print('Found primary keys: ' + ', '.join(primary_key_columns) + '\n')

  datalake_df = read_parquet_files(days, columns=primary_key_columns)
  datalake_df.name = "Datalake"
  min_values = datalake_df[primary_key_columns].min().tolist()
  max_values = datalake_df[primary_key_columns].max().tolist()
  
  conditions = []
  for i, primary_key in enumerate(primary_key_columns):
    conditions.append(f"{primary_key} BETWEEN {min_values[i]} AND {max_values[i]}")
  conditions_str = " AND ".join(conditions)

  query = f"SELECT {', '.join(primary_key_columns)} FROM {table_name} WHERE {conditions_str}"

  mysql_df = fetch_mysql_data(sql_connector, query)
  mysql_df.name = "MySQL"

  check_subset(datalake_df, mysql_df, primary_key_columns)

  pandasgui.show(mysql_df, datalake_df)

  # Close connection
  sql_connector.close_connection()


def compare_all_data_datalake() -> None:
  datalake_connector = establish_datalake_connection()

  table_name, year, month, days = select_and_download_data(datalake_connector)

  sql_connector = establish_sql_connection()
  primary_key_columns = sql_connector.get_primary_key_columns(table_name)
  print('Found primary keys: ' + ', '.join(primary_key_columns) + '\n')
  all_columns = sql_connector.get_column_info(table_name)

  datalake_df = read_parquet_files(days, columns=all_columns)
  datalake_df.name = "Datalake"
  min_values = datalake_df[primary_key_columns].min().tolist()
  max_values = datalake_df[primary_key_columns].max().tolist()
  

  conditions = []
  for i, primary_key in enumerate(primary_key_columns):
    conditions.append(f"{primary_key} BETWEEN {min_values[i]-1} AND {max_values[i]+1}")
  conditions_str = " AND ".join(conditions)

  query = f"SELECT * FROM {table_name} WHERE {conditions_str}"
  mysql_df = fetch_mysql_data(sql_connector, query)
  mysql_df = mysql_df.map(convert_unhashable)
  mysql_df.name = "MySQL"

  check_subset(datalake_df, mysql_df, list(all_columns.keys()))

  pandasgui.show(mysql_df, datalake_df)

  # Close connection
  sql_connector.close_connection()


def check_duplicates_datalake() -> None:
  datalake_connector = establish_datalake_connection()

  table_name, year, month, days = select_and_download_data(datalake_connector)

  sql_connector = establish_sql_connection()

  primary_key_columns = sql_connector.get_primary_key_columns(table_name)
  print('Found primary keys: ' + ', '.join(primary_key_columns) + '\n')

  combined_df = read_parquet_files(days, columns=primary_key_columns)

  pandasgui.show(combined_df)

  # Check for duplicates based on primary key columns
  duplicates = combined_df[combined_df.duplicated(subset=primary_key_columns, keep=False)]

  if len(duplicates) > 0:
    print(f"Duplicates found based on primary keys: {', '.join(primary_key_columns)}")
    print(duplicates)
  else:
    print("No duplicates found based on primary keys.")

  sql_connector.close_connection()


def get_date_range(days: list) -> tuple:
  starting_date = days[1][days[1].find("/") + 1: days[1].find(".")]
  ending_date = days[len(days) - 2][days[len(days) - 2].find("/") + 1: days[len(days) - 2].rfind(".")]
  print(f"Due to the selected files, the dates under consideration go from: {starting_date} to: {ending_date}", "\n")
  starting_date = starting_date.replace("/", "-")
  ending_date = ending_date.replace("/", "-")
  return starting_date, ending_date


def compare_primary_keys_sql(selected_num_rows: int) -> None:
  datalake_connector = DataLakeExplorer()
  sql_connector = MySQLConnector()

  datalake_connector.connect()
  table_name, year, months, days = datalake_connector.select_table_data()

  if len(days) < 3:
    print("Select at least 3 days to study.")
    return
  
  datalake_connector.download_selected_files(days)

  starting_date, ending_date = get_date_range(days)
  
  primary_keys = sql_connector.get_primary_key_columns(table_name)

  datalake_df = read_parquet_files(days, columns=primary_keys)
  datalake_df.name = "Datalake"

  if selected_num_rows != 'all':
    limit = f"LIMIT {selected_num_rows}"
  else:
    limit = ""

  query = f"SELECT {','.join(primary_keys)} FROM {table_name} WHERE server_time BETWEEN '{starting_date}' AND '{ending_date}' {limit}"

  mysql_df = fetch_mysql_data(sql_connector, query)
  mysql_df.name = "MySQL"

  # pandasgui.show(mysql_df, datalake_df)

  check_subset(mysql_df, datalake_df, primary_keys)


def compare_all_data_sql(selected_num_rows: int) -> None:
  datalake_connector = DataLakeExplorer()
  sql_connector = MySQLConnector()

  datalake_connector.connect()
  table_name, year, months, days = datalake_connector.select_table_data()

  if len(days) < 3:
    print("Select at least 3 days to study.")
    return
  
  datalake_connector.download_selected_files(days)

  starting_date, ending_date = get_date_range(days)

  # Get all columns from the table
  all_columns = sql_connector.get_column_info(table_name)

  if selected_num_rows != 'all':
    limit = f"LIMIT {selected_num_rows}"
  else:
    limit = ""

  query = f"SELECT * FROM {table_name} WHERE server_time BETWEEN '{starting_date}' AND '{ending_date}' {limit}"

  mysql_df = fetch_mysql_data(sql_connector, query)
  mysql_df.name = "MySQL"

  datalake_df = read_parquet_files(days, columns=all_columns)
  datalake_df.name = "Datalake"

  # pandasgui.show(mysql_df, datalake_df)

  check_subset(mysql_df, datalake_df, list(all_columns.keys()))