from mysql_connector import MySQLConnector
from azure_connector import DataLakeExplorer
import pandasgui
import pyarrow.parquet as pq
from tqdm import tqdm
import pandas as pd
import pyarrow.parquet as pq
from tqdm import tqdm

# Constants
DATA_FOLDER = 'data/'

def establish_connection_datalake():
  dl_connector = DataLakeExplorer()
  dl_connector.connect()
  return dl_connector

def estabilish_connection_sql():
  sql_connector = MySQLConnector()
  sql_connector.connect()
  return sql_connector

def select_and_download_data(dl_connector: DataLakeExplorer) -> tuple:
  table_name, year, month, days = dl_connector.select_table_data()
  dl_connector.download_selected_files(days)
  return table_name, year, month, days

def fetch_mysql_data(sql_connector: MySQLConnector, query: str) -> pd.DataFrame:
  print("Getting data from MySQL.")
  results = sql_connector.execute_query(query)
  print("Fetched data from MySQL.")
  df_mysql = pd.DataFrame(results, columns=[column[0] for column in sql_connector.cursor.description])
  return df_mysql

def read_parquet_files(days: list, columns: list = None) -> pd.DataFrame:
  dfs_datalake = []
  for days_file_path in tqdm(days, desc="Reading files"):
    parquet_file = pq.ParquetFile(DATA_FOLDER + days_file_path)
    df = parquet_file.read(columns=columns).to_pandas()
    dfs_datalake.append(df)
  df_datalake = pd.concat(dfs_datalake, ignore_index=True)
  print("Data from datalake has been loaded.")
  return df_datalake

def check_mismatches(df_mysql: pd.DataFrame, df_datalake: pd.DataFrame, comparison_columns: list) -> None:
  all_present = df_mysql.set_index(comparison_columns).index.isin(df_datalake.set_index(comparison_columns).index).all()
  if all_present:
    print("All rows from MySQL are present in datalake")
  else:
    print("Some rows from MySQL are missing in datalake")
    missing_rows = df_mysql[~df_mysql.set_index(comparison_columns).index.isin(df_datalake.set_index(comparison_columns).index)]
    print("Missing rows:")
    print(missing_rows)

def pks_datalake(selected_num_rows: int) -> None:
  dl_connector = establish_connection_datalake()

  table_name, year, month, days = select_and_download_data(dl_connector)

  sql_connector = estabilish_connection_sql()

  primary_key_columns = sql_connector.get_primary_key_columns(table_name)
  print('Found primary keys: ' + ', '.join(primary_key_columns) + '\n')

  for days_file_path in tqdm(days, desc="Processing files"):
    table_parquet = pq.read_table(DATA_FOLDER + days_file_path)
    df = table_parquet.to_pandas()
    primary_keys_parquet = df[primary_key_columns].values.tolist()

    missing_in_mysql = []
    if selected_num_rows=='all':
      rows_to_check=len(primary_keys_parquet)
    else:
      rows_to_check = min(selected_num_rows, len(primary_keys_parquet))

    for primary_key in tqdm(
      primary_keys_parquet[:rows_to_check], desc="Checking rows", total=rows_to_check
    ):
      conditions = " AND ".join(
        [f"{column} = '{value}'" for column, value in zip(primary_key_columns, primary_key)]
      )
      query = f"SELECT 1 FROM {table_name} WHERE {conditions}"

      exists = sql_connector.execute_query(query)
      if not exists:
        missing_in_mysql.append(primary_key)

    if missing_in_mysql:
      tqdm.write(f"Mismatches found in file: {days_file_path}")
      tqdm.write("Missing in MySQL: {} \n".format(missing_in_mysql))
    else:
      tqdm.write(f"No mismatches found in file: {days_file_path} \n")

  # Close connection
  sql_connector.close_connection()


def all_data_datalake(selected_num_rows: int) -> None:
  dl_connector = establish_connection_datalake()

  table_name, year, month, days = select_and_download_data(dl_connector)

  sql_connector = estabilish_connection_sql()
  column_info = sql_connector.get_column_info(table_name)

  for days_file_path in tqdm(days, desc="Processing files"):
    table_parquet = pq.read_table(DATA_FOLDER + days_file_path)
    df = table_parquet.to_pandas()

    if selected_num_rows=='all':
      rows_to_check=len(df)
    else:
      rows_to_check = min(selected_num_rows, len(df))
    mismatches_found = False

    for _, row in tqdm(df.iterrows(), desc="Checking rows", total=rows_to_check):
      if _ >= rows_to_check:
        break

      conditions = []
      for column, value in row.items():
        data_type = column_info[column]

        if pd.isna(value) or value == 'None' or value == 'nan':
          conditions.append(f"{column} IS NULL")
        elif data_type.startswith('binary'):
          conditions.append(f"{column} = UNHEX('{value.hex()}')")
        else:
          conditions.append(f"{column} = '{value}'")

      conditions = " AND ".join(conditions)
      query = f"SELECT 1 FROM {table_name} WHERE {conditions}"
      exists = sql_connector.execute_query(query)

      if not exists:
        mismatches_found = True
        tqdm.write(f"Mismatch found in file: {days_file_path}")
        tqdm.write("Mismatched row: {}".format(dict(row)))

    if not mismatches_found:
      tqdm.write(f"No mismatches found in file: {days_file_path} \n")

  # Close connection
  sql_connector.close_connection()


def duplicates_datalake() -> None:
  dl_connector = establish_connection_datalake()

  table_name, year, month, days = select_and_download_data(dl_connector)

  sql_connector = estabilish_connection_sql()

  primary_key_columns = sql_connector.get_primary_key_columns(table_name)
  print('Found primary keys: ' + ', '.join(primary_key_columns) + '\n')

  combined_df = read_parquet_files(days)

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


def pks_sql(selected_num_rows: int) -> None:
  dl_connector = DataLakeExplorer()
  sql_connector = MySQLConnector()

  dl_connector.connect()
  table_name, year, months, days = dl_connector.select_table_data()

  if len(days) < 3:
    print("Select at least 3 days to study.")
    return
  
  dl_connector.download_selected_files(days)

  starting_date, ending_date = get_date_range(days)
  
  primary_keys = sql_connector.get_primary_key_columns(table_name)

  if selected_num_rows != 'all':
    limit = f"LIMIT {selected_num_rows}"
  else:
    limit = ""

  query = f"SELECT {",".join(primary_keys)} FROM {table_name} WHERE server_time BETWEEN '{starting_date}' AND '{ending_date}' {limit}"

  df_mysql = fetch_mysql_data(sql_connector, query)
  df_datalake = read_parquet_files(days, columns=primary_keys)

  # pandasgui.show(df_mysql, df_datalake)

  check_mismatches(df_mysql, df_datalake, primary_keys)


def all_data_sql(selected_num_rows: int) -> None:
  dl_connector = DataLakeExplorer()
  sql_connector = MySQLConnector()

  dl_connector.connect()
  table_name, year, months, days = dl_connector.select_table_data()

  if len(days) < 3:
    print("Select at least 3 days to study.")
    return
  
  dl_connector.download_selected_files(days)

  starting_date, ending_date = get_date_range(days)

  # Get all columns from the table
  all_columns = sql_connector.get_column_info(table_name)

  if selected_num_rows != 'all':
    limit = f"LIMIT {selected_num_rows}"
  else:
    limit = ""

  query = f"SELECT * FROM {table_name} WHERE server_time BETWEEN '{starting_date}' AND '{ending_date}' {limit}"
  df_mysql = fetch_mysql_data(sql_connector, query)
  df_datalake = read_parquet_files(days, columns=all_columns)

  # pandasgui.show(df_MySQL, df_datalake)

  check_mismatches(df_mysql, df_datalake, list(all_columns.keys()))


def duplicates_sql():
  pass