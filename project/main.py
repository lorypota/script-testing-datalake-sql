from mysql_connector import MySQLConnector
from azure_connector import DataLakeExplorer
from testing_settings_prompt import *
import pyarrow.parquet as pq
from tqdm import tqdm
import pandas as pd
import os
import shutil
import pandasgui

def check_primary_keys(df, rows_to_check, table_name, primary_key_columns, sqlConnector, days_file_path):
  primary_keys_parquet = df[primary_key_columns].values.tolist()
  missing_in_mysql = []
  
  for primary_key in tqdm(primary_keys_parquet[:rows_to_check], desc="Checking rows", total=rows_to_check):
    conditions = " AND ".join([f"{column} = '{value}'" for column, value in zip(primary_key_columns, primary_key)])
    query = f"SELECT 1 FROM {table_name} WHERE {conditions}"
    exists = sqlConnector.execute_query(query)
    if not exists:
      missing_in_mysql.append(primary_key)

  if missing_in_mysql:
    tqdm.write(f"Mismatches found in file: {days_file_path}")
    tqdm.write("Missing in MySQL: {} \n".format(missing_in_mysql))
    return True
  return False


def check_all_columns(df, rows_to_check, table_name, column_info, sqlConnector, days_file_path):
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
    exists = sqlConnector.execute_query(query)
    
    if not exists:
      mismatches_found = True
      tqdm.write(f"Mismatch found in file: {days_file_path}")
      tqdm.write("Mismatched row: {}".format(dict(row)))
      
  return mismatches_found


def process_files(sqlConnector, table_name, days, column_data, selected_num_rows, check_func):
  for days_file_path in tqdm(days, desc="Processing files"):
    table_parquet = pq.read_table('data/' + days_file_path)
    df = table_parquet.to_pandas()
    
    if selected_num_rows == 'all':
      rows_to_check = len(df)
    else:
      rows_to_check = min(selected_num_rows, len(df))
    
    mismatches_found = check_func(df, rows_to_check, table_name, column_data, sqlConnector, 
                                  days_file_path)
    
    if not mismatches_found:
      tqdm.write(f"No mismatches found in file: {days_file_path} \n")


def process_datalake(selected_num_rows, check_func):
  dlConnector = DataLakeExplorer()
  sqlConnector = MySQLConnector()
  
  dlConnector.connect()
  table_name, _, _, days = dlConnector.select_table_data()
  dlConnector.download_selected_files(days)
  
  sqlConnector.connect()
  if check_func == check_primary_keys:
    column_data = sqlConnector.get_primary_key_columns(table_name)
    print('Found primary keys: ' + ', '.join(column_data) + '\n')
  else:
    column_data = sqlConnector.get_column_info(table_name)
  
  process_files(sqlConnector, table_name, days, column_data, selected_num_rows, check_func)
  
  sqlConnector.close_connection()


def pksDatalake(selected_num_rows):
  process_datalake(selected_num_rows, check_primary_keys)


def allDataDatalake(selected_num_rows):
  process_datalake(selected_num_rows, check_all_columns)


def duplicatesDatalake():
  dlConnector = DataLakeExplorer()
  sqlConnector = MySQLConnector()

  dlConnector.connect()
  table_name, year, month, days = dlConnector.select_table_data()
  dlConnector.download_selected_files(days)

  sqlConnector.connect()
  primary_key_columns = sqlConnector.get_primary_key_columns(table_name)
  print('Found primary keys: ' + ', '.join(primary_key_columns) + '\n')

  # Read all parquet files into a single DataFrame
  dfs = []
  for days_file_path in tqdm(days, desc="Reading files"):
    table_parquet = pq.read_table('data/' + days_file_path)
    df = table_parquet.to_pandas()
    dfs.append(df)

  combined_df = pd.concat(dfs, ignore_index=True)

  # Check for duplicates based on primary key columns
  duplicates = combined_df[combined_df.duplicated(subset=primary_key_columns, keep=False)]

  if len(duplicates) > 0:
    print(f"Duplicates found based on primary keys: {', '.join(primary_key_columns)}")
    print(duplicates)
  else:
    print("No duplicates found based on primary keys.")

  sqlConnector.close_connection()


def pksSQL(selected_num_rows):
  dlConnector = DataLakeExplorer()
  sqlConnector = MySQLConnector()

  dlConnector.connect()
  table_name, year, months, days = dlConnector.select_table_data()

  if len(days) < 3:
    print("Select at least 3 days to study.")
    return
  
  dlConnector.download_selected_files(days)

  starting_date = days[1][days[1].find("/") + 1 : days[1].find(".")]
  ending_date = days[len(days)-2][days[len(days)-2].find("/") + 1 : days[len(days)-2].rfind(".")]
  print("Due to the selected files, the dates under consideration go from: ", starting_date, 
        " to: ", ending_date, "\n")
  
  starting_date = starting_date.replace("/", "-")
  ending_date = ending_date.replace("/", "-")
  
  primary_keys = sqlConnector.get_primary_key_columns(table_name)

  if selected_num_rows != 'all':
    limit = f"LIMIT {selected_num_rows}"
  else:
    limit = ""

  query = f"SELECT {",".join(primary_keys)} FROM {table_name} WHERE server_time BETWEEN '{starting_date}' AND '{ending_date}' {limit}"
  print("Getting data from MySQL.")
  results = sqlConnector.execute_query(query)
  print("Fetched data from MySQL. \n")

  # Save results of MySQL in DataFrame
  df_MySQL = pd.DataFrame(results, columns=[column[0] for column in sqlConnector.cursor.description])

  # Read all parquet files into a single DataFrame
  dfs_datalake = []
  for days_file_path in tqdm(days, desc="Reading files"):
    parquet_file = pq.ParquetFile('data/' + days_file_path)
    df = parquet_file.read(columns=primary_keys).to_pandas()
    dfs_datalake.append(df)
  df_datalake = pd.concat(dfs_datalake, ignore_index=True)
  print("Data from datalake has been loaded.\n")

  # pandasgui.show(df_MySQL, df_datalake)

  # Check if all rows from df_MySQL are present in df_datalake
  all_present = df_MySQL.set_index(primary_keys).index.isin(df_datalake.set_index(primary_keys).index).all()

  if all_present:
    print("All rows from df_MySQL are present in df_datalake")
  else:
    print("Some rows from df_MySQL are missing in df_datalake")
    
    # Get the missing rows
    missing_rows = df_MySQL[~df_MySQL.set_index(primary_keys).index.isin(df_datalake.set_index(primary_keys).index)]
    print("Missing rows:")
    print(missing_rows)


def allDataSQL(selected_num_rows):
  dlConnector = DataLakeExplorer()
  sqlConnector = MySQLConnector()

  dlConnector.connect()
  table_name, year, months, days = dlConnector.select_table_data()

  if len(days) < 3:
    print("Select at least 3 days to study.")
    return

  dlConnector.download_selected_files(days)

  starting_date = days[1][days[1].find("/") + 1: days[1].find(".")]
  ending_date = days[len(days) - 2][days[len(days) - 2].find("/") + 1: days[len(days) - 2].rfind(".")]
  print("Due to the selected files, the dates under consideration go from:", starting_date,
        "to:", ending_date, "\n")

  starting_date = starting_date.replace("/", "-")
  ending_date = ending_date.replace("/", "-")

  # Get all columns from the table
  all_columns = sqlConnector.get_column_info(table_name)

  if selected_num_rows != 'all':
    limit = f"LIMIT {selected_num_rows}"
  else:
    limit = ""

  query = f"SELECT * FROM {table_name} WHERE server_time BETWEEN '{starting_date}' AND '{ending_date}' {limit}"
  print("Getting data from MySQL.")
  results = sqlConnector.execute_query(query)
  print("Fetched data from MySQL.\n")

  # Save results of MySQL in DataFrame
  df_MySQL = pd.DataFrame(results, columns=[column[0] for column in sqlConnector.cursor.description])

  # Read all parquet files into a single DataFrame
  dfs_datalake = []
  for days_file_path in tqdm(days, desc="Reading files"):
    parquet_file = pq.ParquetFile('data/' + days_file_path)
    df = parquet_file.read(columns=all_columns).to_pandas()
    dfs_datalake.append(df)
  df_datalake = pd.concat(dfs_datalake, ignore_index=True)
  print("Data from datalake has been loaded.\n")

  pandasgui.show(df_MySQL, df_datalake)

  # Check if all rows from df_MySQL are present in df_datalake
  all_present = df_MySQL.equals(df_datalake)

  if all_present:
    print("All rows from df_MySQL are present in df_datalake")
  else:
    print("Some rows from df_MySQL are missing or different in df_datalake")

    # Get the missing or different rows
    missing_rows = df_MySQL[~df_MySQL.apply(tuple, 1).isin(df_datalake.apply(tuple, 1))]
    print("Missing or different rows:")
    print(missing_rows)


def duplicatesSQL():
  pass


def main():
  # Prompt user for data source and testing option
  selected_data_source = select_data_source()

  if selected_data_source == "delete_data":
    if os.path.exists("data"):
      shutil.rmtree("data")
      print("Deleted the 'data' folder and its contents.")
    else:
      print("The 'data' folder does not exist.")
    return

  selected_testing_option = select_testing_option(selected_data_source)
  
  if selected_data_source == 'datalake':
    if selected_testing_option == 'no_duplicates':
      duplicatesDatalake()
    else:
      selected_num_rows = select_rows_number()
      if selected_testing_option == 'primary_keys':
        pksDatalake(selected_num_rows)
      elif selected_testing_option == 'all_data':
        allDataDatalake(selected_num_rows)
  else: #datasource is SQL
    if selected_testing_option == 'no_duplicates':
      duplicatesSQL()
    else:
      selected_num_rows = select_rows_number()
      if selected_testing_option == 'primary_keys':
        pksSQL(selected_num_rows)
      elif selected_testing_option == 'all_data':
        allDataSQL(selected_num_rows)


if __name__ == '__main__':
  main()
