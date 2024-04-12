from mysql_connector import MySQLConnector
from azure_connector import DataLakeExplorer
from testing_settings_prompt import *
import pyarrow.parquet as pq
from tqdm import tqdm
import pandas as pd

def pksDatalake(selected_num_rows):
  dlConnector = DataLakeExplorer()
  dlConnector.connect()

  table_name, year, month, days = dlConnector.select_table_data()
  dlConnector.download_selected_files(days)

  sqlConnector = MySQLConnector()
  sqlConnector.connect()

  primary_key_columns = sqlConnector.get_primary_key_columns(table_name)

  print('Found primary keys: ' + ', '.join(primary_key_columns) + '\n')

  for days_file_path in tqdm(days, desc="Processing files"):
    data_file_path = days_file_path[days_file_path.rfind("/") + 1 :]

    table_parquet = pq.read_table('data/' + data_file_path)
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
      
      exists = sqlConnector.execute_query(query)
      if not exists:
        missing_in_mysql.append(primary_key)

    if missing_in_mysql:
      tqdm.write(f"Mismatches found in file: {days_file_path}")
      tqdm.write("Missing in MySQL: {} \n".format(missing_in_mysql))
    else:
      tqdm.write(f"No mismatches found in file: {days_file_path} \n")

  # Close connection and delete data folder
  sqlConnector.close_connection()
  dlConnector.delete_data_folder()


def allDataDatalake(selected_num_rows):
  dlConnector = DataLakeExplorer()
  dlConnector.connect()

  table_name, year, month, days = dlConnector.select_table_data()
  dlConnector.download_selected_files(days)

  sqlConnector = MySQLConnector()
  sqlConnector.connect()

  column_info = sqlConnector.get_column_info(table_name)
  for days_file_path in tqdm(days, desc="Processing files"):
    data_file_path = days_file_path[days_file_path.rfind("/") + 1 :]
    table_parquet = pq.read_table('data/' + data_file_path)
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
      exists = sqlConnector.execute_query(query)

      if not exists:
        mismatches_found = True
        tqdm.write(f"Mismatch found in file: {days_file_path}")
        tqdm.write("Mismatched row: {}".format(dict(row)))

    if not mismatches_found:
      tqdm.write(f"No mismatches found in file: {days_file_path} \n")

  # Close connection and delete data folder
  sqlConnector.close_connection()
  dlConnector.delete_data_folder()


def pksSQL(selected_num_rows):
  pass


def allDataSQL(selected_num_rows):
  pass


def main():
  # Prompt user for data source and testing option
  selected_data_source = select_data_source()
  selected_testing_option = select_testing_option(selected_data_source)
  selected_num_rows = select_rows_number()

  if selected_data_source == 'datalake':
    if selected_testing_option == 'primary_keys':
      pksDatalake(selected_num_rows)
    else:
      allDataDatalake(selected_num_rows)
  else: #datasource is SQL
    if selected_testing_option == 'primary_keys':
      pksSQL(selected_num_rows)
    else:
      allDataSQL(selected_num_rows)


if __name__ == '__main__':
  main()
