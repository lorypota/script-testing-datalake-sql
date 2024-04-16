from testing_settings_prompt import *
from data_processor import *
import os
import shutil


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
      duplicates_datalake()
    else:
      selected_num_rows = select_rows_number()
      if selected_testing_option == 'primary_keys':
        pks_datalake(selected_num_rows)
      elif selected_testing_option == 'all_data':
        all_data_datalake(selected_num_rows)
  else: #datasource is SQL
    if selected_testing_option == 'no_duplicates':
      duplicates_sql()
    else:
      selected_num_rows = select_rows_number()
      if selected_testing_option == 'primary_keys':
        pks_sql(selected_num_rows)
      elif selected_testing_option == 'all_data':
        all_data_sql(selected_num_rows)


if __name__ == '__main__':
  main()
