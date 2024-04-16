from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.exceptions import HttpResponseError
from dotenv import load_dotenv
from InquirerPy import inquirer
from tqdm import tqdm
import os
from datetime import datetime, timedelta

class DataLakeExplorer:
  def __init__(self):
    load_dotenv()
    self.service = None
    self.file_system_client = None
  
  def connect(self):
    try:
      self.service = DataLakeServiceClient(
        account_url=os.getenv('ACCOUNT_URL'),
        credential=os.getenv('SAS_TOKEN')
      )
      # Get a reference to the file system (container)
      file_system_name = "datafactorybronze"
      self.file_system_client = self.service.get_file_system_client(
        file_system_name)
    except HttpResponseError as e:
      print("Connection failed!")
      print(f"Error: {e}")
      exit()

  def select_folder(self, path, level, message, type='list', selecting_folders=True):
    folder_paths = self.file_system_client.get_paths(path=path)
    if selecting_folders:
      choices = [path.name for path in folder_paths if path.is_directory and path.name.count('/') == level]
    else:
      choices = [path.name for path in folder_paths if not path.is_directory and path.name.count('/') == level]

    if type == 'checkbox':
      keybindings = {
        "toggle-all": [{"key": ["c-a"]}],
      }
      selected_folder = inquirer.checkbox(
        message=message,
        choices=choices,
        keybindings=keybindings
      ).execute()
    elif type == 'list':
      selected_folder = inquirer.select(
        message=message,
        choices=choices
      ).execute()

    return selected_folder

  def select_table_data(self):
    selected_table = self.select_folder(None, 0, 'Select table:')
    selected_year = self.select_folder(selected_table, 1, 'Select year:')
    selected_months = self.select_folder(selected_year, 2, 'Select months (press space to select, enter to continue, ctrl-a to select all): ', 'checkbox')
    
    if len(selected_months) == 1:
      selected_days = self.select_folder(selected_months[0], 3, f'Select days for {selected_months[0]}, (press space to select, enter to continue, ctrl-a to select all): ', 'checkbox', False)
    elif len(selected_months) >= 1:
      starting_day = self.select_folder(selected_months[0], 3, f'Select starting day for month {selected_months[0]}', selecting_folders=False)

      ending_day = self.select_folder(selected_months[len(selected_months) - 1], 3, f'Select ending day for month {selected_months[len(selected_months) - 1]}', selecting_folders=False)

      start_date_str = starting_day.split('/')[-3: starting_day.rfind(".")]
      end_date_str = ending_day.split('/')[-3: starting_day.rfind(".")]

      start_date = datetime(int(start_date_str[0]), int(start_date_str[1]), int(start_date_str[2][:start_date_str[2].rfind(".")]))
      end_date = datetime(int(end_date_str[0]), int(end_date_str[1]), int(end_date_str[2][:end_date_str[2].rfind(".")]))

      selected_days = []
      current_date = start_date
      while current_date <= end_date:
        file_path = f"{selected_table}/{selected_year.split("/")[1]}/{current_date.month:02d}/{current_date.day:02d}.parquet"
        selected_days.append(file_path)
        current_date += timedelta(days=1)

    return selected_table, selected_year, selected_months, selected_days
  
  def download_selected_files(self, selected_days: list):
    # Create the "data" folder if it doesn't exist
    os.makedirs("data", exist_ok=True)

    total_files = len(selected_days)
    progress_bar = tqdm(total=total_files, unit="file")

    for day in selected_days:
      table_name, year, month, day_file = day.split("/")
      day_file = day_file.split(".")[0]

      # Create table, year and day folder if they don't exist
      table_folder = f"data/{table_name}"
      os.makedirs(table_folder, exist_ok=True)

      year_folder = f"{table_folder}/{year}"
      os.makedirs(year_folder, exist_ok=True)

      month_folder = f"{year_folder}/{month}"
      os.makedirs(month_folder, exist_ok=True)

      progress_bar.set_description(f"Downloading {day}")
      file_client = self.file_system_client.get_file_client(day)

      local_file_path = f"{month_folder}/{day_file}.parquet"
      if os.path.exists(local_file_path):
        progress_bar.set_description(f"Skipping {day} (already downloaded)")
      else:
        progress_bar.set_description(f"Downloading {day}")
        file_client = self.file_system_client.get_file_client(day)

        with open(local_file_path, "wb") as file_handle:
          download = file_client.download_file()
          download.readinto(file_handle)

      progress_bar.update(1)
    
    progress_bar.close()
    print("All files downloaded successfully.")