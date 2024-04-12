from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.exceptions import HttpResponseError
from dotenv import load_dotenv
from InquirerPy import prompt
from tqdm import tqdm
import os
import shutil

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
    
    questions = [
      {
        'type': type,
        'message': message,
        'name': 'selected_folder',
        'choices': choices,
      }
    ]
    selected_folder = prompt(questions)['selected_folder']
    return selected_folder

  def select_table_data(self):
    selected_table = self.select_folder(None, 0, 'Select table:')
    selected_year = self.select_folder(selected_table, 1, 'Select year:')
    selected_month = self.select_folder(selected_year, 2, 'Select month:')
    selected_days = self.select_folder(selected_month, 3, 'Select days (press space to select, enter to continue): ', 'checkbox', False)
    return selected_table, selected_year, selected_month, selected_days
  
  def download_selected_files(self, selected_days: list):
    # Create the "data" folder if it doesn't exist
    os.makedirs("data", exist_ok=True)

    total_files = len(selected_days)
    progress_bar = tqdm(total=total_files, unit="file")

    for day in selected_days:
      progress_bar.set_description(f"Downloading {day}")

      file_client = self.file_system_client.get_file_client(day)

      local_file_path = f"data/{day[day.rfind("/") + 1 :]}"
      with open(local_file_path, "wb") as file_handle:
        download = file_client.download_file()
        download.readinto(file_handle)

      progress_bar.update(1)
    
    progress_bar.close()
    print("All files downloaded successfully.")
  
  def delete_data_folder(self):
    data_folder = "data"
    if os.path.exists(data_folder):
      shutil.rmtree(data_folder)
      print("Deleted the 'data' folder and its contents.")
    else:
      print("The 'data' folder does not exist.")