from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.exceptions import HttpResponseError
from dotenv import load_dotenv
import os
from InquirerPy import prompt

load_dotenv()

try:
  service = DataLakeServiceClient(
    account_url=os.getenv('ACCOUNT_URL'), 
    credential=os.getenv('SAS_TOKEN'))

  # Get a reference to the file system (container)
  file_system_name = "datafactorybronze"
  file_system_client = service.get_file_system_client(file_system_name)

except HttpResponseError as e:
  print("Connection failed!")
  print(f"Error: {e}")
  exit()


def select_folder(path, level, message, type='list', selecting_folders=True):
  folder_paths = file_system_client.get_paths(path=path)
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


selected_folder = select_folder(None, 0, 'Select folder:')

selected_year = select_folder(selected_folder, 1, 'Select year:')

selected_month = select_folder(selected_year, 2, 'Select month:')

selected_days = select_folder(selected_month, 3, 'Select days (press space to select, enter to continue): ', 'checkbox', False)

print(selected_days)