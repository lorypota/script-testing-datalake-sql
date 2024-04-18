from InquirerPy import prompt
from datetime import datetime, timedelta


def select_data_source():
  data_source_options = [
    {
      'type': 'list',
      'message': 'Select action:',
      'name': 'data_source',
      'choices': [
        {
          'name': 'Fetch data from datalake and compare it to MySQL.',
          'value': 'datalake'
        },
        {
          'name': 'Fetch data from MySQL and compare it to datalake.',
          'value': 'mysql'
        },
        {
          'name': 'Delete stored downloaded data.',
          'value': 'delete_data'
        }
      ]
    }
  ]

  selected_data_source = prompt(data_source_options)['data_source']
  return selected_data_source


def select_testing_option(data_source):
  testing_options = [
    {
      'type': 'list',
      'message': 'Select a testing option:',
      'name': 'testing_option',
      'choices': [
        {
          'name': f'Check that primary keys in {data_source} correspond to the other data source.',
          'value': 'primary_keys'
        },
        {
          'name': f'Check that all data in {data_source} correspond to the other data source.',
          'value': 'all_data'
        }
      ]
    }
  ]

  if data_source == 'datalake':
    testing_options[0]['choices'].append({
      'name': 'Check that the primary keys in datalake have no duplicates.',
      'value': 'no_duplicates'
    })

  selected_option = prompt(testing_options)['testing_option']
  return selected_option


def select_rows_number():
  testing_options = [
    {
      'type': 'list',
      'message': 'Select the number of rows to check in each file:',
      'name': 'rows_number',
      'choices': [
        {
          'name': '10 rows',
          'value': 10
        },
        {
          'name': '25 rows',
          'value': 25
        },
        {
          'name': '50 rows',
          'value': 50
        },
        {
          'name': '100 rows',
          'value': 100
        },
        {
          'name': '250 rows',
          'value': 250
        },
        {
          'name': '500 rows',
          'value': 500
        },
        {
          'name': 'All rows',
          'value': 'all'
        },
        {
          'name': 'Custom number',
          'value': 'custom'
        }
      ]
    }
  ]

  selected_option = prompt(testing_options)['rows_number']

  if selected_option == 'custom':
    custom_number = prompt({
      'type': 'input',
      'message': 'Enter the custom number of rows to test in each file:',
      'name': 'custom_number',
      'validate': lambda val: val.isdigit() and int(val) > 0 or 'Please enter a valid positive number'
    })['custom_number']
    selected_option = int(custom_number)

  return selected_option