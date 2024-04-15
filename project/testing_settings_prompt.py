from InquirerPy import prompt
from datetime import datetime, timedelta


def select_data_source():
  data_source_options = [
    {
      'type': 'list',
      'message': 'Select the data source:',
      'name': 'data_source',
      'choices': [
        {
          'name': 'DataLake',
          'value': 'datalake'
        },
        {
          'name': 'MySQL',
          'value': 'mysql'
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
          'name': f'Check that primary keys in {data_source} correspond to the other data source',
          'value': 'primary_keys'
        },
        {
          'name': f'Check that all data in {data_source} correspond to the other data source',
          'value': 'all_data'
        },
        {
          'name': f'Check that the primary keys in {data_source} have no duplicates',
          'value': 'no_duplicates'
        }
      ]
    }
  ]

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