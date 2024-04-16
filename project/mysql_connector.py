import mysql.connector
from dotenv import load_dotenv
import os

class MySQLConnector:
  def __init__(self):
    load_dotenv()
    self.connection = None
    self.cursor = None
  
  def connect(self):
    try:
      self.connection = mysql.connector.connect(
        host=os.getenv('SQL_HOSTNAME'),
        user=os.getenv('SQL_USER'),
        password=os.getenv('SQL_PW'),
        port=3306,
        database="elsp-an-matomo-db"
      )
      if self.connection.is_connected():
        print("Connected to MySQL server successfully!")
        self.cursor = self.connection.cursor()
      else:
        print("Failed to connect to MySQL server.")
    except mysql.connector.Error as error:
      print(f"Error connecting to MySQL server: {error}")
  
  def execute_query(self, query):
    if self.cursor:
      self.cursor.execute(query)
      return self.cursor.fetchall()
    else:
      print("No cursor available. Please connect to the database first.")

  def get_primary_key_columns(self, table_name):
    try:
      if not self.connection or not self.connection.is_connected():
        self.connect()
      if self.cursor:
        query = f"""
          SELECT COLUMN_NAME
          FROM INFORMATION_SCHEMA.COLUMNS
          WHERE TABLE_SCHEMA = 'elsp-an-matomo-db'
          AND TABLE_NAME = '{table_name}'
          AND COLUMN_KEY = 'PRI'
        """
        self.cursor.execute(query)
        primary_key_columns = [row[0] for row in self.cursor.fetchall()]
        return primary_key_columns
      else:
        print("No cursor available. Please connect to the database first.")
    except mysql.connector.Error as error:
      print(f"Error retrieving primary key columns: {error}")
  
  def get_column_info(self, table_name):
    try:
      if not self.connection or not self.connection.is_connected():
        self.connect()
      if self.cursor:
        query = f"DESCRIBE {table_name}"
        column_info = self.execute_query(query)
        return {row[0]: row[1] for row in column_info}
      else:
        print("No cursor available. Please connect to the database first.")
    except mysql.connector.Error as error:
      print(f"Error retrieving column infos: {error}")
  
  def close_connection(self):
    if self.cursor:
      self.cursor.close()
    if self.connection and self.connection.is_connected():
      self.connection.close()
      print("MySQL connection closed.")