# Testing script Data Lake & MySQL

This script provides a CLI interface for testing data stored in a data lake storage and MySQL server, to ensure the data matches between the two.

Setup:

    Place the .env file in the project root directory
    Install the required packages: pip install -r requirements.txt

Usage:

To run the script run the file main.py

The script will test the data in the data lake against the MySQL database and report any discrepancies, to help validate data migration from the MySQL database to the data lake.
