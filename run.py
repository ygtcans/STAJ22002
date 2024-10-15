import pandas as pd
from src.data_io_manager import LocalDataHandler, PostgresDataHandler, MySQLDataHandler
from src.db_connections import AWSClient
from src.data_cleaner import DataCleaner

def main():
    mysql = MySQLDataHandler()
    df = mysql.read("test_table")
    print(df.head(1))
if __name__ == '__main__':
    main()