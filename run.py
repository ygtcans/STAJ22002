import pandas as pd
from src.data_io_manager import LocalDataHandler
from src.db_connections import AWSClient
from src.data_cleaner import DataCleaner
from pyspark.sql import SparkSession
def main():

    # SparkSession'ı başlatma
    spark = SparkSession.builder \
        .appName("CSVReader") \
        .getOrCreate()
    # CSV dosyasını okuma
    file_path = "data/US_Airline_Flight_Routes_and_Fares_1993-2024.csv"
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    clean = DataCleaner(df)
    cleaned_df = clean.clean_data()
    # DataFrame'i gösterme
    cleaned_df.show(1)
    clean.stop_spark()
if __name__ == '__main__':
    main()