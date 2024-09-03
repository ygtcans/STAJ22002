import os
import logging
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from data_io_manager import S3DataHandler
from data_transformation import DataTransformator
import pandas as pd

# Utility function to configure logging
def configure_logging(log_dir: str, log_filename: str) -> logging.Logger:
    """
    Configures the logging system.

    Args:
        log_dir (str): The directory to store log files.
        log_filename (str): The name of the log file.

    Returns:
        logging.Logger: Configured logger.
    """
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    log_path = os.path.join(log_dir, log_filename)
    logging.basicConfig(filename=log_path,
                        filemode='a',
                        level=logging.INFO,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    return logging.getLogger(__name__)

# Extraction component
class DataExtractor:
    def __init__(self, input_file_path: str):
        self.input_file_path = input_file_path
        self.logger = configure_logging(log_dir="logs", 
                                        log_filename=f"extract_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log")
        self.spark = SparkSession.builder.appName("Data Extraction").getOrCreate()

    def extract(self) -> DataFrame:
        try:
            data = self.spark.read.csv(self.input_file_path, header=True, inferSchema=True)
            self.logger.info("Data extraction completed successfully.")
            return data
        except Exception as e:
            self.logger.error(f"Error extracting data: {e}")
            raise

    def stop_session(self):
        self.spark.stop()

# Transformation component
class DataTransformer:
    def __init__(self):
        self.transformer = DataTransformator()
        self.logger = configure_logging(log_dir="logs", 
                                        log_filename=f"transform_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log")

    def transform(self, df: DataFrame) -> DataFrame:
        try:
            transformed_data = self.transformer.fill_numeric_missing_values(df)
            transformed_data = self.transformer.fill_string_missing_values(transformed_data)
            transformed_data = self.transformer.replace_outliers_with_capping(transformed_data)
            self.logger.info("Data transformation completed successfully.")
            return transformed_data
        except Exception as e:
            self.logger.error(f"Error transforming data: {e}")
            raise

    def stop_session(self):
        # Implement session stop if necessary in DataTransformator
        pass

# Loading component
class DataLoader:
    def __init__(self, local_dir: str, s3_bucket_name: str, s3_object_name: str):
        self.local_dir = local_dir
        self.s3_bucket_name = s3_bucket_name
        self.s3_object_name = s3_object_name
        self.logger = configure_logging(log_dir="logs",
                                        log_filename=f"load_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log")

    def _save_to_local(self, df: DataFrame) -> str:
        try:
            # Convert DataFrame to Pandas DataFrame
            pandas_df = df.toPandas()

            # Define local file path
            local_file_path = os.path.join(self.local_dir, 'data.csv')
            
            # Ensure the local directory exists
            os.makedirs(self.local_dir, exist_ok=True)

            # Save DataFrame to CSV file
            pandas_df.to_csv(local_file_path, index=False)
            self.logger.info(f"Data saved to local file {local_file_path}.")
            
            return local_file_path
        except Exception as e:
            self.logger.error(f"Error saving data to local file: {e}")
            raise

    def load(self, df: DataFrame) -> None:
        try:
            # Save DataFrame to local file
            local_file_path = self._save_to_local(df)

            # Initialize S3DataHandler and upload the local file
            s3_handler = S3DataHandler()
            s3_handler.write(path=local_file_path, bucket_name=self.s3_bucket_name, object_name=self.s3_object_name)

            self.logger.info("Data loading to S3 completed successfully.")
        except Exception as e:
            self.logger.error(f"Error loading data to S3: {e}")
            raise

# ETL orchestrator
class FlightDataETL:
    def __init__(self, input_file_path: str, local_dir: str, s3_bucket_name: str, s3_object_name: str):
        self.extractor = DataExtractor(input_file_path)
        self.transformer = DataTransformer()
        self.loader = DataLoader(local_dir, s3_bucket_name, s3_object_name)
        self.logger = configure_logging(log_dir="logs", 
                                        log_filename=f"etl_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log")

    def run(self) -> None:
        try:
            data = self.extractor.extract()
            transformed_data = self.transformer.transform(data)
            self.loader.load(transformed_data)
            self.logger.info("ETL process completed successfully.")
        except Exception as e:
            self.logger.error(f"ETL process failed: {e}")
            raise
        finally:
            self.extractor.stop_session()
            self.transformer.stop_session()

# Main function to run the ETL process
if __name__ == "__main__":
    # Define your parameters here
    input_file_path = 'data/US_Airline_Flight_Routes_and_Fares_1993-2024.csv'  # Replace with your input file path
    local_dir = 'transformed_data'  # Replace with your local directory path
    s3_bucket_name = 'ygtcans-test-bucket'  # Replace with your S3 bucket name
    s3_object_name = 'airlane_data'  # Replace with your S3 object name

    # Create an instance of the ETL process
    etl = FlightDataETL(input_file_path, local_dir, s3_bucket_name, s3_object_name)
    
    # Run the ETL process
    etl.run()
