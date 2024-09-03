from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, mean, stddev, when
from pyspark.sql.types import IntegerType, FloatType, DoubleType, StringType
import logging
from datetime import datetime
import os

class DataTransformator:
    def __init__(self, app_name="Data Transformation", log_dir="data_transformation_logs", z_threshold=3):
        """
        The DataTransformator class performs data cleaning and transformation operations.

        Args:
            app_name (str): The name of the Spark application.
            log_dir (str): The directory where logs should be saved.
            z_threshold (float): The Z-score threshold for detecting outliers.
        """
        self.spark = SparkSession.builder.appName(app_name).getOrCreate()
        self.z_threshold = z_threshold
        
        # Ensure log directory exists
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        # Configure logging
        log_filename = os.path.join(log_dir, f"data_transformation_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log")
        logging.basicConfig(filename=log_filename,
                            filemode='a',
                            level=logging.INFO,
                            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)

    def fill_numeric_missing_values(self, df: DataFrame) -> DataFrame:
        """
        Fills missing values in numeric columns with the mean.
        
        Args:
            df (DataFrame): The PySpark DataFrame to process.
        
        Returns:
            DataFrame: PySpark DataFrame with missing values filled.
        """
        try:
            numeric_cols = [col_name for col_name in df.columns if isinstance(df.schema[col_name].dataType, (IntegerType, FloatType, DoubleType))]
            for col_name in numeric_cols:
                mean_value = df.select(mean(col(col_name))).first()[0]
                if mean_value is not None:
                    df = df.fillna({col_name: mean_value})
                    self.logger.info(f"Filled missing values in numeric column '{col_name}' with mean value {mean_value}.")
            return df
        except Exception as e:
            self.logger.error(f"Error filling numeric missing values: {e}")
            raise

    def fill_string_missing_values(self, df: DataFrame) -> DataFrame:
        """
        Fills missing values in string columns with the mode or empty string.
        
        Args:
            df (DataFrame): The PySpark DataFrame to process.
        
        Returns:
            DataFrame: PySpark DataFrame with missing values filled.
        """
        try:
            string_cols = [col_name for col_name in df.columns if isinstance(df.schema[col_name].dataType, StringType)]
            for col_name in string_cols:
                mode_value_row = df.groupBy(col_name).count().orderBy('count', ascending=False).first()
                mode_value = mode_value_row[0] if mode_value_row and mode_value_row[0] is not None else ""
                df = df.fillna({col_name: mode_value})
                self.logger.info(f"Filled missing values in string column '{col_name}' with mode value '{mode_value}'.")
            return df
        except Exception as e:
            self.logger.error(f"Error filling string missing values: {e}")
            raise

    def replace_outliers_with_capping(self, df: DataFrame) -> DataFrame:
        """
        Identifies outliers based on Z-score and caps them at the threshold limit.

        Args:
            df (DataFrame): The PySpark DataFrame to process.

        Returns:
            DataFrame: PySpark DataFrame with outliers capped at threshold limits.
        """
        try:
            def calculate_z_scores(df: DataFrame, col_name: str):
                mean_value = df.select(mean(col(col_name))).first()[0]
                std_dev = df.select(stddev(col(col_name))).first()[0]
                return mean_value, std_dev
            
            numeric_cols = [col_name for col_name in df.columns if isinstance(df.schema[col_name].dataType, (IntegerType, FloatType, DoubleType))]
            for col_name in numeric_cols:
                mean_value, std_dev = calculate_z_scores(df, col_name)
                if mean_value is not None and std_dev is not None:
                    lower_limit = mean_value - self.z_threshold * std_dev
                    upper_limit = mean_value + self.z_threshold * std_dev
                    df = df.withColumn(
                        col_name,
                        when(col(col_name) < lower_limit, lower_limit)
                        .when(col(col_name) > upper_limit, upper_limit)
                        .otherwise(col(col_name))
                    )
                    self.logger.info(f"Capped outliers in numeric column '{col_name}' to [{lower_limit}, {upper_limit}].")
            return df
        except Exception as e:
            self.logger.error(f"Error capping outliers: {e}")
            raise

    def stop_session(self):
        """
        Stops the SparkSession.
        
        Returns:
            None
        """
        self.spark.stop()
