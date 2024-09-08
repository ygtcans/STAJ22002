import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
import logging

class DataCleaner:
    def __init__(self, df):
        """Takes the data and begins processing."""
        self.df = df
        logging.basicConfig(level=logging.INFO)

    def fill_missing_numeric_with_median(self):
        """Fills missing values in numerical columns with the median of the column."""
        try:
            numeric_cols = self.df.select_dtypes(include=['float64', 'int64']).columns
            for col in numeric_cols:
                median_value = self.df[col].median()
                self.df[col].fillna(median_value, inplace=True)
                logging.info(f"Filled missing values in {col} with median value {median_value}")
        except Exception as e:
            logging.error(f"Error filling missing values in numerical columns: {e}")

    def fill_missing_string_with_mode(self):
        """Fills missing values in string (categorical) columns with the most frequent value (mode)."""
        try:
            string_cols = self.df.select_dtypes(include=['object']).columns
            for col in string_cols:
                mode_value = self.df[col].mode()[0]
                self.df[col].fillna(mode_value, inplace=True)
                logging.info(f"Filled missing values in {col} with mode value {mode_value}")
        except Exception as e:
            logging.error(f"Error filling missing values in string columns: {e}")

    def cap_outliers(self, lower_percentile=0.01, upper_percentile=0.99):
        """Caps outliers in numerical columns using the capping method.
           lower_percentile: Lower limit (e.g., 1%)
           upper_percentile: Upper limit (e.g., 99%)"""
        try:
            numeric_cols = self.df.select_dtypes(include=['float64', 'int64']).columns
            for col in numeric_cols:
                lower_bound = self.df[col].quantile(lower_percentile)
                upper_bound = self.df[col].quantile(upper_percentile)
                self.df[col] = self.df[col].clip(lower=lower_bound, upper=upper_bound)
                logging.info(f"Capped outliers in {col} to the range ({lower_bound}, {upper_bound})")
        except Exception as e:
            logging.error(f"Error capping outliers: {e}")

    def remove_duplicate_rows(self):
        """Removes duplicate rows."""
        try:
            self.df = self.df.drop_duplicates()
            logging.info("Removed duplicate rows")
        except Exception as e:
            logging.error(f"Error removing duplicate rows: {e}")

    def remove_highly_correlated_features(self, threshold=0.9):
        """Removes features with high correlation.
           threshold: Correlation threshold value"""
        try:
             # Calculate correlation only for numerical columns
            numeric_cols = self.df.select_dtypes(include=['float64', 'int64']).columns
            if numeric_cols.empty:
                logging.info("No numeric columns available for correlation analysis")
                return

            corr_matrix = self.df[numeric_cols].corr()
            upper = corr_matrix.where(np.triu(np.ones(corr_matrix.shape), k=1).astype(bool))
            to_drop = [column for column in upper.columns if any(upper[column].abs() > threshold)]
            self.df.drop(columns=to_drop, inplace=True)
            logging.info(f"Removed highly correlated features: {to_drop}")
        except Exception as e:
            logging.error(f"Error removing highly correlated features: {e}")


    def clean_data(self):
        """Initiates the data cleaning process."""
        try:
            logging.info("Starting data cleaning process")
            self.cap_outliers()  # Cap outlier values
            self.fill_missing_numeric_with_median()  # Fill numerical columns with median
            self.fill_missing_string_with_mode()  # Fill string columns with mode
            self.remove_duplicate_rows()  # Remove duplicate rows
            self.remove_highly_correlated_features()  # Remove highly correlated features
            logging.info("Data cleaning process completed")
            return self.df
        except Exception as e:
            logging.error(f"Error during data cleaning process: {e}")
