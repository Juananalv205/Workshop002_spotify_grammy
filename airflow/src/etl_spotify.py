# src/etl_spotify.py
import pandas as pd
from unidecode import unidecode

class EtlSpotifyAirflow:
    def __init__(self, data):
        """
        Initializes the EtlSpotifyAirflow class with Spotify dataset.

        Args:
            data (DataFrame): The Spotify data as a pandas DataFrame.
        """
        self.spotify_data = data
    
    # Attempt to clean the specified column by filling NaNs, stripping whitespace, and converting to lowercase
    """
        Cleans the data of an individual column.
        
        Args:
            column (Series): The column data to be cleaned.

        Returns:
            Series: The cleaned column data.
    """
    def clean_column(self, column):

        try:
            cleaned_column = column.fillna('').str.strip().str.lower()
            cleaned_column = cleaned_column.apply(unidecode)
            cleaned_column[cleaned_column == ''] = None
            return cleaned_column
        except Exception as e:
            print(f"An error occurred: {e}")
            return False

    """
        Cleans the data only of specified columns internally.

        Returns:
            DataFrame: The DataFrame with cleaned columns.
    """
    def clean_columns(self):
        columns_to_clean = ['artists', 'album_name', 'track_name']  # Define columns to clean

        try:
            # Apply cleaning only to the specified columns
            self.spotify_data[columns_to_clean] = self.spotify_data[columns_to_clean].apply(self.clean_column)
            return self.spotify_data
        except Exception as e:
            print(f"An error occurred: {e}")
            return False
        
    # Remove duplicates based on the 'track_id' column while keeping the specified duplicates
    """
        Removes duplicates from a DataFrame based on a specific column.

        Args:
            keep (str): Determines which duplicates to keep. Options are 'first', 'last', or False.

        Returns:
            DataFrame: The DataFrame with duplicates removed.
    """
    def remove_duplicates(self, keep='first'):
        try:
            self.spotify_data.drop_duplicates(subset="track_id", keep=keep, inplace=True)
            return self.spotify_data
        except Exception as e:
            print(f"An error occurred: {e}")
            return False
        
    # Filter records where 'time_signature' is not 0 and 'duration_ms' is greater than 0    
    """
        Filters records where 'time_signature' is not equal to 0 and 'duration_ms' is greater than 0.

        Returns:
            DataFrame: The DataFrame with filtered records.
    """
    def filter_time_signature(self):
        try:
            # Filter records where 'time_signature' is not 0
            self.spotify_data = self.spotify_data[self.spotify_data['time_signature'] != 0]
            # Filter records where 'duration_ms' is greater than 0
            self.spotify_data = self.spotify_data[self.spotify_data['duration_ms'] > 0]
            return self.spotify_data
        except Exception as e:
            print(f"An error occurred during filtering: {e}")
            return False
        
    # Executes the complete ETL process including cleaning, deduplication, and filtering
    """
        Executes the complete ETL process with cleaning, deduplication, and filtering.

        Returns:
            DataFrame: The cleaned and filtered Spotify data.
    """
    def run_etl(self):
        
        # Clean predefined columns
        self.spotify_data = self.clean_columns()
        # Remove duplicates in the 'track_id' column
        self.spotify_data = self.remove_duplicates('track_id')
        # Filter by time_signature greater than 0
        self.filter_time_signature()
        return self.spotify_data
