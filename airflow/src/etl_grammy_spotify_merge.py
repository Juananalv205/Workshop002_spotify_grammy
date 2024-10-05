import pandas as pd
import os

class EtlGrammySpotifyMerge:
    # Function that initializes the EtlGrammySpotifyMerge class with Grammy and Spotify data, setting the save path.
    """
        Initializes the EtlGrammySpotifyMerge class with Grammy and Spotify datasets and sets the default save path.
        
        Args:
            grammy_data (DataFrame): The Grammy data as a pandas DataFrame.
            spotify_data (DataFrame): The Spotify data as a pandas DataFrame.
            save_path (str): Directory path to save the combined output file. Default is 'data/clean'.
    """
    def __init__(self, grammy_data, spotify_data, save_path='data/clean'):
        self.grammy_data = grammy_data
        self.spotify_data = spotify_data
        self.save_path = save_path

    # Function that merges Grammy and Spotify album data based on partial matches.
    """
        Merges Grammy and Spotify data based on partial matches in album names and artists.

        Returns:
            tuple: (DataFrame with all matched records, DataFrame with Spotify-only records for albums).
    """
    def merge_albums(self):
        merged_data_albums = pd.merge(
            self.grammy_data,
            self.spotify_data,
            how='outer',  # Keep all records from both datasets
            left_on=['artist', 'nominee'],  # Grammy DataFrame columns
            right_on=['artists', 'album_name'],  # Spotify DataFrame columns
            indicator=True  # Indicator to show the origin of each row
        )

        # Add 'grammy_nomination' column to indicate Grammy nominations in album matches
        merged_data_albums['grammy_nomination'] = merged_data_albums['_merge'] == 'both'

        # Filter for records from Spotify that lack Grammy nominations
        only_spotify_albums = merged_data_albums[merged_data_albums['_merge'] == 'right_only']
        print(f"Number of records only in Spotify (albums): {only_spotify_albums.shape[0]}")

        return merged_data_albums, only_spotify_albums

    # Function that merges Grammy and Spotify song data based on song title matches.
    """
        Merges Grammy and Spotify data based on matches in songs.

        Returns:
            tuple: (DataFrame with all matched records, DataFrame with Spotify-only records for songs).
    """
    def merge_songs(self):
        merged_data_songs = pd.merge(
            self.grammy_data,
            self.spotify_data,
            how='outer',  # Keep all records from both datasets
            left_on=['artist', 'nominee'],  # Grammy DataFrame columns
            right_on=['artists', 'track_name'],  # Spotify DataFrame columns
            indicator=True  # Indicator to show the origin of each row
        )

        # Add 'grammy_nomination' column to indicate Grammy nominations in song matches
        merged_data_songs['grammy_nomination'] = merged_data_songs['_merge'] == 'both'

        # Filter for records from Spotify that lack Grammy nominations
        only_spotify_songs = merged_data_songs[merged_data_songs['_merge'] == 'right_only']
        print(f"Number of records only in Spotify (songs): {only_spotify_songs.shape[0]}")

        return merged_data_songs, only_spotify_songs

    # Function that combines merged album and song data, removing duplicates.
    """
        Combines the album and song DataFrames, removing any duplicate records.

        Args:
            merged_data_albums (DataFrame): DataFrame containing merged album data.
            merged_data_songs (DataFrame): DataFrame containing merged song data.

        Returns:
            DataFrame: Combined DataFrame with unique records.
    """
    def combine_data(self, merged_data_albums, merged_data_songs):
        
        combined_data = pd.concat([merged_data_albums, merged_data_songs], ignore_index=True)
        combined_data = combined_data.drop_duplicates()

        print(f"Total number of records in the combined DataFrame: {combined_data.shape[0]}")

        return combined_data

    # Function that saves the combined dataset to an Excel file at a specified path.
    """
        Saves the combined DataFrame to an Excel file at the specified path.

        Args:
            combined_data (DataFrame): DataFrame containing the merged data.
            file_name (str): File name for the saved Excel file. Default is 'combined_data_with_grammy_nomination.xlsx'.
    """
    def save_combined_data(self, combined_data, file_name='combined_data_with_grammy_nomination.xlsx'):
        # Create the directory if it does not exist
        os.makedirs(self.save_path, exist_ok=True)
        # Save the file to the specified location
        file_path = os.path.join(self.save_path, file_name)
        combined_data.to_excel(file_path, index=False)
        print(f"Combined data saved at: {file_path}")

    # Function that executes the entire merging process, combining Grammy and Spotify data and saving the result.
    """
        Executes the merging process, combining Grammy and Spotify data by albums and songs,
        and saves the final combined data to an Excel file.

        Returns:
            DataFrame: The final combined DataFrame with Grammy nominations indicated.
    """
    def run_merge(self):
        # Perform album merging
        merged_data_albums, only_spotify_albums = self.merge_albums()

        # Perform song merging
        merged_data_songs, only_spotify_songs = self.merge_songs()

        # Combine the results
        combined_data = self.combine_data(merged_data_albums, merged_data_songs)

        # Save the combined data to an Excel file
        self.save_combined_data(combined_data)

        return combined_data

