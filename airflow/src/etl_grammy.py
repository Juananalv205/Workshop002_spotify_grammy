import pandas as pd
import re
from unidecode import unidecode

class EtlGrammyAirflow:
    # Initializes the EtlGrammyAirflow class with Grammy dataset.
    """
        Initializes the EtlGrammyAirflow class with Grammy dataset.
        
        Args:
            data (DataFrame): The Grammy data as a pandas DataFrame.
    """
    def __init__(self, data):
        self.grammy_data = data

    # Simplifies the Grammy award title in the 'title' column.
    """
        Modifies the Grammy award title to simplify it.
    """
    def simplify_grammy_title(self):
        def simplify_title(title):
            match = re.search(r'(\d+(?:st|nd|rd|th)) Annual GRAMMY Awards', title)
            if match:
                number_ordinal = match.group(1)
                if number_ordinal == "1st":
                    return f"{number_ordinal} GRAMMY Award"
                else:
                    return f"{number_ordinal} GRAMMY Awards"
            return title

        self.grammy_data['title'] = self.grammy_data['title'].apply(simplify_title)
        return self.grammy_data

    # Marks the first winner as True in the 'winner' column, and others as False.
    """
        Marks the first entry as True in the 'winner' column and others as False.
    """
    def mark_winner(self):
        def mark_first_winner(group):
            group['winner'] = [True] + [False] * (len(group) - 1)
            return group
        self.grammy_data = self.grammy_data.groupby(['year', 'category'], group_keys=False).apply(mark_first_winner)
        return self.grammy_data

    # Extracts the artist's name from the 'workers' column and assigns it to the 'artist' column.
    """
        Extracts the artist's name from the 'workers' column and assigns it to the 'artist' column.
    """
    def extract_artist_from_workers(self):
        def extract_artist(workers):
            try:
                match = re.search(r'\((.*?)\)', str(workers))
                return match.group(1) if match else None
            except Exception as e:
                return None

        self.grammy_data['artist'] = self.grammy_data.apply(
            lambda row: extract_artist(row['workers']) if pd.isna(row['artist']) and not pd.isna(row['workers']) else row['artist'],
            axis=1
        )
        return self.grammy_data

    # Fills missing artist values in 'artist' using 'nominee' values.
    """
        If 'artist' is missing but 'nominee' is present, fills 'artist' with 'nominee' value.
    """
    def fill_missing_artists(self):
        self.grammy_data['artist'] = self.grammy_data['artist'].fillna(self.grammy_data['nominee'])
        return self.grammy_data

    # Removes records where 'artist' is null.
    """
        Removes records where the 'artist' column is null.
    """
    def remove_null_artists(self):
        self.grammy_data = self.grammy_data.dropna(subset=['artist'])
        return self.grammy_data

    # Cleans the format of the 'category', 'artist', and 'nominee' columns.
    """
        Cleans the 'category', 'artist', and 'nominee' columns.
    """
    def clean_columns(self):
        def clean_column(column):
            column = column.fillna('').str.strip().str.lower()
            column = column.apply(unidecode)
            column[column == ''] = None
            return column

        columns_to_clean = ['category', 'artist', 'nominee']
        self.grammy_data[columns_to_clean] = self.grammy_data[columns_to_clean].apply(clean_column)
        return self.grammy_data

    # Filters categories based on specific keywords.
    """
        Filters categories that contain specific keywords.
    """
    def filter_categories(self):
        unique_categories = self.grammy_data['category'].unique()
        unique_categories_df = pd.DataFrame(unique_categories, columns=['category'])

        key_words = ['album', 'r&b', 'song', 'artist', 'vocal', 'performance', 'record']
        pattern = '|'.join(key_words)

        filtered_categories = unique_categories_df[unique_categories_df['category'].str.contains(pattern, case=False, regex=True)]
        unique_categories = filtered_categories['category'].unique()

        self.grammy_data = self.grammy_data[self.grammy_data['category'].isin(unique_categories)]
        return self.grammy_data

    # Executes the complete ETL process on the Grammy dataset.
    """
        Executes the complete ETL process, including:
        - Simplifying Grammy award titles
        - Marking the first entry as the winner
        - Extracting artist names from the 'workers' column
        - Filling missing artist entries
        - Removing records with missing artists
        - Cleaning specified columns
        - Filtering categories based on keywords
        Returns the cleaned Grammy dataset.
    """
    def run_etl(self):
        # Simplify Grammy award title
        self.simplify_grammy_title()
        
        # Mark the first entry as the Grammy winner
        self.mark_winner()
        
        # Extract artist's name from the 'workers' column
        self.extract_artist_from_workers()
        
        # Fill missing 'artist' entries using 'nominee' values
        self.fill_missing_artists()
        
        # Remove records where 'artist' is still missing
        self.remove_null_artists()
        
        # Clean 'category', 'artist', and 'nominee' column formats
        self.clean_columns()
        
        # Filter categories based on specific keywords
        self.filter_categories()
        
        # Print confirmation that ETL process is complete
        print("ETL process completed successfully.")
        
        # Return the transformed Grammy dataset
        return self.grammy_data
