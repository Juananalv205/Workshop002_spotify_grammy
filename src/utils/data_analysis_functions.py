import pandas as pd

class DataAnalyzer:
    def __init__(self, data):
        self.data = data
    
    def summarize_data(self, columns_name):
        """Create a summary of the data."""
        summary = {
            'Column Name': [],
            'Data Type': [],
            'Unique Values': [],
            'Repeated Values': [],
            'Missing Values': [],
            'Number of Duplicates': []
        }

        for column in columns_name:
            summary['Column Name'].append(column)
            summary['Data Type'].append(self.data[column].dtype)
            summary['Unique Values'].append(self.data[column].nunique())
            summary['Repeated Values'].append(self.data[column].size - self.data[column].nunique())
            summary['Missing Values'].append(self.data[column].isnull().sum())
            summary['Number of Duplicates'].append(self.data[column].duplicated().sum())

        return pd.DataFrame(summary)

    def calculate_statistics(self, selected_columns):
        """Calculates statistics for the selected columns."""
        results = []

        for column_name in selected_columns:
            column = self.data[column_name]
            mode_value = round(column.mode()[0], 2)
            variance_value = round(column.var(), 2)
            skewness_value = round(column.skew(), 2)
            kurtosis_value = round(column.kurt(), 2)

            results.append({
                'Column Name': column_name,
                'Mode': mode_value,
                'Variance': variance_value,
                'Skewness': skewness_value,
                'Kurtosis': kurtosis_value,
            })

        return pd.DataFrame(results)

    def analyze_and_combine(self, selected_columns):
        """Combines the statistics with the DataFrame description and returns only the combined DataFrame."""
        # Calculate statistics for the selected columns
        statistics_df = self.calculate_statistics(selected_columns)

        # Combine statistics with the description of the DataFrame
        describe_df = self.data[selected_columns].describe().T.reset_index()
        describe_df.rename(columns={'index': 'Column Name'}, inplace=True)

        combined_df = pd.merge(statistics_df, describe_df, on='Column Name', how='inner')

        return combined_df

