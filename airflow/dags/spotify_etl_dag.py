#Import Libraries
import sys
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import re
from unidecode import unidecode

class ETL_spotify_Airflow:
    def __init__(self, data):
        self.spotify_data = data

    def delete_columns(df, columns):
        """
        Elimina columnas de un DataFrame.

        Args:
            df (pd.DataFrame): El DataFrame del cual se eliminarán las columnas.
            columns (list or str): Una lista de nombres de columnas a eliminar o un solo nombre de columna.

        Returns:
            pd.DataFrame: El DataFrame sin las columnas especificadas.
        """
        try:
            # Verifica si columns es una lista o una cadena
            if isinstance(columns, str):
                columns = [columns]  # Convierte en lista si es un solo string

            # Elimina las columnas especificadas
            df.drop(columns, axis=1, inplace=True)
            
            return df
        except KeyError as e:
            print(f"Error: {e}. Verifica que las columnas existan en el DataFrame.")
            return False
        except Exception as e:
            print(f"Ocurrió un error: {e}")
            return False

    def clean_column(column):
        try:
            # Rellenar valores None con una cadena vacía, eliminar espacios, convertir a minúsculas y quitar tildes
            cleaned_column = column.fillna('').str.strip().str.lower()
            cleaned_column = cleaned_column.apply(unidecode)
            
            # Reemplazar cadenas vacías con None (NaN)
            cleaned_column[cleaned_column == ''] = None
            return cleaned_column
        except Exception as e:
            print(f"Ocurrió un error: {e}")
            return False
    
    
            