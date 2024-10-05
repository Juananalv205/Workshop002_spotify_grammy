"""
DAG for ETL process of Spotify and Grammy datasets.
This DAG performs the following steps:
1. Load Spotify dataset from PostgreSQL and push it to XCom.
2. Load Grammy dataset from PostgreSQL and push it to XCom.
3. Transform Spotify dataset using the EtlSpotifyAirflow class.
4. Transform Grammy dataset using the EtlGrammyAirflow class.
5. Merge the transformed Spotify and Grammy datasets using the EtlGrammySpotifyMerge class.
6. Infer the schema and create seed data for the merged dataset using the CreateSchemaSeed class.
7. Create a table in PostgreSQL and insert the seed data.
Tasks:
- load_spotify_dataset: Load Spotify dataset from PostgreSQL.
- load_grammy_dataset: Load Grammy dataset from PostgreSQL.
- transform_spotify_data: Transform Spotify dataset.
- transform_grammy_data: Transform Grammy dataset.
- merge_datasets: Merge Spotify and Grammy datasets.
- infer_schema_and_seed: Infer schema and create seed data for the merged dataset.
- create_table_and_insert_data: Create table in PostgreSQL and insert seed data.
Dependencies:
- load_spotify_dataset >> transform_spotify_data >> merge_datasets >> infer_schema_and_seed >> create_table_and_insert_data
- load_grammy_dataset >> transform_grammy_data >> merge_datasets >> infer_schema_and_seed >> create_table_and_insert_data
"""

from airflow import DAG  # Import DAG class for workflow management
from airflow.operators.python import PythonOperator  # Import PythonOperator to run Python functions as tasks
from airflow.utils.dates import days_ago  # Utility for defining start dates relative to the current date
from datetime import datetime  # For setting specific start dates
import pandas as pd  # Import pandas for data manipulation
import sys  # System-specific parameters and functions
import os  # For interacting with the operating system
from datetime import timedelta  # Utility for specifying time intervals

# Add 'src' directory to the Python path to enable module imports for ETL classes
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))
from etl_spotify import EtlSpotifyAirflow
from etl_grammy import EtlGrammyAirflow
from etl_grammy_spotify_merge import EtlGrammySpotifyMerge

# Add 'connections' directory to the Python path for database connection modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'src')))
from connections.db import PostgreSQLConnection
from utils.create_schema_seed import CreateSchemaSeed

# Initialize PostgreSQL database connection service
db_service = PostgreSQLConnection()

# Function to get the full path of an SQL file for running queries
"""
    Constructs the full file path for a given SQL query file located in the 'sql/queries' directory.

    Args:
        filename (str): The name of the SQL query file.

    Returns:
        str: The full path to the SQL query file.
"""
def get_sql_query_path(filename):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(current_dir, '../../sql/queries', filename)

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',  # Owner of the DAG
    'depends_on_past': False,  # Tasks don't wait on previous runs
    'email_on_failure': False,  # Don't send emails on failure
    'email_on_retry': False,  # Don't send emails on retry
    'retries': 1,  # Number of retries on failure
    'retry_delay': timedelta(minutes=5),  # Delay between retries
}

# Define DAG with schedule and parameters
with DAG(
    'etl_process_spotify_grammy',  # DAG name
    default_args=default_args,
    description='Parallel dataset loading and cleaning',
    schedule_interval='@daily',  # Schedule to run daily
    start_date=datetime(2024, 10, 4, 23, 0, 0),  # Start date
    catchup=False,  # Don't catch up past runs
) as dag:

    # Load Spotify dataset into XCom
    """
        Loads the Spotify dataset from the PostgreSQL database into a DataFrame and pushes it to XCom.

        Args:
            **kwargs: Contextual arguments for task, including XCom.

        Returns:
            pd.DataFrame: DataFrame containing Spotify data.
    """
    def load_spotify_dataset(**kwargs):
        df = db_service.create_dataframe(query_path=get_sql_query_path('select_all_rows.sql'), table_name='spotify_staging')
        kwargs['ti'].xcom_push(key='spotify_data', value=df)  # Send DataFrame to XCom
        return df

    # Load Grammy dataset into XCom
    """
        Loads the Grammy dataset from the PostgreSQL database into a DataFrame and pushes it to XCom.

        Args:
            **kwargs: Contextual arguments for task, including XCom.

        Returns:
            pd.DataFrame: DataFrame containing Grammy data.
    """
    def load_grammy_dataset(**kwargs):
        
        df = db_service.create_dataframe(query_path=get_sql_query_path('select_all_rows.sql'), table_name='grammy_staging')
        kwargs['ti'].xcom_push(key='grammy_data', value=df)  # Send DataFrame to XCom
        return df

    # Run Spotify ETL process using XCom data
    """
        Initializes the ETL process for Spotify data using the DataFrame pulled from XCom.

        Args:
            **kwargs: Contextual arguments for task, including XCom.
    """
    def run_etl_spotify_with_data(**kwargs):
        df_spotify = kwargs['ti'].xcom_pull(task_ids='load_spotify_dataset', key='spotify_data')
        etl_spotify = EtlSpotifyAirflow(data=df_spotify)  # Initialize ETL class with data
        etl_spotify.run_etl()  # Execute ETL process

    # Run Grammy ETL process using XCom data
    """
        Initializes the ETL process for Grammy data using the DataFrame pulled from XCom.

        Args:
            **kwargs: Contextual arguments for task, including XCom.
    """
    def run_etl_grammy_with_data(**kwargs):
        df_grammy = kwargs['ti'].xcom_pull(task_ids='load_grammy_dataset', key='grammy_data')
        etl_grammy = EtlGrammyAirflow(data=df_grammy)  # Initialize ETL class with data
        etl_grammy.run_etl()  # Execute ETL process

    # Merge Spotify and Grammy datasets using XCom data
    """
        Merges the Spotify and Grammy datasets by combining DataFrames pulled from XCom.

        Args:
            **kwargs: Contextual arguments for task, including XCom.
    """
    def run_etl_grammy_spotify_merge(**kwargs):
        df_grammy = kwargs['ti'].xcom_pull(task_ids='load_grammy_dataset', key='grammy_data')
        df_spotify = kwargs['ti'].xcom_pull(task_ids='load_spotify_dataset', key='spotify_data')
        etl_merge = EtlGrammySpotifyMerge(grammy_data=df_grammy, spotify_data=df_spotify)  # Initialize ETL merge class
        df_combined = etl_merge.run_merge()  # Run merging process
        kwargs['ti'].xcom_push(key='combined_data', value=df_combined)  # Store merged data in XCom

    # Infer schema and generate seed SQL files for the database
    """
        Infers the PostgreSQL schema and generates seed SQL scripts for the combined dataset, saving the results and pushing them to XCom.

        Args:
            **kwargs: Contextual arguments for task, including XCom.
    """
    def infer_schema_and_seed(**kwargs):
        df_merge = kwargs['ti'].xcom_pull(task_ids='merge_datasets', key='combined_data')
        schema_seed_class = CreateSchemaSeed()  # Initialize schema and seed class
        save_path = 'sql/schema_seed_clean'
        os.makedirs(save_path, exist_ok=True)  # Create directory if it doesn't exist
        schema_path = os.path.join(save_path, "spotify_grammy_clean_schema.sql")
        schema_script = schema_seed_class.infer_schema_postgres(df=df_merge, table_name='spotify_grammy_clean', file_path=schema_path)
        seed_path = os.path.join(save_path, "spotify_grammy_clean_seed.sql")
        seed_script = schema_seed_class.create_seed_postgres(df=df_merge, table_name='spotify_grammy_clean', file_path=seed_path)
        kwargs['ti'].xcom_push(key='schema_data_clean', value=schema_script)  # Store schema script in XCom
        kwargs['ti'].xcom_push(key='seed_data_clean', value=seed_script)  # Store seed script in XCom

    # Load data into PostgreSQL by creating tables and inserting records
    """
        Creates a table in PostgreSQL based on the inferred schema and inserts data using the seed script.

        Args:
            **kwargs: Contextual arguments for task, including XCom.
    """
    def load_data_to_postgres(**kwargs):
        schema_script = kwargs['ti'].xcom_pull(task_ids='infer_schema_and_seed', key='schema_data_clean')
        seed_script = kwargs['ti'].xcom_pull(task_ids='infer_schema_and_seed', key='seed_data_clean')
        db_service.run_query(query=schema_script)  # Create table in PostgreSQL
        db_service.insert_data_from_sql(sql_script=seed_script)  # Insert records into table
    
    # Define tasks to load datasets, transform data, merge datasets, infer schema, and load data into PostgreSQL
    
    #Load Spotify dataset
    """
        Task to load the Spotify dataset.
        Executes the `load_spotify_dataset` function, which queries and loads Spotify data from the database into a DataFrame, 
        then pushes it to XCom for downstream tasks.
        task_id: load_spotify_dataset
    """
    load_dataset_spotify = PythonOperator(
        task_id='load_spotify_dataset',
        python_callable=load_spotify_dataset,
        provide_context=True)

    #Load Grammy dataset
    """
        Task to load the Grammy dataset.
        Executes the `load_grammy_dataset` function, which queries and loads Grammy data from the database into a DataFrame, 
        then pushes it to XCom for downstream tasks.
        task_id: load_grammy_dataset
    """
    load_dataset_grammy = PythonOperator(
        task_id='load_grammy_dataset',
        python_callable=load_grammy_dataset,
        provide_context=True
        
    )
    
    # Transformation task for Spotify
    """
        Transformation task for the Spotify dataset.

        Executes `run_etl_spotify_with_data`, which initializes and runs the ETL process 
        for Spotify data using the `EtlSpotifyAirflow` class, pulling data from XCom.

        task_id: transform_spotify_data
    """
    transform_spotify = PythonOperator(
        task_id='transform_spotify_data',
        python_callable=run_etl_spotify_with_data,
        provide_context=True  
        
    )

    # Transformation task for Grammy
    """
        Transformation task for the Grammy dataset.

        Executes `run_etl_grammy_with_data`, which initializes and runs the ETL process 
        for Grammy data using the `EtlGrammyAirflow` class, pulling data from XCom.

        task_id: transform_grammy_data
    """
    transform_grammy = PythonOperator(
        task_id='transform_grammy_data',
        python_callable=run_etl_grammy_with_data,
        provide_context=True  
        
    )

    # Task to merge datasets
    """
        Task to merge Spotify and Grammy datasets.
        Executes `run_etl_grammy_spotify_merge`, which merges the Spotify and Grammy datasets 
        using `EtlGrammySpotifyMerge` and pushes the combined data to XCom.
        task_id: merge_datasets
    """
    merge_datasets = PythonOperator(
        task_id='merge_datasets',
        python_callable=run_etl_grammy_spotify_merge,
        provide_context=True  
        
    )
    
    
    # Task to infer schema and create seed data
    """
        Task to infer the database schema and create seed data.

        Runs `infer_schema_and_seed`, which generates a schema and seed data SQL scripts 
        for the combined dataset and saves them, also pushing to XCom for later use.

        task_id: infer_schema_and_seed
    """
    infer_schema_seed = PythonOperator(
        task_id='infer_schema_and_seed',
        python_callable=infer_schema_and_seed,
        provide_context=True
    )

    
    # Task to create table and insert data into PostgreSQL
    """
        Task to create a PostgreSQL table and insert data.

        Executes `load_data_to_postgres`, which pulls schema and seed SQL scripts from XCom 
        to create a table and insert data into the PostgreSQL database.

        task_id: create_table_and_insert_data
    """
    create_table_insert_data = PythonOperator(
        task_id='create_table_and_insert_data',
        python_callable=load_data_to_postgres,
        provide_context=True
    )

    # Define task dependencies
    load_dataset_spotify >> transform_spotify >> merge_datasets >> infer_schema_seed >> create_table_insert_data
    load_dataset_grammy >> transform_grammy >> merge_datasets >> infer_schema_seed >> create_table_insert_data
