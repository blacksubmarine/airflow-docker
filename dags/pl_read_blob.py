import logging
import json
import io
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook
from azure.storage.filedatalake import DataLakeServiceClient
from datetime import datetime

# Define the DAG
dag = DAG(
    dag_id='read_laptop_data',
    schedule_interval=None,
    start_date=datetime(2024, 9, 30),
    catchup=False,
)

def read_csv_from_adls():
    # Retrieve the connection using the connection ID
    connection = BaseHook.get_connection('nortal_blob_storage')
    
    # Log connection details for debugging
    logging.info(f"Connection ID: {connection.conn_id}")
    logging.info(f"Extra: {connection.extra}")

    # Retrieve account name and account key from the Extra field
    try:
        extra = json.loads(connection.extra or "{}")  # Safely parse the JSON
        logging.info(f"Extra parsed as JSON: {extra}")  # Log parsed JSON
        
        account_name = extra.get('account_name')
        account_key = extra.get('account_key')

        # Log retrieved account details for debugging
        logging.info(f"Account Name: {account_name}")
        logging.info(f"Account Key: {'***' if account_key else None}")  # Mask the key for security

        # Check if account name and key are retrieved correctly
        if not account_name or not account_key:
            raise ValueError("Account name or account key is missing from the connection details.")
        
        # Construct the connection string
        connection_string = f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net"

        # Create a DataLakeServiceClient using the connection string
        service_client = DataLakeServiceClient.from_connection_string(connection_string)

        # Specify the container and file path
        container_name = "nortal"
        file_path = "raw_devices_data/laptopData.csv"
        
        # Get the file system client
        file_system_client = service_client.get_file_system_client(file_system=container_name)
        
        # Get the file client
        file_client = file_system_client.get_file_client(file_path)
        
        # Download the file and read it into a Pandas DataFrame
        download_stream = file_client.download_file()
        data = download_stream.readall()
        df = pd.read_csv(io.BytesIO(data))

        # Print the first 5 rows of the DataFrame
        print(df.head(5))

    except Exception as e:
        logging.error(f"Error in reading CSV from ADLS: {e}")
        raise

# Define the task
read_csv_task = PythonOperator(
    task_id='read_csv',
    python_callable=read_csv_from_adls,
    dag=dag,
)

# Set task dependencies if needed
