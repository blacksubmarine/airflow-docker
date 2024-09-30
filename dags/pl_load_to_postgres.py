import logging
import json
import io
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook
from azure.storage.filedatalake import DataLakeServiceClient
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime

# Define the DAG
dag = DAG(
    dag_id='read_and_insert_laptops_data',
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

        # Clean the DataFrame
        df = df.dropna(subset=['Unnamed: 0'])  # Drop rows where 'Unnamed: 0' is NaN
        df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)  # Trim whitespace
        df.fillna('Not Available', inplace=True)  # Replace NaN values with 'Not Available'

        # Remove 'kg' and cast 'Weight' to numeric
        df['Weight'] = pd.to_numeric(df['Weight'].str.replace('kg', ''), errors='coerce')
        df['Inches'] = pd.to_numeric(df['Inches'], errors='coerce')
        

        # Drop rows where 'Inches' or 'Weight' are NaN after conversion
        df.dropna(subset=['Inches', 'Weight'], inplace=True)

        # Save the cleaned DataFrame to a CSV for the next task
        df.to_csv('/tmp/cleaned_laptops_data.csv', index=False)
        logging.info("Cleaned DataFrame saved to /tmp/cleaned_laptops_data.csv")

    except Exception as e:
        logging.error(f"Error in reading CSV from ADLS: {e}")
        raise

def insert_data_into_postgres():
    # Load the cleaned DataFrame
    df = pd.read_csv('/tmp/cleaned_laptops_data.csv')

    # Create a PostgresHook object to connect to the PostgreSQL database
    pg_hook = PostgresHook(postgres_conn_id='postgres_laptops')

    # Insert records into PostgreSQL
    for _, row in df.iterrows():
        insert_query = """
        INSERT INTO device.laptops_details (Company, TypeName, Inches, ScreenResolution, Cpu, Ram, Memory, Gpu, OpSys, Weight, Price)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        pg_hook.run(insert_query, parameters=(row['Company'], row['TypeName'], row['Inches'],
                                               row['ScreenResolution'], row['Cpu'], row['Ram'],
                                               row['Memory'], row['Gpu'], row['OpSys'], row['Weight'],
                                               row['Price']))

    logging.info("Data inserted into PostgreSQL successfully.")

# Define the tasks
read_csv_task = PythonOperator(
    task_id='read_csv',
    python_callable=read_csv_from_adls,
    dag=dag,
)

insert_data_task = PythonOperator(
    task_id='insert_data',
    python_callable=insert_data_into_postgres,
    dag=dag,
)

# Set task dependencies
read_csv_task >> insert_data_task
