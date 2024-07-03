from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.utils.dates import days_ago
import pandas as pd
import requests
from io import StringIO
import pyodbc

# Define the list of file URLs
file_urls = [
    "https://raw.githubusercontent.com/blacksubmarine/data_sources/main/hired_employees.csv",
    "https://raw.githubusercontent.com/blacksubmarine/data_sources/main/departments.csv",
    "https://raw.githubusercontent.com/blacksubmarine/data_sources/main/jobs.csv"
]

default_args = {
    'owner': 'airflow',
    'retries': 0
}

# Define the DAG
with DAG(
    'elt_first_load_bkup',
    default_args=default_args,
    description='A simple DAG to extract, process, and save historical data from multiple files',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['example'],
) as dag:

    def download_and_process_file(file_url, **kwargs):
        response = requests.get(file_url)
        if response.status_code == 200:
            data = StringIO(response.text)
            try:
                df = pd.read_csv(data, on_bad_lines='skip')
                # Process the DataFrame as needed
                print(f"Processed {file_url}:")
                print(df.head())
                # Push the DataFrame to XCom
                kwargs['ti'].xcom_push(key='processed_data', value=df.to_json())
            except pd.errors.ParserError as e:
                print(f"Failed to parse {file_url} due to parsing error: {e}")
        else:
            raise Exception(f"Failed to download file: {file_url}")
        
    def save_processed_file(table_name, **kwargs):
        # Pull the DataFrame from XCom
        df_json = kwargs['ti'].xcom_pull(key='processed_data', task_ids=f'download_and_process_{table_name}')
        if df_json:
            df = pd.read_json(df_json)
            
            full_table_name = f"company.{table_name}"
            # Get MSSQL connection details from Airflow connection
            conn_id = 'mssql_connector_01'
            connection = BaseHook.get_connection(conn_id)
            
            try:
                # Establish the connection using the connection details
                conn = pyodbc.connect(
                    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                    f"SERVER={connection.host};"
                    f"DATABASE=ods_db;"
                    f"UID={connection.login};"
                    f"PWD={connection.password}"
                )
                

                columns = ",".join([f"[{col}]" for col in df.columns]) 
                placeholders = ",".join("?" * len(df.columns))
                # Check if table exists
                cursor = conn.cursor()
                cursor.execute(f"IF OBJECT_ID('company.{table_name}', 'U') IS NULL BEGIN exec create_tables_sp END")
                conn.commit()
                print(f"Table {full_table_name} verified or created.")
                
                # Determine if table is empty
                cursor.execute(f"SELECT COUNT(*) FROM {full_table_name}")
                row_count = cursor.fetchone()[0]
                
                if row_count > 0:
                    # Delete existing data if table is not empty
                    cursor.execute(f"DELETE FROM {full_table_name}")
                    conn.commit()
                    print(f"Deleted existing data from table {full_table_name}.")
                
                # Prepare the insert query
                
                insert_query = f"INSERT INTO {full_table_name} ({columns}) VALUES ({placeholders})"
                
                # Insert rows into the table
                cursor.fast_executemany = True  # Enable fast executemany mode
                cursor.executemany(insert_query, df.values.tolist())
                conn.commit()
                
                print(f"Inserted data into table {full_table_name}.")

                # Close the cursor and connection
                cursor.close()
                conn.close()
                
            except Exception as e:
                print(f"Failed to save data to {full_table_name}: {e}")
        else:
            print(f"DataFrame for {table_name} is None, skipping save.")



    # def save_processed_file(table_name, **kwargs):
    #     # Pull the DataFrame from XCom
    #     df_json = kwargs['ti'].xcom_pull(key='processed_data', task_ids=f'download_and_process_{table_name}')
    #     if df_json:
    #         df = pd.read_json(df_json)

    #         full_table_name = f"company.{table_name}"
    #         # Get MSSQL connection details from Airflow connection
    #         conn_id = 'mssql_connector_01'
    #         connection = BaseHook.get_connection(conn_id)
            
    #         try:
    #             # Establish the connection using the connection details
    #             conn = pyodbc.connect(
    #                 f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    #                 f"SERVER={connection.host};"
    #                 f"DATABASE=ods_db;"  # Using the schema from the connection
    #                 f"UID={connection.login};"
    #                 f"PWD={connection.password}"
    #             )
                
    #             # Check if table exists
    #             cursor = conn.cursor()
    #             table_exists = False
    #             cursor.execute(f"SELECT top 1 1 FROM information_schema.tables WHERE table_schema = 'company' AND table_name = '{table_name}'")
    #             if cursor.fetchone():
    #                 table_exists = True
                
    #             if table_exists:
    #                 # Replace the table
    #                 df.to_sql(table_name, conn, schema='company', if_exists='replace', index=False)
    #                 print(f"Replaced data in table {full_table_name}.")
    #             else:
    #                 # Create the table
    #                 df.to_sql(table_name, conn, schema='company', if_exists='fail', index=False)
    #                 print(f"Created table {full_table_name} and saved data.")
                
    #             # Close the connection
    #             conn.close()
                
    #         except Exception as e:
    #             print(f"Failed to save data to {full_table_name}: {e}")
    #     else:
    #         print(f"DataFrame for {table_name} is None, skipping save.")

    # Create tasks for each file URL
    for file_url in file_urls:
        file_name = file_url.split("/")[-1]
        table_name = file_name.split(".")[0]
        download_task_id = f'download_and_process_{table_name}'
        download_task = PythonOperator(
            task_id=download_task_id,
            python_callable=download_and_process_file,
            op_args=[file_url],
            provide_context=True,
            retries=0
        )

        # Create a corresponding task to save the processed data to SQL Server
        save_task = PythonOperator(
            task_id=f'save_{table_name}',
            python_callable=save_processed_file,
            op_args=[table_name],
            provide_context=True,
            retries=0
        )

        # Set the dependency: download -> save
        download_task >> save_task
