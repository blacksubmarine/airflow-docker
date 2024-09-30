from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

# Define default arguments
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

# Define a function to test the connection and print the result
def test_postgres_query():
    # Create a PostgresHook object to connect to the 'postgres_laptops' connection
    pg_hook = PostgresHook(postgres_conn_id='postgres_laptops')
    
    # Execute the query
    result = pg_hook.get_records('SELECT 1 FROM device.laptops_details;')
    
    # Print the result
    print("Query Result:", result)

# Define the DAG
with DAG(
    dag_id='test_postgres_connection',
    default_args=default_args,
    schedule_interval=None,  # Set to None for manual runs
    catchup=False,
) as dag:

    # PythonOperator to test the PostgreSQL connection and print the query result
    test_postgres_connection = PythonOperator(
        task_id='test_connection',
        python_callable=test_postgres_query
    )

# Set the task order (optional since there's only one task)
test_postgres_connection
