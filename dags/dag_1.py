from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import requests

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


def execute_zeppelin_notebook_1(**kwargs):
    # Zeppelin API endpoint to execute a notebook
    zeppelin_api_url = 'http://localhost:8082/api/notebook/job/2JKKZ7ZNT'

    # Make a POST request to execute the Zeppelin notebook
    response = requests.post(zeppelin_api_url)

    # Check if the request was successful (HTTP status code 200)
    if response.status_code == 200:
        print("Zeppelin notebook executed successfully 1.")
    else:
        print(f"Failed to execute Zeppelin notebook 1. Status code: {response.status_code}")
        print(response.text)

# Create the main DAG
dag = DAG(
    'Extract_data_DAG',
    default_args=default_args,
    description='data collection DAG',
    schedule_interval='*/5 * * * *',
)
with dag:
    # Use the PythonOperator to execute the Zeppelin notebook
    extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=execute_zeppelin_notebook_1,
        provide_context=True,  # Pass the context to the Python function

    )
    # Set task dependencies
    extract_data