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
    'retry_delay': timedelta(minutes=5),
}


def execute_zeppelin_notebook_1(**kwargs):
    # Zeppelin API endpoint to execute a notebook
    zeppelin_api_url = 'http://localhost:8082/api/notebook/job/2JK82P7DB'

    # Make a POST request to execute the Zeppelin notebook
    response = requests.post(zeppelin_api_url)

    # Check if the request was successful (HTTP status code 200)
    if response.status_code == 200:
        print("Zeppelin notebook executed successfully 1.")
    else:
        print(f"Failed to execute Zeppelin notebook 1. Status code: {response.status_code}")
        print(response.text)


# def execute_zeppelin_notebook_2(**kwargs):
#     # Zeppelin API endpoint to execute a notebook
#     zeppelin_api_url = 'http://localhost:8082/api/notebook/job/2JGNWB8KA'

#     # Make a POST request to execute the Zeppelin notebook
#     response = requests.post(zeppelin_api_url)

#     # Check if the request was successful (HTTP status code 200)
#     if response.status_code == 200:
#         print("Zeppelin notebook executed successfully 2.")
#     else:
#         print(f"Failed to execute Zeppelin notebook 2. Status code: {response.status_code}")
#         print(response.text)




# Create the main DAG
dag = DAG(
    'daily_machine_learning_evaluation_DAG',
    default_args=default_args,
    description='daily machine learning model evaluation DAG',
    schedule_interval='@daily',
)
with dag:
    # Use the PythonOperator to execute the Zeppelin notebook
    evaluate_and_store_best_model_metrics = PythonOperator(
        task_id='evaluate_and_store_best_model_metrics',
        python_callable=execute_zeppelin_notebook_1,
        provide_context=True,  # Pass the context to the Python function
    )

    # spark_stream = PythonOperator(
    #     task_id='spark_stream',
    #     python_callable=execute_zeppelin_notebook_2,
    #     provide_context=True,  # Pass the context to the Python function

    # )


    # Set task dependencies
    evaluate_and_store_best_model_metrics