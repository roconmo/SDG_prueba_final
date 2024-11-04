from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import sys

sys.path.append('/opt/airflow')  # Ensure the root directory is in the Python path

# Import the functions from prueba_sdg_final.py
from prueba_sdg_final import create_docker_files, build_and_run_docker, copy_results_to_host, cleanup_docker_resources
# Default arguments for error handling, retries, etc.
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

# Define the DAG
with DAG(
    dag_id="SDG_Processing_spark",
    default_args=default_args,
    start_date=datetime(2024, 10, 30),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    # Task to create Docker files
    create_docker_files_task = PythonOperator(
        task_id="create_docker_files",
        python_callable=create_docker_files
    )

    # Task to build and run Docker
    build_and_run_docker_task = PythonOperator(
        task_id="build_and_run_docker",
        python_callable=build_and_run_docker
    )

    # Task to copy results from Docker container to host
    copy_results_to_host_task = PythonOperator(
        task_id="copy_results_to_host",
        python_callable=copy_results_to_host
    )

    # Task to clean up Docker resources, regardless of task success or failure
    cleanup_docker_resources_task = PythonOperator(
        task_id="cleanup_docker_resources",
        python_callable=cleanup_docker_resources,
        trigger_rule=TriggerRule.ALL_DONE
    )

    # Define dependencies
    create_docker_files_task >> build_and_run_docker_task >> copy_results_to_host_task >> cleanup_docker_resources_task
