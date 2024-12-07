from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.datafusion import CloudDataFusionStartPipelineOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 7),
    'depends_on_past': False,
    'email': ['lowszekai1@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Creates a DAG object named employee_data
# passes the default_args dictionary to the DAG
# Airflow will not backfill missing runs for previous days
dag = DAG('employee_data',
          default_args=default_args,
          description='Runs an external Python script',
          schedule_interval='@daily',
          catchup=False)

with dag:
    # Executes a bash command to run a python script at a specified location
    run_script_task = BashOperator(
        task_id='extract_data',
        bash_command='python /home/airflow/gcs/dags/scripts/extract.py',
    )

    # Triggers a Cloud Data Fusion pipeline
    start_pipeline = CloudDataFusionStartPipelineOperator(
        location="us-central1",
        pipeline_name="etl-pipeline-2",
        instance_name="datafusion-dev",
        task_id="start_datafusion_pipeline",
    )

    # Sets a dependency between tasks
    run_script_task >> start_pipeline