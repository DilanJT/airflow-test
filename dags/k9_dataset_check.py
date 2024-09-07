from airflow import DAG
from airflow.decorators import task
from airflow.datasets import Dataset
from datetime import datetime, timedelta
import json
import os
import requests

# Define the dataset
k9_dataset = Dataset("/opt/airflow/data/k9_facts.json")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='k9_dataset_check',
    default_args=default_args,
    description='Check for updates in K9 dataset and update local file if needed',
    schedule_interval='@daily',
    start_date=datetime(2024, 9, 3),
    catchup=False,
    tags=['k9_care', 'dataset_check'],
) as dag:

    @task(outlets=[k9_dataset])
    def check_and_update_dataset():
        source_url = "https://raw.githubusercontent.com/vetstoria/random-k9-etl/main/source_data.json"
        local_file_path = "/opt/airflow/data/k9_facts.json"

        # Fetch data from source
        response = requests.get(source_url)
        if response.status_code != 200:
            raise Exception(f"Failed to fetch data: HTTP {response.status_code}")
        source_data = response.json()

        # Check if local file exists and read it
        if os.path.exists(local_file_path):
            with open(local_file_path, "r") as file:
                local_data = json.load(file)
        else:
            local_data = []

        # Compare source and local data
        if source_data != local_data:
            print("Changes found in the dataset. Updating local file.")
            with open(local_file_path, "w") as file:
                json.dump(source_data, file, indent=2)
            return True
        else:
            print("No changes in the dataset.")
            return False
        
        

    check_and_update_dataset()