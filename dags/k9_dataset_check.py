import logging
from airflow import DAG
from airflow.decorators import task
from airflow.datasets import Dataset
from airflow.operators.python import BranchPythonOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
import json
import os
import requests

DATASET_IDENTIFIER = os.environ.get('K9_DATASET_PATH', '/opt/airflow/data/k9_facts.json')
SOURCE_URL = os.environ.get('K9_SOURCE_URL', 'https://raw.githubusercontent.com/vetstoria/random-k9-etl/main/source_data.json')
EMAIL_RECIPIENT = os.environ.get('K9_EMAIL_RECIPIENT', 'default@example.com')
START_DATE = os.environ.get('K9_START_DATE', '2024-09-03')


k9_dataset = Dataset(DATASET_IDENTIFIER)

def choose_branch(ti):
    changes_detected = ti.xcom_pull(task_ids="check_dataset")
    if changes_detected:
        return ["update_dataset", "send_update_email"]
    else:
        return ["not_update_dataset", "send_no_update_email"]


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
}


with DAG(
    dag_id="k9_dataset_check",
    default_args=default_args,
    description="Check for updates in K9 dataset and update local file if needed",
    schedule_interval="@daily",
    start_date=datetime.isoformat(START_DATE),
    catchup=False,
    tags=["k9_care", "dataset_check"],
) as dag:

    @task()
    def check_dataset():
        source_url = SOURCE_URL
        local_file_path = "/opt/airflow/data/k9_facts.json"

        try:
            response = requests.get(source_url)
            if response.status_code != 200:
                raise Exception(f"Failed to fetch data: HTTP {response.status_code}")
            source_data = response.json()

            if os.path.exists(local_file_path):
                with open(local_file_path, "r") as file:
                    local_data = json.load(file)
            else:
                local_data = []

            # Compare source and local data
            if source_data != local_data:
                logging.info("Changes found in the dataset. Updating local file.")
                with open(local_file_path, "w") as file:
                    json.dump(source_data, file, indent=2)
                return True
            else:
                print("No changes in the dataset.")
                return False
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to fetch data: {str(e)}")
            raise
        except IOError as e:
            logging.error(f"File operation error: {str(e)}")
            raise
        except Exception as e:
            logging.error(f"Exception occured: {str(e)}")
            raise


    @task(outlets=[k9_dataset])
    def update_dataset():
        logging.info("Updating dataset and triggering downstream DAG.")


    @task()
    def not_update_dataset():
        logging.info("No updates, downstream dag will not be triggered")


    check_updates = BranchPythonOperator(
        task_id="check_updates", python_callable=choose_branch
    )

    email_update = EmailOperator(
        task_id="send_update_email",
        to=EMAIL_RECIPIENT,
        subject="K9 Dataset Update Notification - {{ ds }}",
        html_content="Changes detected in K9 dataset on {{ ds }}. Triggering k9_etl_dag.",
    )

    email_no_update = EmailOperator(
        task_id="send_no_update_email",
        to=EMAIL_RECIPIENT,
        subject="K9 Dataset No Update Notification - {{ ds }}",
        html_content="No changes detected in K9 dataset on {{ ds }}. k9_etl_dag will not be triggered.",
    )
    
    # Triggers the dataset only if there are updates in the source file
    (
        check_dataset()
        >> check_updates
        >> [update_dataset(), not_update_dataset(), email_update, email_no_update]
    )
