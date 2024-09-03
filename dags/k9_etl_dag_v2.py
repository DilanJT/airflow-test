import os
from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models import Connection
from airflow.operators.python import get_current_context
from airflow.exceptions import AirflowException
from airflow.operators.email import EmailOperator
import requests
import json
from datetime import datetime, timedelta, timezone
import logging
from hashlib import md5
import sql

# Define default_args for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    # 'email': ['your-email@example.com'],
    'email_on_failure': True,
    'email_on_success': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id="k9_etl_dag_v2",
    description="ETL DAG for K9 facts with versioning and error handling",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=['k9_care', 'etl'],
) as dag:
    
    # Task: Create or update the k9_facts table
    create_pet_table = SQLExecuteQueryOperator(
        task_id="create_k9_table",
        conn_id="k9_care",
        sql="""
        CREATE TABLE IF NOT EXISTS k9_facts (
            id SERIAL PRIMARY KEY,
            fact_id TEXT UNIQUE,
            created_date TIMESTAMP,
            description TEXT,
            category VARCHAR(50),
            last_modified_date TIMESTAMP,
            is_deleted BOOLEAN DEFAULT FALSE
        );
        CREATE INDEX IF NOT EXISTS idx_k9_facts_fact_id ON k9_facts(fact_id);
        CREATE INDEX IF NOT EXISTS idx_k9_facts_last_modified ON k9_facts(last_modified_date);
        """
    )

    @task()
    def extract():
        # url = "https://raw.githubusercontent.com/vetstoria/random-k9-etl/main/source_data.json"
        # try:
        #     response = requests.get(url)
        #     response.raise_for_status()
        #     data = response.json()
        #     return data
        # except requests.RequestException as e:
        #     raise AirflowException(f"Failed to fetch data: {str(e)}")

        file_path = '/opt/airflow/data/k9_facts.json'
        if not os.path.exists(file_path):
            raise Exception(f"File not found: {file_path}")
        with open(file_path, 'r') as file:
            data = json.load(file)
        return data

    @task()
    def transform(data: list):
        transformed_data = []
        for item in data:
            fact = item.get("fact", "N/A")
            created_date = item.get("created_date", "N/A") # TODO: instaead of n/a get the date timestamp with milisesonds
            category = "with_numbers" if any(char.isdigit() for char in fact) else "without_numbers"
            fact_id = md5(created_date.encode()).hexdigest()
            transformed_data.append({
                "fact_id": fact_id,
                "description": fact,
                "created_date": created_date,
                "category": category
            })
        return transformed_data

    @task()
    def load_data(data):
        ti = get_current_context()['ti']
        conn = Connection.get_connection_from_secrets("k9_care")
        import psycopg2
        from psycopg2 import sql
        from datetime import datetime
        results = {"inserted": 0, "updated": 0, "deleted": 0}
        try:
            with psycopg2.connect(
                dbname=conn.schema,
                user=conn.login,
                password=conn.password,
                host=conn.host,
                port=conn.port
            ) as connection:
                with connection.cursor() as cursor:
                    # Get existing fact_ids which are not deleted
                    cursor.execute("SELECT fact_id FROM k9_facts WHERE is_deleted = FALSE")
                    existing_fact_ids = set(row[0] for row in cursor.fetchall())

                    for item in data:
                        fact_id = item['fact_id']
                        now = datetime.now(timezone.utc)

                        if fact_id in existing_fact_ids:
                            # Update existing record
                            cursor.execute("""
                                UPDATE k9_facts
                                SET description = %s, category = %s, last_modified_date = %s
                                WHERE fact_id = %s AND (description != %s OR category != %s)
                            """, (item['description'], item['category'], now, fact_id, item['description'], item['category']))
                            if cursor.rowcount > 0:
                                results['updated'] += 1
                            existing_fact_ids.remove(fact_id)
                        else:
                            # Insert new record
                            cursor.execute("""
                                INSERT INTO k9_facts (fact_id, created_date, description, category, last_modified_date)
                                VALUES (%s, %s, %s, %s, %s)
                            """, (fact_id, item['created_date'], item['description'], item['category'], now))
                            results['inserted'] += 1

                    # Mark records as deleted if they're not in the incoming data
                    if existing_fact_ids:
                        delete_query = sql.SQL("UPDATE k9_facts SET is_deleted = TRUE, last_modified_date = %s WHERE fact_id IN ({})").format(
                            sql.SQL(',').join(sql.Placeholder() * len(existing_fact_ids))
                        )
                        cursor.execute(delete_query, (now, *existing_fact_ids))
                        results['deleted'] = cursor.rowcount

                    connection.commit()
            return results
        except Exception as e:
            raise AirflowException(f"Database operation failed: {str(e)}")

    @task()
    def check_updates(execution_results):
        ti = get_current_context()['ti']
        transformed_data = ti.xcom_pull(task_ids='transform')
        
        report = (f"ETL Report for {datetime.now().strftime('%Y-%m-%d')}:\n"
                f"Processed {len(transformed_data)} records.\n"
                f"{execution_results['inserted']} records were inserted.\n"
                f"{execution_results['updated']} records were updated.\n"
                f"{execution_results['deleted']} records were marked as deleted.\n")
        
        if execution_results['inserted'] > 0 or execution_results['updated'] > 0 or execution_results['deleted'] > 0:
            report += "There were updates in the source dataset."
        else:
            report += "No updates were detected in the source dataset."
        
        return report

    # Extract and transform data
    extracted_data = extract()
    transformed_data = transform(extracted_data)

    # Load data and get execution results
    execution_results = load_data(transformed_data)

    # Check for updates and generate report
    update_report = check_updates(execution_results)

    # Send email with update report
    send_report_email = EmailOperator(
        task_id='send_report_email',
        to='dicmandilan@gmail.com',
        subject='K9 Facts ETL Daily Update Report',
        html_content="{{ task_instance.xcom_pull(task_ids='check_updates') }}",
    )

    # Email on success
    email_success = EmailOperator(
        task_id='send_success_email',
        to='dicmandilan@gmail.com',
        subject='K9 ETL DAG Completed Successfully',
        html_content="The K9 ETL DAG has completed successfully. Here's the update report:<br><br>{{ task_instance.xcom_pull(task_ids='check_updates') }}",
    )

    # Email on failure
    email_failure = EmailOperator(
        task_id='send_failure_email',
        to='dicmandilan@gmail.com',
        subject='K9 ETL DAG Failed',
        html_content="The K9 ETL DAG has failed. Please check the Airflow logs for more details.",
        trigger_rule='one_failed',
    )

    # Log completion
    @task()
    def log_completion(update_report: str):
        logging.info(f"K9 ETL job completed. Update report:\n{update_report}")
        return "Job completed successfully"

    completion_log = log_completion(update_report)

    # Set up task dependencies
    create_pet_table >> extracted_data >> transformed_data >> execution_results >> update_report >> completion_log
    # TODO: send email reports
    completion_log >> send_report_email >> [email_success, email_failure]

print("DAG Compiled Successfully")