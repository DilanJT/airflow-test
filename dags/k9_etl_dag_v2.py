import logging
import os
from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.models.connection import Connection
from airflow.operators.python import get_current_context
import requests
import json
from datetime import datetime

# Define the DAG
with DAG(
    dag_id="k9_etl_dag_v2",
    description="ETL DAG to extract data from a JSON file, transform it, and load it to a database",
    schedule_interval="@daily",  # Run the DAG daily
    start_date=days_ago(1),
    catchup=False,
) as dag:
    
    # Task: Create the k9_facts_v2 table if it doesn't exist
    create_pet_table = SQLExecuteQueryOperator(
        task_id="create_k9_table",
        conn_id="k9_care",
        sql="""
        CREATE TABLE IF NOT EXISTS k9_facts_v2 (
            id SERIAL PRIMARY KEY,
            fact_id INTEGER,
            created_date TIMESTAMP,
            description TEXT,
            category VARCHAR(50),
            version INTEGER,
            is_current BOOLEAN
        );
        """
    )

    # Task 1: Extract data from the JSON file
    @task()
    def extract():
        url = "https://raw.githubusercontent.com/vetstoria/random-k9-etl/main/source_data.json"
        response = requests.get(url)
        if response.status_code != 200:
            raise Exception(f"Failed to fetch data: HTTP {response.status_code}")
        data = response.json()
        return data

    # Task 2: Transform the data
    @task()
    def transform(data: list):
        transformed_data = []
        for item in data:
            fact = item.get("fact", "N/A")
            created_date = item.get("created_date", "N/A")
            
            # Categorize facts
            if any(char.isdigit() for char in fact):
                category = "with_numbers"
            else:
                category = "without_numbers"
            
            transformed_data.append({
                "fact_id": int(item.get("id", 0)),
                "description": fact,
                "created_date": created_date,
                "category": category
            })
        return transformed_data

    # Task 3: Load the transformed data into a text file
    @task()
    def load_to_file(transformed_data: list):
        output_dir = "/opt/airflow/output"
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, f"k9_output_{datetime.now().strftime('%Y%m%d')}.txt")
        with open(output_file, "w") as f:
            for item in transformed_data:
                f.write(f"• Fact ID: {item['fact_id']}, Category: {item['category']}, Fact: {item['description']}, Created Date: {item['created_date']}\n")
        print(f"Data loaded into {output_file}")

    # Task 4: Prepare SQL for loading data
    @task()
    def prepare_sql(transformed_data: list):
        insert_queries = []
        for item in transformed_data:
            query = f"""
            WITH upsert AS (
                UPDATE k9_facts_v2
                SET is_current = FALSE
                WHERE fact_id = {item['fact_id']} AND is_current = TRUE
                RETURNING fact_id
            )
            INSERT INTO k9_facts_v2 (fact_id, created_date, description, category, version, is_current)
            SELECT 
                {item['fact_id']},
                '{item['created_date']}',
                '{item['description'].replace("'", "''")}',
                '{item['category']}',
                COALESCE((SELECT MAX(version) FROM k9_facts_v2 WHERE fact_id = {item['fact_id']}), 0) + 1,
                TRUE
            WHERE NOT EXISTS (SELECT 1 FROM upsert);
            """
            insert_queries.append(query)
        return insert_queries

    # Task 5: Check for new updates
    @task()
    def check_updates(transformed_data: list):
        conn = Connection.get_connection_from_secrets("k9_care")
        import psycopg2
        with psycopg2.connect(
            dbname=conn.schema,
            user=conn.login,
            password=conn.password,
            host=conn.host,
            port=conn.port
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT MAX(fact_id) FROM k9_facts_v2;")
                max_fact_id = cursor.fetchone()[0] or 0

        new_facts = [item for item in transformed_data if item['fact_id'] > max_fact_id]
        if new_facts:
            Variable.set("new_facts_count", len(new_facts))
            return f"Found {len(new_facts)} new facts."
        else:
            return "No new facts found."

    # Extract and transform data
    extracted_data = extract()
    transformed_data = transform(extracted_data)

    # Load data to file
    file_output = load_to_file(transformed_data)

    # Check for updates
    update_check = check_updates(transformed_data)

    # Prepare SQL queries
    insert_queries = prepare_sql(transformed_data)

    # Task 6: Load the transformed data into the database
    load_to_db = SQLExecuteQueryOperator(
        task_id='load_to_database',
        conn_id="k9_care",
        sql=insert_queries,
        autocommit=True,
    )

    @task()
    def log_completion():
        context = get_current_context()
        ti = context['ti']
        update_result = ti.xcom_pull(task_ids="check_updates")
        logging.info(f"K9 ETL job completed. Update result: {update_result}")
        return "Job completed successfully"

    completion_log = log_completion()

    # Set up task dependencies
    create_pet_table >> extracted_data >> transformed_data >> file_output
    transformed_data >> update_check
    [file_output, update_check] >> insert_queries >> load_to_db >> completion_log

print("DAG Compiled Successfully")