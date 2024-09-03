import os
from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models.connection import Connection
import requests
import json

# Define the DAG
with DAG(
    dag_id="k9_etl_dag",
    description="ETL DAG to extract data from a JSON file, transform it, and load it to a database",
    schedule_interval=None,  # Run the DAG on demand
    start_date=days_ago(1),
    catchup=False,
) as dag:
    
    # Task: Create the k9_facts table if it doesn't exist
    create_pet_table = SQLExecuteQueryOperator(
        task_id="create_k9_table",
        conn_id="k9_care",  # Replace with your actual connection ID
        sql="sql/k9_facts_create.sql"
    )

    # Task 1: Extract data from the JSON file
    @task
    def extract():
        url = "https://raw.githubusercontent.com/vetstoria/random-k9-etl/main/source_data.json"
        response = requests.get(url)
        data = response.json()
        print(data)
        return data

    # Task 2: Transform the data
    @task
    def transform(data: list):
        transformed_data = []
        for item in data:
            fact = item.get("fact", "N/A")
            created_date = item.get("created_date", "N/A")
            transformed_data.append({
                "description": fact,
                "created_date": created_date
            })
        return transformed_data

    # Task 3: Load the transformed data into a text file
    @task
    def load_to_file(transformed_data: list):
        output_dir = "/opt/airflow/output"
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, "k9_output.txt")
        with open(output_file, "w") as f:
            for item in transformed_data:
                f.write(f"• Fact: {item['description']}, Created Date: {item['created_date']}\n")
        print(f"Data loaded into {output_file}")

    # Task 4: Prepare SQL for loading data
    @task
    def prepare_sql(transformed_data: list):
        insert_queries = []
        for item in transformed_data:
            query = f"""
            INSERT INTO k9_facts (created_date, description)
            VALUES ('{item['created_date']}', '{item['description'].replace("'", "''")}');
            """
            insert_queries.append(query)
        return insert_queries

    # Extract and transform data
    extracted_data = extract()
    transformed_data = transform(extracted_data)

    # Load data to file
    load_to_file(transformed_data)

    # Prepare SQL queries
    insert_queries = prepare_sql(transformed_data)

    # Task 5: Load the transformed data into the database
    load_to_db = SQLExecuteQueryOperator(
        task_id='load_to_database',
        conn_id="k9_care",  # Replace with your actual connection ID
        sql=insert_queries,
        autocommit=True,
    )

    create_pet_table >> load_to_db

print("DAG Compiled Successfully")