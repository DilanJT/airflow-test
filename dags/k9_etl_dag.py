import os
from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
# from airflow.providers.
import requests
import json

# Define the DAG
with DAG(
    dag_id="k9_etl_dag",
    description="ETL DAG to extract data from a JSON file, transform it, and load it to a text file",
    schedule_interval=None,  # Run the DAG on demand
    start_date=days_ago(1),
    catchup=False,
) as dag:
    
    # create_pet_table = SQLExecuteQueryOperator(
    #     task_id="create_k9_table",
    #     postgres_conn_id="postgres_default",
    #     sql="sql/k9_schema.sql",
    # )

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
        # Transform the JSON data into a list of strings with bullet points
        transformed_data = []
        for item in data:
            fact = item.get("fact", "N/A")
            created_date = item.get("created_date", "N/A")
            print(created_date)
            transformed_data.append(f"• Fact: {fact}, Created Date: {created_date}")
        
        return transformed_data

    # Task 3: Load the transformed data into a text file
    @task
    def load(transformed_data: list):
        output_dir = "/opt/airflow/output"
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, "k9_output.txt")
        with open(output_file, "w") as f:
            for line in transformed_data:
                f.write(f"{line}\n")
        print(f"Data loaded into {output_file}")

    # Task 5: Load the transformed data into PostgreSQL
    # @task
    # def load_to_postgres(transformed_data: list):
    #     pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    #     insert_query = """
    #     INSERT INTO k9_facts (created_date, description)
    #     VALUES (%s, %s)
    #     """
    #     data_to_insert = []
    #     for line in transformed_data:
    #         fact_parts = line.split(', Created Date: ')
    #         description = fact_parts[0].replace('• Fact: ', '')
    #         created_date = fact_parts[1]
    #         data_to_insert.append((created_date, description))
    #     pg_hook.insert_rows(table='k9_facts', rows=data_to_insert, target_fields=['created_date', 'description'])
    #     print("Data loaded into the PostgreSQL database")

    # Define the task dependencies
    data = extract()
    transformed_data = transform(data)
    load(transformed_data)