from hashlib import md5
import os
from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.models.connection import Connection
from airflow.operators.python import get_current_context
import requests
import json
from datetime import datetime
import logging

# Define the DAG
with DAG(
    dag_id="k9_etl_dag_v4",
    description="ETL DAG to extract data from a JSON file, transform it, load to a database, and handle deletions",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
) as dag:
    
    # Task: Create the k9_facts_v4 table if it doesn't exist
    create_pet_table = SQLExecuteQueryOperator(
        task_id="create_k9_table",
        conn_id="k9_care",
        sql="""
        CREATE TABLE IF NOT EXISTS k9_facts_v4 (
            id SERIAL PRIMARY KEY,
            fact_id INTEGER,
            created_date TIMESTAMP,
            description TEXT,
            category VARCHAR(50),
            version INTEGER,
            is_current BOOLEAN,
            is_deleted BOOLEAN DEFAULT FALSE
        );
        """
    )

    @task()
    def extract():
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
            created_date = item.get("created_date", "N/A")
             # Categorize facts
            if any(char.isdigit() for char in fact):
                category = "with_numbers"
            else:
                category = "without_numbers"

            fact_id = int(md5(f"{fact}{created_date}".encode()).hexdigest(), 16) % (10 ** 8)
            transformed_data.append({
                "fact_id": fact_id,
                "description": fact,
                "created_date": created_date,
                "category": category
            })
        return transformed_data

    @task()
    def prepare_sql(transformed_data: list):
        insert_queries = []
        for item in transformed_data:
            query = f"""
            WITH current_data AS (
                SELECT version
                FROM k9_facts_v4
                WHERE fact_id = {item['fact_id']} AND is_current = TRUE
            ),
            max_version AS (
                SELECT COALESCE(MAX(version), 0) as max_ver
                FROM k9_facts_v4
            ),
            upsert AS (
                UPDATE k9_facts_v4
                SET is_current = FALSE
                WHERE fact_id = {item['fact_id']} AND is_current = TRUE
                RETURNING fact_id
            )
            INSERT INTO k9_facts_v4 (fact_id, created_date, description, category, version, is_current, is_deleted)
            SELECT 
                {item['fact_id']},
                '{item['created_date']}',
                '{item['description'].replace("'", "''")}',
                '{item['category']}',
                CASE 
                    WHEN EXISTS (SELECT 1 FROM current_data) THEN 2  -- Updated record
                    WHEN EXISTS (SELECT 1 FROM k9_facts_v4) THEN (SELECT max_ver + 1 FROM max_version)  -- New record after initial load
                    ELSE 1  -- First record or part of initial load
                END,
                TRUE,
                FALSE
            WHERE NOT EXISTS (
                SELECT 1 FROM current_data
                WHERE version = 1
            ) OR EXISTS (SELECT 1 FROM upsert);
            """
            insert_queries.append(query)
        
        # Add a query to mark deleted records
        delete_query = f"""
        UPDATE k9_facts_v4
        SET is_current = FALSE, is_deleted = TRUE
        WHERE fact_id NOT IN ({','.join(str(item['fact_id']) for item in transformed_data)})
          AND is_current = TRUE
          AND is_deleted = FALSE;
        """
        insert_queries.append(delete_query)
        
        return insert_queries

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
                cursor.execute("SELECT COUNT(*) FROM k9_facts_v4 WHERE is_current = TRUE AND is_deleted = FALSE;")
                current_count = cursor.fetchone()[0]

                cursor.execute("SELECT COUNT(*) FROM k9_facts_v4 WHERE is_deleted = TRUE;")
                deleted_count = cursor.fetchone()[0]

        new_facts_count = len(transformed_data) - current_count + deleted_count
        Variable.set("new_facts_count", new_facts_count)
        return f"Found {new_facts_count} new facts. {deleted_count} facts were deleted."

    # Extract and transform data
    extracted_data = extract()
    transformed_data = transform(extracted_data)

    # Prepare SQL queries
    insert_queries = prepare_sql(transformed_data)

    # Load the transformed data into the database
    load_to_db = SQLExecuteQueryOperator(
        task_id='load_to_database',
        conn_id="k9_care",
        sql=insert_queries,
        autocommit=True,
    )

    # Check for updates
    update_check = check_updates(transformed_data)

    @task()
    def log_completion():
        context = get_current_context()
        ti = context['ti']
        update_result = ti.xcom_pull(task_ids="check_updates")
        logging.info(f"K9 ETL job completed. Update result: {update_result}")
        return "Job completed successfully"

    completion_log = log_completion()

    # Set up task dependencies
    create_pet_table >> extracted_data >> transformed_data >> insert_queries >> load_to_db >> update_check >> completion_log

print("DAG Compiled Successfully")