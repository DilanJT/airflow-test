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
    dag_id="k9_etl_dag_v6",
    description="ETL DAG with correct versioning strategy",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
) as dag:
    
    # Task: Create or update the k9_facts_v6 table
    create_pet_table = SQLExecuteQueryOperator(
        task_id="create_k9_table",
        conn_id="k9_care",
        sql="""
        CREATE TABLE IF NOT EXISTS k9_facts_v6 (
            id SERIAL PRIMARY KEY,
            fact_id TEXT,
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
            category = "with_numbers" if any(char.isdigit() for char in fact) else "without_numbers"
            fact_id = md5(fact.encode()).hexdigest()
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
        
        # Get the current max version
        max_version_query = "SELECT COALESCE(MAX(version), 0) FROM k9_facts_v6;"
        insert_queries.append(max_version_query)
        
        # Get all existing fact_ids
        existing_facts_query = "SELECT fact_id FROM k9_facts_v6 WHERE is_current = TRUE;"
        insert_queries.append(existing_facts_query)
        
        for item in transformed_data:
            query = f"""
            WITH current_data AS (
                SELECT version, is_deleted
                FROM k9_facts_v6
                WHERE fact_id = '{item['fact_id']}'::varchar AND is_current = TRUE
            ),
            update_existing AS (
                UPDATE k9_facts_v6
                SET is_current = FALSE
                WHERE fact_id = '{item['fact_id']}'::varchar AND is_current = TRUE
                RETURNING 1
            )
            INSERT INTO k9_facts_v6 (fact_id, created_date, description, category, version, is_current, is_deleted)
            SELECT 
                '{item['fact_id']}'::varchar,
                '{item['created_date']}'::timestamp,
                '{item['description'].replace("'", "''")}'::text,
                '{item['category']}'::varchar,
                CASE 
                    WHEN NOT EXISTS (SELECT 1 FROM current_data) THEN 
                        CASE 
                            WHEN (SELECT COUNT(*) FROM k9_facts_v6) = 0 THEN 1
                            ELSE (SELECT MAX(version) FROM k9_facts_v6 WHERE is_current = TRUE)
                        END
                    ELSE (SELECT version FROM current_data)
                END,
                TRUE,
                FALSE
            WHERE 
                NOT EXISTS (SELECT 1 FROM current_data WHERE is_deleted = FALSE)
                OR EXISTS (SELECT 1 FROM update_existing);
            """
            insert_queries.append(query)
        
        return insert_queries

    @task()
    def execute_sql(queries):
        ti = get_current_context()['ti']
        conn = Connection.get_connection_from_secrets("k9_care")
        import psycopg2
        results = []
        with psycopg2.connect(
            dbname=conn.schema,
            user=conn.login,
            password=conn.password,
            host=conn.host,
            port=conn.port
        ) as connection:
            with connection.cursor() as cursor:
                # Execute max version query
                cursor.execute(queries[0])
                max_version = cursor.fetchone()[0]
                
                # Execute existing facts query
                cursor.execute(queries[1])
                existing_facts = set(row[0] for row in cursor.fetchall())
                
                # Process the rest of the queries
                for query in queries[2:]:
                    cursor.execute(query)
                    if cursor.rowcount > 0:
                        results.append(cursor.rowcount)
                
                # Get transformed data from XCom
                transformed_data = ti.xcom_pull(task_ids='transform')
                new_facts = set(item['fact_id'] for item in transformed_data)
                
                # Mark deleted records
                deleted_facts = existing_facts - new_facts
                if deleted_facts:
                    delete_query = f"""
                    WITH deleted_records AS (
                        SELECT fact_id
                        FROM k9_facts_v6
                        WHERE fact_id::varchar IN ({','.join(f"'{f}'" for f in deleted_facts)})
                        AND is_current = TRUE
                        AND is_deleted = FALSE
                    ),
                    update_current AS (
                        UPDATE k9_facts_v6
                        SET is_current = FALSE
                        WHERE id IN (SELECT k9_facts_v6.id 
                                    FROM k9_facts_v6 
                                    JOIN deleted_records 
                                    ON k9_facts_v6.fact_id::varchar = deleted_records.fact_id::varchar)
                        RETURNING fact_id
                    )
                    INSERT INTO k9_facts_v6 (fact_id, created_date, description, category, version, is_current, is_deleted)
                    SELECT 
                        k9_facts_v6.fact_id, 
                        k9_facts_v6.created_date, 
                        k9_facts_v6.description, 
                        k9_facts_v6.category, 
                        {max_version + 1},
                        TRUE, 
                        TRUE
                    FROM k9_facts_v6
                    JOIN update_current ON k9_facts_v6.fact_id::varchar = update_current.fact_id::varchar
                    WHERE k9_facts_v6.is_current = FALSE
                    AND k9_facts_v6.is_deleted = FALSE;
                    """
                    cursor.execute(delete_query)
                    results.append(cursor.rowcount)
                
                connection.commit()
        return results

    @task()
    def check_updates(execution_results):
        ti = get_current_context()['ti']
        transformed_data = ti.xcom_pull(task_ids='transform')
        new_records = len([r for r in execution_results if r > 0])
        deleted_records = execution_results[-1] if execution_results and execution_results[-1] > 0 else 0
        
        return f"Processed {len(transformed_data)} records. {new_records} new or updated records. {deleted_records} records marked as deleted."

    # Extract and transform data
    extracted_data = extract()
    transformed_data = transform(extracted_data)

    # Prepare and execute SQL queries
    insert_queries = prepare_sql(transformed_data)
    execution_results = execute_sql(insert_queries)

    # Check for updates
    update_check = check_updates(execution_results)

    @task()
    def log_completion(update_result: str):
        logging.info(f"K9 ETL job completed. Update result: {update_result}")
        return "Job completed successfully"

    completion_log = log_completion(update_check)

    # Set up task dependencies
    create_pet_table >> extracted_data >> transformed_data >> insert_queries >> execution_results >> update_check >> completion_log

print("DAG Compiled Successfully")