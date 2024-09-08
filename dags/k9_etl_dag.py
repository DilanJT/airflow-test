import os
from airflow import DAG, Dataset
from airflow.decorators import task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models import Connection, Variable
from airflow.operators.python import get_current_context
from airflow.exceptions import AirflowException
from airflow.operators.email import EmailOperator
import json
from datetime import datetime, timedelta, timezone
import logging


k9_dataset = Dataset("k9_facts.json")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_success": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="k9_etl_dag",
    description="ETL DAG for K9 care dog facts",
    default_args=default_args,
    schedule=[k9_dataset],
    start_date=datetime.fromisoformat("2024-09-03"),
    catchup=False,
    tags=["k9_care", "process_dataset"],
) as dag:

    create_pet_table = SQLExecuteQueryOperator(
        task_id="create_k9_table",
        conn_id="{{ var.value.k9_db_conn_id }}",
        sql="/sql/k9_facts_create_table.sql",
    )
    # TODO: change the modified date to modified_at
    @task()
    def extract():
        try:
            file_path = "/opt/airflow/data/k9_facts.json"
            if not os.path.exists(file_path):
                raise Exception(f"File not found: {file_path}")
            with open(file_path, "r") as file:
                data = json.load(file)
            return data
        except Exception as e:
            logging.error(f"Exception occurred in data extraction :{str(e)}")
            raise

    @task()
    def transform(data: list):
        from utils import helpers

        transformed_data = []
        try:
            for item in data:
                fact = item.get("fact", "N/A")
                created_date = item.get("created_date", "N/A")
                category = helpers.categorize_fact(fact)
                fact_id = helpers.encode_to_md5_hash(created_date)
                transformed_data.append(
                    {
                        "fact_id": fact_id,
                        "description": fact,
                        "created_date": created_date,
                        "category": category,
                    }
                )
        except Exception as e:
            logging.error(f"Exception occurred in data transformation :{str(e)}")
            raise
        return transformed_data

    @task()
    def load_data(data):
        import psycopg2
        from psycopg2 import sql

        conn_id = Variable.get("k9_db_conn_id", default_var="k9_care")
        conn = Connection.get_connection_from_secrets(conn_id)

        results = {"inserted": 0, "updated": 0, "deleted": 0}
        try:
            with psycopg2.connect(
                dbname=conn.schema,
                user=conn.login,
                password=conn.password,
                host=conn.host,
                port=conn.port,
            ) as connection:
                with connection.cursor() as cursor:
                    # Get existing fact_ids
                    cursor.execute(
                        "SELECT fact_id, description, category FROM k9_facts"
                    )
                    existing_facts = {
                        row[0]: {"description": row[1], "category": row[2]}
                        for row in cursor.fetchall()
                    }

                    for item in data:
                        fact_id = item["fact_id"]
                        now = datetime.now(timezone.utc)

                        if fact_id in existing_facts:
                            # Check if the record needs to be updated
                            if (
                                item["description"]
                                != existing_facts[fact_id]["description"]
                                or item["category"]
                                != existing_facts[fact_id]["category"]
                            ):
                                cursor.execute(
                                    """
                                    UPDATE k9_facts
                                    SET description = %s, category = %s, last_modified_date = %s, version = version + 1
                                    WHERE fact_id = %s
                                """,
                                    (
                                        item["description"],
                                        item["category"],
                                        now,
                                        fact_id,
                                    ),
                                )
                                results["updated"] += 1
                            del existing_facts[fact_id]
                        else:
                            cursor.execute(
                                """
                                INSERT INTO k9_facts (fact_id, created_date, description, category, last_modified_date, version)
                                VALUES (%s, %s, %s, %s, %s, 0)
                            """,
                                (
                                    fact_id,
                                    item["created_date"],
                                    item["description"],
                                    item["category"],
                                    now,
                                ),
                            )
                            results["inserted"] += 1

                    # delete records that are not in the incoming data
                    if existing_facts:
                        delete_query = sql.SQL(
                            """
                            DELETE FROM k9_facts 
                            WHERE fact_id IN ({})
                        """
                        ).format(
                            sql.SQL(",").join(sql.Placeholder() * len(existing_facts))
                        )
                        cursor.execute(delete_query, tuple(existing_facts.keys()))
                        results["deleted"] = cursor.rowcount

                    connection.commit()
            return results
        except Exception as e:
            raise AirflowException(f"Database operation failed: {str(e)}")

    @task()
    def check_updates(execution_results):
        from utils import helpers

        ti = get_current_context()["ti"]
        transformed_data = ti.xcom_pull(task_ids="transform")

        report = helpers.get_formatted_report(
            len(transformed_data),
            execution_results["inserted"],
            execution_results["updated"],
            execution_results["deleted"],
        )

        if (
            execution_results["inserted"] > 0
            or execution_results["updated"] > 0
            or execution_results["deleted"] > 0
        ):
            report += "There were updates in the 🐶 source dataset."
        else:
            report += "No updates were detected in the 🐶 source dataset."

        return report

    extracted_data = extract()
    transformed_data = transform(extracted_data)
    execution_results = load_data(transformed_data)
    update_report = check_updates(execution_results)

    # Email on success
    email_success = EmailOperator(
        task_id="send_success_email",
        to="{{ var.value.k9_email_recipient }}",
        subject="K9 ETL DAG Completed Successfully",
        html_content="The K9 ETL DAG has completed successfully. Here's the report:<br><br>{{ task_instance.xcom_pull(task_ids='check_updates') }}",
    )

    # Email on failure
    email_failure = EmailOperator(
        task_id="send_failure_email",
        to="{{ var.value.k9_email_recipient }}",
        subject="K9 ETL DAG Failed",
        html_content="The K9 ETL DAG has failed. Please check the Airflow logs for more details.",
        trigger_rule="one_failed",
    )

    @task()
    def log_completion(update_report: str):
        logging.info(f"K9 ETL job completed. Update report:\n{update_report}")
        return "Job completed successfully"

    completion_log = log_completion(update_report)

    # Set up task dependencies
    (
        create_pet_table
        >> extracted_data
        >> transformed_data
        >> execution_results
        >> update_report
        >> completion_log
    )
    completion_log >> [email_success, email_failure]

print("DAG Compiled Successfully")
