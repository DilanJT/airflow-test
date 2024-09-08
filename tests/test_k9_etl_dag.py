from datetime import datetime, timedelta
from hashlib import md5
import unittest
from unittest.mock import patch, MagicMock
from airflow.models import DagBag, TaskInstance, DagRun, Connection
from airflow.utils.state import State
from airflow.utils.types import DagRunType
from airflow.exceptions import AirflowNotFoundException
from airflow.models.xcom_arg import XComArg
from airflow.exceptions import AirflowException
import pytz
from dags.k9_etl_dag import transform, load_data
from dags.utils import helpers
import pendulum


class TestK9ETLDAG(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dagbag = DagBag(dag_folder="dags", include_examples=False)
        cls.dag_id = "k9_etl_dag"

    def setUp(self):
        self.dag = self.dagbag.get_dag(self.dag_id)
        self.assertIsNotNone(self.dag)
        self.logical_date = pendulum.datetime(2024, 9, 3, tz="UTC")
        self.dag_run = DagRun(
            dag_id=self.dag_id,
            run_id=f"test_{self.dag_id}",
            execution_date=self.logical_date,
            run_type=DagRunType.MANUAL,
        )

    def test_dag_loaded(self):
        self.assertIsNotNone(self.dagbag.get_dag(self.dag_id))
        self.assertEqual(len(self.dagbag.import_errors), 0)

    # TODO: convert this to based on the dataset
    # def test_dag_schedule(self):
    #     self.assertEqual(self.dag.schedule_interval, "@daily")

    def test_dag_tasks(self):
        expected_tasks = [
            "create_k9_table",
            "extract",
            "transform",
            "load_data",
            "check_updates",
            "send_success_email",
            "send_failure_email",
            "log_completion",
        ]
        self.assertEqual(
            sorted(task.task_id for task in self.dag.tasks), sorted(expected_tasks)
        )

    @patch(
        "builtins.open",
        new_callable=unittest.mock.mock_open,
        read_data='[{"fact": "Dogs have 42 teeth", "created_date": "2023-01-01T00:00:00Z"}, {"fact": "Dogs can smell fear", "created_date": "2023-01-02T00:00:00Z"}]',
    )
    def test_extract_task(self, mock_open):
        extract_task = self.dag.get_task("extract")
        task_instance = TaskInstance(task=extract_task, run_id=self.dag_run.run_id)
        task_instance.dag_run = self.dag_run

        with patch("os.path.exists", return_value=True):
            result = extract_task.execute(context={"ti": task_instance})

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["fact"], "Dogs have 42 teeth")
        self.assertEqual(result[1]["fact"], "Dogs can smell fear")


    @patch("airflow.models.connection.Connection.get_connection_from_secrets")
    @patch("psycopg2.connect")
    def test_load_data_task(self, mock_connect, mock_get_connection):
        # Mock the connection
        mock_connection = Connection(
            conn_id="k9_care",
            conn_type="postgres",
            host="localhost",
            schema="k9_care",
            login="airflow",
            password="airflow",
            port=5432,
        )
        mock_get_connection.return_value = mock_connection

        # Mock cursor and connection for the database
        mock_cursor = MagicMock()
        mock_connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = (
            mock_cursor
        )

        mock_cursor.fetchall.return_value = [
            ("existing_fact_id", "Existing fact", "without_numbers")
        ]

        input_data = [
            {
                "fact_id": "new_fact_id",
                "description": "Dogs have 42 teeth",
                "created_date": "2023-01-01T00:00:00Z",
                "category": "with_numbers",
            },
            {
                "fact_id": "existing_fact_id",
                "description": "Updated existing fact",
                "created_date": "2023-01-02T00:00:00Z",
                "category": "without_numbers",
            },
        ]

        result = load_data.function(input_data)

        self.assertEqual(mock_cursor.execute.call_count, 3)

        # Check the SELECT operation
        select_call = mock_cursor.execute.call_args_list[0]
        self.assertIn(
            "SELECT fact_id, description, category FROM k9_facts", select_call[0][0]
        )

        # Check the INSERT operation
        insert_call = mock_cursor.execute.call_args_list[1]
        self.assertIn("INSERT INTO k9_facts", insert_call[0][0])

        # Check the UPDATE operation
        update_call = mock_cursor.execute.call_args_list[2]
        self.assertIn("UPDATE k9_facts", update_call[0][0])

        expected_result = {"inserted": 1, "updated": 1, "deleted": 0}

        # Check load_data task returns the expected result
        self.assertEqual(result, expected_result)

        # Test exception handling
        mock_connect.side_effect = Exception("Database connection failed")
        with self.assertRaises(AirflowException):
            load_data.function(input_data)

    @patch("airflow.models.connection.Connection.get_connection_from_secrets")
    @patch("psycopg2.connect")
    def test_versioning_and_no_duplication(self, mock_connect, mock_get_connection):

        mock_connection = Connection(
            conn_id="k9_care",
            conn_type="postgres",
            host="host.docker.internal",
            schema="airflow_demo",
            login="newuser",
            password="root",
            port=5432,
        )
        mock_get_connection.return_value = mock_connection

        mock_cursor = MagicMock()
        mock_connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = (
            mock_cursor
        )

        # First run: insert new data
        mock_cursor.fetchall.return_value = []

        input_data = [
            {"fact": "Dogs have 42 teeth", "created_date": "2023-01-01T00:00:00Z"}
        ]

        transformed_data = transform.function(input_data)

        result = load_data.function(transformed_data)

        self.assertEqual(mock_cursor.execute.call_count, 2)  # One SELECT, one INSERT

        # Check the SELECT operation
        select_call = mock_cursor.execute.call_args_list[0]
        self.assertIn(
            "SELECT fact_id, description, category FROM k9_facts", select_call[0][0]
        )

        # Check the INSERT operation
        insert_call = mock_cursor.execute.call_args_list[1]
        self.assertIn("INSERT INTO k9_facts", insert_call[0][0])

        self.assertEqual(result["inserted"], 1)
        self.assertEqual(result["updated"], 0)

        # Reset mock_cursor for the second run
        mock_cursor.reset_mock()
        mock_cursor.fetchall.return_value = [
            (transformed_data[0]["fact_id"], "Dogs have 42 teeth", "with_numbers")
        ]

        input_data = [
            {"fact": "Dogs have 42 sharp teeth", "created_date": "2023-01-01T00:00:00Z"}
        ]

        transformed_data = transform.function(input_data)

        result = load_data.function(transformed_data)

        # Check that the correct SQL operations were performed
        self.assertEqual(mock_cursor.execute.call_count, 2)  # One SELECT, one UPDATE

        # Check the SELECT operation
        select_call = mock_cursor.execute.call_args_list[0]
        self.assertIn(
            "SELECT fact_id, description, category FROM k9_facts", select_call[0][0]
        )

        # Check the UPDATE operation
        update_call = mock_cursor.execute.call_args_list[1]
        self.assertIn("UPDATE k9_facts", update_call[0][0])

        self.assertEqual(result["inserted"], 0)
        self.assertEqual(result["updated"], 1)

        # Check that version was incremented and last_modified_date was updated
        self.assert_update_query_correct(
            mock_cursor.execute.call_args_list[1], transformed_data
        )

    def assert_update_query_correct(self, call, transformed_data):
        query, params = call[0]

        # Validate the query structure by ignoring the white spaces
        expected_query = """
            UPDATE k9_facts
            SET description = %s, category = %s, last_modified_date = %s, version = version + 1
            WHERE fact_id = %s
        """
        self.assertEqual(
            " ".join(query.split()),
            " ".join(expected_query.split()),
            "The UPDATE query structure is incorrect",
        )

        # Check the params
        self.assertEqual(params[0], "Dogs have 42 sharp teeth", "Incorrect description")
        self.assertEqual(params[1], "with_numbers", "Incorrect category")
        self.assertIsInstance(
            params[2], datetime, "last_modified_date should be a datetime object"
        )

        # Convert the last_modified_date to UTC if it's timezone
        if params[2].tzinfo is not None:
            last_modified_date = params[2].astimezone(pytz.UTC)
        else:
            last_modified_date = pytz.UTC.localize(params[2])

        # Compare with a UTC now
        now_utc = pytz.UTC.localize(datetime.utcnow())
        self.assertGreater(
            last_modified_date,
            now_utc - timedelta(seconds=10),
            "last_modified_date is not recent",
        )
        self.assertLess(
            last_modified_date,
            now_utc + timedelta(seconds=10),
            "last_modified_date is in the future",
        )

        self.assertEqual(params[3], transformed_data[0]["fact_id"], "Incorrect fact_id")

        # Check if the version is being incremented
        self.assertIn(
            "version = version + 1", query, "Version is not being incremented"
        )

        # Check if last_modified_date is being updated
        self.assertIn(
            "last_modified_date = %s", query, "last_modified_date is not being updated"
        )


if __name__ == "__main__":
    unittest.main()
