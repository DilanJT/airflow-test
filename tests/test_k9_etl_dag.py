from hashlib import md5
import unittest
from unittest.mock import patch, MagicMock
from airflow.models import DagBag, TaskInstance, DagRun, Connection
from airflow.utils.state import State
from airflow.utils.types import DagRunType
from airflow.exceptions import AirflowNotFoundException
from airflow.models.xcom_arg import XComArg
from airflow.exceptions import AirflowException
from dags.k9_etl_dag_v3 import transform, load_data
import pendulum

class TestK9ETLDAG(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dagbag = DagBag(dag_folder="dags", include_examples=False)
        cls.dag_id = "k9_etl_dag_v3"

    def setUp(self):
        self.dag = self.dagbag.get_dag(self.dag_id)
        self.assertIsNotNone(self.dag)
        self.logical_date = pendulum.datetime(2024, 9, 3, tz="UTC")
        self.dag_run = DagRun(
            dag_id=self.dag_id,
            run_id=f"test_{self.dag_id}",
            execution_date=self.logical_date,
            run_type=DagRunType.MANUAL
        )

    def test_dag_loaded(self):
        self.assertIsNotNone(self.dagbag.get_dag(self.dag_id))
        self.assertEqual(len(self.dagbag.import_errors), 0)

    def test_dag_schedule(self):
        self.assertEqual(self.dag.schedule_interval, "@daily")

    def test_dag_tasks(self):
        expected_tasks = [
            "create_k9_table",
            "extract",
            "transform",
            "load_data",
            "check_updates",
            "send_report_email",
            "send_success_email",
            "send_failure_email",
            "log_completion",
        ]
        self.assertEqual(sorted(task.task_id for task in self.dag.tasks), sorted(expected_tasks))

    @patch('builtins.open', new_callable=unittest.mock.mock_open, read_data='[{"fact": "Dogs have 42 teeth", "created_date": "2023-01-01T00:00:00Z"}, {"fact": "Dogs can smell fear", "created_date": "2023-01-02T00:00:00Z"}]')
    def test_extract_task(self, mock_open):
        extract_task = self.dag.get_task("extract")
        task_instance = TaskInstance(task=extract_task, run_id=self.dag_run.run_id)
        task_instance.dag_run = self.dag_run
        
        with patch('os.path.exists', return_value=True):
            result = extract_task.execute(context={"ti": task_instance})
        
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["fact"], "Dogs have 42 teeth")
        self.assertEqual(result[1]["fact"], "Dogs can smell fear")

    # def test_transform_task(self):
    #     transform_task = self.dag.get_task("transform")
        
    #     input_data = [
    #         {"fact": "Dogs have 42 teeth", "created_date": "2023-01-01T00:00:00Z"},
    #         {"fact": "Dogs can smell fear", "created_date": "2023-01-02T00:00:00Z"},
    #     ]
        
    #     task_instance = TaskInstance(task=transform_task, run_id=self.dag_run.run_id)
    #     task_instance.dag_run = self.dag_run
        
    #     # Mock the XCom pull to return the input_data
    #     with patch.object(task_instance, 'xcom_pull', return_value=input_data):
    #         result = transform_task.execute(context={"ti": task_instance})
        
    #     self.assertEqual(len(result), 2)
    #     self.assertEqual(result[0]["category"], "with_numbers")
    #     self.assertEqual(result[1]["category"], "without_numbers")
    def calculate_md5(self, input_string):
        """ Helper function to calculate MD5 hash of a string """
        return md5(input_string.encode()).hexdigest()

    def test_transform_task(self):
        # Define the input data that would come from the XCom or upstream task
        input_data = [
            {"fact": "Dogs have 42 teeth", "created_date": "2023-01-01T00:00:00Z"},
            {"fact": "Dogs can smell fear", "created_date": "2023-01-02T00:00:00Z"},
        ]

        # Directly call the underlying function of the transform task
        result = transform.function(input_data)

        # Dynamically calculate expected MD5 values to avoid hardcoding them
        expected_result = [
            {
                "fact_id": self.calculate_md5("2023-01-01T00:00:00Z"),  # Dynamically calculate
                "description": "Dogs have 42 teeth",
                "created_date": "2023-01-01T00:00:00Z",
                "category": "with_numbers"
            },
            {
                "fact_id": self.calculate_md5("2023-01-02T00:00:00Z"),  # Dynamically calculate
                "description": "Dogs can smell fear",
                "created_date": "2023-01-02T00:00:00Z",
                "category": "without_numbers"
            }
        ]

        # Assert that the transform task returns the expected transformed data
        self.assertEqual(result, expected_result)

    @patch('airflow.models.connection.Connection.get_connection_from_secrets')
    @patch('psycopg2.connect')
    def test_load_data_task(self, mock_connect, mock_get_connection):
        # Mock the connection
        mock_connection = Connection(
            conn_id='k9_care',
            conn_type='postgres',
            host='localhost',
            schema='k9_care',
            login='airflow',
            password='airflow',
            port=5432
        )
        mock_get_connection.return_value = mock_connection

        # Mock cursor and connection for the database
        mock_cursor = MagicMock()
        mock_connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cursor

        # Mock existing data in the database
        mock_cursor.fetchall.return_value = [
            ("existing_fact_id", "Existing fact", "without_numbers")
        ]

        # Input data for the load_data function
        input_data = [
            {
                "fact_id": "new_fact_id",
                "description": "Dogs have 42 teeth",
                "created_date": "2023-01-01T00:00:00Z",
                "category": "with_numbers"
            },
            {
                "fact_id": "existing_fact_id",
                "description": "Updated existing fact",
                "created_date": "2023-01-02T00:00:00Z",
                "category": "without_numbers"
            }
        ]

        # Call the load_data function
        result = load_data.function(input_data)

        # Assert that the correct SQL operations were performed
        self.assertEqual(mock_cursor.execute.call_count, 3)  # One insert, one update

        # Check the insert operation
        insert_call = mock_cursor.execute.call_args_list[0]
        self.assertIn("INSERT INTO k9_facts_v3", insert_call[0][0])

        # Check the update operation
        update_call = mock_cursor.execute.call_args_list[1]
        self.assertIn("UPDATE k9_facts_v3", update_call[0][0])

        # Expected result from the load_data task
        expected_result = {
            "inserted": 1,  # One new fact is inserted
            "updated": 1,   # One existing fact is updated
            "deleted": 0    # No facts are deleted
        }

        # Assert that the load_data task returns the expected result
        self.assertEqual(result, expected_result)

        # Test exception handling
        mock_connect.side_effect = Exception("Database connection failed")
        with self.assertRaises(AirflowException):
            load_data.function(input_data)

    @patch('airflow.models.connection.Connection.get_connection_from_secrets')
    @patch('psycopg2.connect')
    def test_versioning_and_no_duplication(self, mock_connect, mock_get_connection):
        # Mock the connection
        mock_connection = Connection(
            conn_id='k9_care',
            conn_type='Postgres',
            host='host.docker.internal',
            schema='airflow_demo',
            login='newuser',
            password='root',
            port=5432
        )
        mock_get_connection.return_value = mock_connection

        load_data_task = self.dag.get_task("load_data")
        
        mock_cursor = MagicMock()
        mock_connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cursor
        
        # First run: insert new data
        mock_cursor.fetchall.return_value = []
        
        input_data = [
            {
                "fact_id": "fact1",
                "description": "Dogs have 42 teeth",
                "created_date": "2023-01-01T00:00:00Z",
                "category": "with_numbers"
            },
        ]
        
        task_instance = TaskInstance(task=load_data_task, run_id=self.dag_run.run_id)
        task_instance.dag_run = self.dag_run
        
        # Mock the XCom pull to return the input_data
        with patch.object(task_instance, 'xcom_pull', return_value=input_data):
            result = load_data_task.execute(context={"ti": task_instance})
        
        self.assertEqual(result["inserted"], 1)
        self.assertEqual(result["updated"], 0)
        
        # Second run: update existing data
        mock_cursor.fetchall.return_value = [
            ("fact1", "Dogs have 42 teeth", "with_numbers")
        ]
        
        input_data = [
            {
                "fact_id": "fact1",
                "description": "Dogs have 42 sharp teeth",
                "created_date": "2023-01-01T00:00:00Z",
                "category": "with_numbers"
            },
        ]
        
        # Mock the XCom pull to return the updated input_data
        with patch.object(task_instance, 'xcom_pull', return_value=input_data):
            result = load_data_task.execute(context={"ti": task_instance})
        
        self.assertEqual(result["inserted"], 0)
        self.assertEqual(result["updated"], 1)
        
        # Check that version was incremented and last_modified_date was updated
        mock_cursor.execute.assert_called_with(
            """
            UPDATE k9_facts_v3
            SET description = %s, category = %s, last_modified_date = %s, version = version + 1
            WHERE fact_id = %s
        """,
            ("Dogs have 42 sharp teeth", "with_numbers", unittest.mock.ANY, "fact1")
        )

    # def test_check_updates_task(self):
    #     input_data = [
    #         {"fact": "Dogs have 42 teeth", "created_date": "2023-01-01T00:00:00Z"},
    #         {"fact": "Dogs can smell fear", "created_date": "2023-01-02T00:00:00Z"}
    #     ]

    #     transformed_data = transform.function(input_data)

    #     execution_results = {
    #         "inserted": 1,
    #         "updated": 2,
    #         "deleted": 0
    #     }

    #     result = check_updates.function(transformed_data)

    #     expected_result = execution_results

    #     self.assertEqual(result, expected_result)

if __name__ == '__main__':
    unittest.main()