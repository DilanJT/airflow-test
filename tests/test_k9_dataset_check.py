import unittest
from unittest.mock import patch, MagicMock
from airflow.models import DagBag, TaskInstance, DagRun
from airflow.utils.types import DagRunType
from airflow.models import Variable
from airflow.exceptions import AirflowException
import pendulum
import json
import os


class TestK9DatasetCheckDAG(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dagbag = DagBag(dag_folder="dags", include_examples=False)
        cls.dag_id = "k9_dataset_check"

    def setUp(self):
        self.dag = self.dagbag.get_dag(self.dag_id)
        self.assertIsNotNone(self.dag)
        self.sample_date = pendulum.datetime(2024, 9, 3, tz="UTC")
        self.dag_run = DagRun(
            dag_id=self.dag_id,
            run_id=f"test_{self.dag_id}",
            execution_date=self.sample_date,
            run_type=DagRunType.MANUAL,
        )

    def test_dag_load(self):
        self.assertIsNotNone(self.dagbag.get_dag(self.dag_id))
        self.assertEqual(len(self.dagbag.import_errors), 0)

    def test_dag_structure(self):
        expected_tasks = [
            "check_dataset",
            "check_updates",
            "update_dataset",
            "not_update_dataset",
            "send_update_email",
            "send_no_update_email",
        ]
        self.assertEqual(
            sorted(task.task_id for task in self.dag.tasks), sorted(expected_tasks)
        )

    @patch("requests.get")
    @patch(
        "builtins.open",
        new_callable=unittest.mock.mock_open,
        read_data='[{"fact": "Dogs have 42 teeth"}]',
    )
    @patch("os.path.exists", return_value=True)
    @patch("airflow.models.Variable.get")
    def test_check_dataset_task_no_changes(
        self, mock_variable_get, mock_exists, mock_open, mock_requests_get
    ):
        mock_variable_get.return_value = "http://example.com/api"
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [{"fact": "Dogs have 42 teeth"}]
        mock_requests_get.return_value = mock_response

        check_dataset_task = self.dag.get_task("check_dataset")
        task_instance = TaskInstance(
            task=check_dataset_task, run_id=self.dag_run.run_id
        )
        task_instance.dag_run = self.dag_run

        result = check_dataset_task.execute(context={"ti": task_instance})

        # CHecking if no changes has been detected
        self.assertFalse(result)

    @patch("requests.get")
    @patch(
        "builtins.open",
        new_callable=unittest.mock.mock_open,
        read_data='[{"fact": "Dogs have 42 teeth"}]',
    )
    @patch("os.path.exists", return_value=True)
    @patch("airflow.models.Variable.get")
    def test_check_dataset_task_with_changes(
        self, mock_variable_get, mock_exists, mock_open, mock_requests_get
    ):
        mock_variable_get.return_value = "http://example.com/api"
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {"fact": "Dogs have 42 teeth"},
            {"fact": "Dogs can smell fear"},
        ]
        mock_requests_get.return_value = mock_response

        check_dataset_task = self.dag.get_task("check_dataset")
        task_instance = TaskInstance(
            task=check_dataset_task, run_id=self.dag_run.run_id
        )
        task_instance.dag_run = self.dag_run

        result = check_dataset_task.execute(context={"ti": task_instance})

        # Checking if changes are detected
        self.assertTrue(result)
        mock_open.assert_called_with("/opt/airflow/data/k9_facts.json", "w")

        # Check if the write method was called with the correct data
        expected_json = json.dumps(
            [{"fact": "Dogs have 42 teeth"}, {"fact": "Dogs can smell fear"}], indent=2
        )
        write_calls = mock_open().write.call_args_list
        written_data = "".join(call.args[0] for call in write_calls)
        self.assertEqual(written_data, expected_json)

    @patch("airflow.operators.python.BranchPythonOperator.execute")
    def test_branch_operator(self, mock_branch_execute):
        branch_task = self.dag.get_task("check_updates")
        task_instance = TaskInstance(task=branch_task, run_id=self.dag_run.run_id)
        task_instance.dag_run = self.dag_run

        # Test when changes are detected
        mock_branch_execute.return_value = ["update_dataset", "send_update_email"]
        result = branch_task.execute(context={"ti": task_instance})
        self.assertEqual(result, ["update_dataset", "send_update_email"])

        # Test when no changes are detected
        mock_branch_execute.return_value = [
            "not_update_dataset",
            "send_no_update_email",
        ]
        result = branch_task.execute(context={"ti": task_instance})
        self.assertEqual(result, ["not_update_dataset", "send_no_update_email"])

    def test_email_tasks(self):
        update_email_task = self.dag.get_task("send_update_email")
        no_update_email_task = self.dag.get_task("send_no_update_email")

        self.assertEqual(
            update_email_task.subject, "K9 Dataset Update Notification - {{ ds }}"
        )
        self.assertEqual(
            no_update_email_task.subject, "K9 Dataset No Update Notification - {{ ds }}"
        )

        self.assertIn("Changes detected in K9 dataset", update_email_task.html_content)
        self.assertIn(
            "No changes detected in K9 dataset", no_update_email_task.html_content
        )


if __name__ == "__main__":
    unittest.main()
