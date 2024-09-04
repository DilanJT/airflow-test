import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone
from airflow.models import Connection, DAG
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowException

# Import your DAG file. Adjust the import path as necessary.
from dags.k9_etl_dag_v3 import transform, load_data

class TestK9ETLDag(unittest.TestCase):

    def setUp(self):
        self.sample_data = [
            {"fact": "Dogs have 42 teeth", "created_date": "2023-09-01T12:00:00Z"},
            {"fact": "A dog's sense of smell is much stronger than humans", "created_date": "2023-09-02T12:00:00Z"},
        ]
        
        # Create a test DAG
        self.dag = DAG(
            'test_dag',
            default_args={'owner': 'airflow'},
            schedule='@daily',
            start_date=datetime(2021, 1, 1),
        )

    def test_transform_categorization(self):
        with self.dag:
            transform_task = transform(self.sample_data)
        
        transformed_data = transform_task.operator.execute(context={})
        
        self.assertEqual(transformed_data[0]['category'], 'with_numbers')
        self.assertEqual(transformed_data[1]['category'], 'without_numbers')

    def test_transform_fact_id_generation(self):
        with self.dag:
            transform_task = transform(self.sample_data)
        
        transformed_data = transform_task.operator.execute(context={})
        
        self.assertIsNotNone(transformed_data[0]['fact_id'])
        self.assertIsNotNone(transformed_data[1]['fact_id'])
        self.assertNotEqual(transformed_data[0]['fact_id'], transformed_data[1]['fact_id'])

    @patch('psycopg2.connect')
    @patch('airflow.models.Connection.get_connection_from_secrets')
    def test_load_data_insertion(self, mock_get_connection, mock_connect):
        mock_connection = MagicMock()
        mock_get_connection.return_value = mock_connection

        mock_cursor = MagicMock()
        mock_connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cursor

        # Simulate empty database
        mock_cursor.fetchall.return_value = []

        with self.dag:
            transform_task = transform(self.sample_data)
            load_task = load_data(transform_task)

        # Execute transform task
        transformed_data = transform_task.operator.execute(context={})

        # Mock TaskInstance and XCom
        ti = MagicMock()
        ti.xcom_pull.return_value = transformed_data

        # Execute load task
        context = {'ti': ti}
        result = load_task.operator.execute(context=context)

        self.assertEqual(result['inserted'], 2)
        self.assertEqual(result['updated'], 0)
        self.assertEqual(result['deleted'], 0)

        # Check if INSERT was called with correct data
        insert_calls = [call for call in mock_cursor.execute.call_args_list if 'INSERT INTO k9_facts_v3' in str(call)]
        self.assertEqual(len(insert_calls), 2)

    @patch('psycopg2.connect')
    @patch('airflow.models.Connection.get_connection_from_secrets')
    def test_load_data_update(self, mock_get_connection, mock_connect):
        mock_connection = MagicMock()
        mock_get_connection.return_value = mock_connection

        mock_cursor = MagicMock()
        mock_connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cursor

        with self.dag:
            transform_task = transform(self.sample_data)
            load_task = load_data(transform_task)

        # Execute transform task
        transformed_data = transform_task.operator.execute(context={})

        # Simulate existing data in the database
        existing_data = [
            (item['fact_id'], "Old description", "old_category")
            for item in transformed_data
        ]
        mock_cursor.fetchall.return_value = existing_data

        # Mock TaskInstance and XCom
        ti = MagicMock()
        ti.xcom_pull.return_value = transformed_data

        # Execute load task
        context = {'ti': ti}
        result = load_task.operator.execute(context=context)

        self.assertEqual(result['inserted'], 0)
        self.assertEqual(result['updated'], 2)
        self.assertEqual(result['deleted'], 0)

        # Check if UPDATE was called with correct data
        update_calls = [call for call in mock_cursor.execute.call_args_list if 'UPDATE k9_facts_v3' in str(call)]
        self.assertEqual(len(update_calls), 2)

    @patch('psycopg2.connect')
    @patch('airflow.models.Connection.get_connection_from_secrets')
    def test_load_data_modified_date_update(self, mock_get_connection, mock_connect):
        mock_connection = MagicMock()
        mock_get_connection.return_value = mock_connection

        mock_cursor = MagicMock()
        mock_connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cursor

        with self.dag:
            transform_task = transform(self.sample_data)
            load_task = load_data(transform_task)

        # Execute transform task
        transformed_data = transform_task.operator.execute(context={})

        # Simulate existing data in the database
        existing_data = [
            (item['fact_id'], item['description'], item['category'])
            for item in transformed_data
        ]
        mock_cursor.fetchall.return_value = existing_data

        # Modify one of the facts
        modified_data = self.sample_data.copy()
        modified_data[0]['fact'] = "Dogs have 42 teeth (updated)"

        with self.dag:
            new_transform_task = transform(modified_data)
            new_load_task = load_data(new_transform_task)

        # Execute new transform task
        new_transformed_data = new_transform_task.operator.execute(context={})

        # Mock TaskInstance and XCom
        ti = MagicMock()
        ti.xcom_pull.return_value = new_transformed_data

        # Execute new load task
        context = {'ti': ti}
        result = new_load_task.operator.execute(context=context)

        self.assertEqual(result['inserted'], 0)
        self.assertEqual(result['updated'], 1)
        self.assertEqual(result['deleted'], 0)

        # Check if UPDATE was called with last_modified_date
        update_calls = [call for call in mock_cursor.execute.call_args_list if 'UPDATE k9_facts_v3' in str(call)]
        self.assertEqual(len(update_calls), 1)
        self.assertIn('last_modified_date', str(update_calls[0]))

if __name__ == '__main__':
    unittest.main()