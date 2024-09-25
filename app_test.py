import unittest
from unittest.mock import patch, MagicMock
from app import app, get_median_visits_before_order_query, get_median_session_duration_before_order_query

class TestApp(unittest.TestCase):

    # Setup for the Flask test client
    def setUp(self):
        self.app = app.test_client()
        self.app.testing = True

    # Test the root endpoint
    def test_home(self):
        response = self.app.get('/')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data.decode(), "Session Analysis API is running.")

    # Test the metrics endpoint by mocking the database query function
    @patch('app.fetch_single_value_from_db')
    def test_order_metrics(self, mock_fetch):
        # Mock return values for the median visits and session duration
        mock_fetch.side_effect = [3, 120]  # 3 visits, 120 minutes duration
        
        # Call the /metrics/orders endpoint
        response = self.app.get('/metrics/orders')
        
        # Verify the status code
        self.assertEqual(response.status_code, 200)
        
        # Verify the returned JSON data
        expected_data = {
            "median_visits_before_order": 3,
            "median_session_duration_minutes_before_order": 120
        }
        self.assertEqual(response.json, expected_data)

        # Ensure the function was called twice (once for each query)
        self.assertEqual(mock_fetch.call_count, 2)

        # Verify that the correct queries were passed
        mock_fetch.assert_any_call(get_median_visits_before_order_query())
        mock_fetch.assert_any_call(get_median_session_duration_before_order_query())

    # Test database connection failure handling in fetch_single_value_from_db
    @patch('app.psycopg2.connect', side_effect=Exception("Database connection failed"))
    def test_db_connection_failure(self, mock_connect):
        from app import fetch_single_value_from_db

        # Call the fetch function and check the exception handling
        result = fetch_single_value_from_db('SELECT 1')
        self.assertEqual(result, "Database connection failed")

if __name__ == '__main__':
    unittest.main()
