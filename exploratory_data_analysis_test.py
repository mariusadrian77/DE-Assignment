import unittest
from unittest.mock import patch
from exploratory_data_analysis import fetch_and_transform_data, sessionize_data
import pandas as pd

class TestExploratoryDataAnalysis(unittest.TestCase):
    
    @patch('exploratory_data_analysis.requests.get')
    def test_fetch_data(self, mock_get):
        # Mock the response to return status code 200 and valid json data
        mock_get.return_value.status_code = 200
        mock_get.return_value.text = """
        {"id": 463469, "type": "search", "event": {"user-agent": "Mozilla/5.0", "ip": "200.15.173.55", "customer-id": 1234, "timestamp": "2022-04-28T07:38:46.290271", "query": "Synchronized didactic task-force"}}
        {"id": 452437, "type": "page_view", "event": {"user-agent": "Mozilla/5.0", "ip": "121.225.65.59", "customer-id": 5678, "timestamp": "2022-04-28T07:17:46.290271", "page": "https://xcc-webshop.com/cart"}}
        """
        
        # Call the function
        result = fetch_and_transform_data()

        # Check that the result is not None and has the expected number of events
        self.assertIsNotNone(result)
        self.assertEqual(len(result), 2)  # Check that two events are returned

        # Ensure that the 'timestamp' column is correctly created in the result dataframe
        self.assertIn('timestamp', result.columns)

        # Ensure the data is parsed correctly
        self.assertEqual(result.iloc[0]['timestamp'], pd.Timestamp("2022-04-28T07:38:46.290271"))

        # Check if the customer_id and ip are as expected
        self.assertEqual(result.iloc[0]['customer_id'], 1234)
        self.assertEqual(result.iloc[0]['ip'], "200.15.173.55")


    def test_sessionize_data(self):
        # Sample dataframe with customer activity and timestamps
        data = {
            "customer_id": [1234, 1234, 1234, 5678, 5678, 5678],
            "timestamp": pd.to_datetime([
                "2022-04-28 10:00:00",  # First event for customer 1234 (Session 1)
                "2022-04-28 10:05:00",  # Within 8 minutes, still in Session 1
                "2022-04-28 10:15:00",  # More than 8 minutes later, new session (Session 2)
                "2022-04-28 09:00:00",  # First event for customer 5678 (Session 1)
                "2022-04-28 09:05:00",  # Within 8 minutes, still in Session 1
                "2022-04-28 09:20:00"   # More than 8 minutes later, new session (Session 2)
            ]),
            "event_type": ["page_view", "add_to_cart", "page_view", "page_view", "add_to_cart", "checkout"]
        }
        
        df = pd.DataFrame(data)

        # Apply sessionization with an 8-minute timeout
        sessionized_df = sessionize_data(df, session_timeout=8)

        # Check that session ids are assigned correctly for customer 1234
        customer_1234_sessions = sessionized_df[sessionized_df['customer_id'] == 1234]
        self.assertEqual(customer_1234_sessions.iloc[0]['session_id'], 1)  # First session for customer 1234
        self.assertEqual(customer_1234_sessions.iloc[2]['session_id'], 2)  # Second session for customer 1234

        # Check that session ids are assigned correctly for customer 5678
        customer_5678_sessions = sessionized_df[sessionized_df['customer_id'] == 5678]
        self.assertEqual(customer_5678_sessions.iloc[0]['session_id'], 1)  # First session for customer 5678
        self.assertEqual(customer_5678_sessions.iloc[2]['session_id'], 2)  # Second session for customer 5678

        # Check the total number of unique session_ids (should be 2 per customer)
        self.assertEqual(customer_1234_sessions['session_id'].nunique(), 2)
        self.assertEqual(customer_5678_sessions['session_id'].nunique(), 2)

if __name__ == '__main__':
    unittest.main()
