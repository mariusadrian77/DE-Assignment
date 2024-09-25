import unittest
from unittest.mock import patch, MagicMock
import io
import pandas as pd
from db_insert import copy_dataframe_to_db

class TestDBInsert(unittest.TestCase):

    @patch('psycopg2.connect')
    def test_copy_dataframe_to_db(self, mock_connect):
        mock_conn = MagicMock()
        mock_cursor = mock_conn.cursor.return_value
        mock_connect.return_value = mock_conn
        
        # Sample dataframe
        df = pd.DataFrame({
            "id": [1],
            "event_type": ["PAGE_VIEW"],
            "timestamp": ["2023-01-01 10:00:00"],
            "customer_id": ["123"],
            "user_agent": ["Mozilla"],
            "ip": ["127.0.0.1"],
            "query": [None],
            "page": ["/home"],
            "referrer": [None],
            "session_id": [1]
        })

        # Call the function
        copy_dataframe_to_db("DATABASE_KEY", df, "webshop_events")

        # Verify that the COPY SQL was executed
        mock_cursor.copy_expert.assert_called_once()

if __name__ == '__main__':
    unittest.main()
