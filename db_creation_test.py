import unittest
from unittest.mock import patch, MagicMock
import psycopg2
from db_creation import create_db_table, manage_database


class TestDBCreation(unittest.TestCase):
    
    @patch('psycopg2.connect')
    def test_db_connection_success(self, mock_connect):
        mock_conn = MagicMock()
        mock_cursor = mock_conn.cursor.return_value
        mock_connect.return_value = mock_conn
        
        # Call the function to create the database table
        create_db_table()
        
        # Check if psycopg2.connect() was called
        mock_connect.assert_called_once()
        # Verify that the correct SQL query is executed for creating the table
        mock_cursor.execute.assert_called_with("""CREATE TABLE IF NOT EXISTS webshop_events (
            id INT PRIMARY KEY,
            event_type VARCHAR(50),
            timestamp TIMESTAMP,
            customer_id VARCHAR(255),
            user_agent TEXT,
            ip VARCHAR(50),
            query TEXT,
            page TEXT,
            referrer TEXT,
            session_id INT
        );""")
        
    @patch('psycopg2.connect')
    def test_table_drop(self, mock_connect):
        mock_conn = MagicMock()
        mock_cursor = mock_conn.cursor.return_value
        mock_connect.return_value = mock_conn

        # Call manage_database with a drop table SQL statement
        drop_sql = "DROP TABLE IF EXISTS webshop_events;"
        manage_database("DATABASE_KEY", "", drop_sql)
        
        # Ensure the drop table SQL was executed
        mock_cursor.execute.assert_called_with(drop_sql)

    @patch('psycopg2.connect', side_effect=psycopg2.OperationalError)
    def test_db_connection_failure(self, mock_connect):
        with self.assertRaises(psycopg2.OperationalError):
            create_db_table()

if __name__ == '__main__':
    unittest.main()
