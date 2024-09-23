import psycopg2
from urllib.parse import urlparse
from dotenv import load_dotenv
import os

# Helper function to handle database connections and execute queries
def manage_database(db_url_key, create_table_sql, drop_table_sql=None):
    # Load environment variables from the .env file
    load_dotenv()

    # Connect to the database
    db_url = os.getenv(db_url_key)
    url = urlparse(db_url)
    conn_params = {
        'dbname': url.path[1:],   
        'user': url.username,       
        'password': url.password,   
        'host': url.hostname,       
        'port': url.port            
    }

    conn = psycopg2.connect(**conn_params)
    print(f"Connection to {db_url_key} established successfully!")

    # Create a cursor object to execute SQL queries
    cursor = conn.cursor()
    cursor.execute(create_table_sql)

    # Save changes
    conn.commit()
    print("Table created successfully!")

    # Drop the table if necessary
    if drop_table_sql:
        cursor.execute(drop_table_sql)
        conn.commit()
        print("Table dropped successfully!")

    # Close the cursor and connection
    cursor.close()
    conn.close()

# Function to create a database table for the JSON event data
def create_db_table():
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS webshop_events (
            id INT PRIMARY KEY,
            event_type VARCHAR(50),
            timestamp TIMESTAMP,
            customer_id VARCHAR(255),
            user_agent TEXT,
            ip VARCHAR(50),
            query TEXT,
            url TEXT,
            page TEXT,
            referrer TEXT,
            session_id INT
        );
    """
    drop_table_sql = "" # DROP TABLE IF EXISTS webshop_events; Optional table drop logic
    manage_database("DATABASE_KEY", create_table_sql, drop_table_sql)


if __name__ == "__main__":
    create_db_table()