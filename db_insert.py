import requests
import json
import os
import io
import pandas as pd
import psycopg2
from urllib.parse import urlparse
from dotenv import load_dotenv
from exploratory_data_analysis import fetch_and_transform_data, sessionize_data

# Helper function to handle database connections and bulk insert using COPY
def copy_dataframe_to_db(db_url_key, dataframe, table_name):
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

    # Use an in-memory string buffer to hold CSV data
    csv_buffer = io.StringIO()
    # Convert the dataframe to CSV format in the buffer
    dataframe.to_csv(csv_buffer, index=False, header=False)
    csv_buffer.seek(0)  # Move the buffer cursor to the beginning

    # Perform the COPY operation
    try:
        copy_sql = f"""
        COPY {table_name} (id, event_type, timestamp, customer_id, user_agent, ip, query, url, page, referrer, session_id)
        FROM stdin WITH CSV DELIMITER ',' NULL 'None' ESCAPE '\\';
        """
        cursor.copy_expert(copy_sql, csv_buffer)
        conn.commit()
        print(f"Data copied successfully to {table_name}!")
    except Exception as e:
        print(f"Error copying data: {e}")
        conn.rollback()
    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()


def main():
    # Fetch and transform the data 
    df = fetch_and_transform_data()

    # Insert the df into the database
    if df is not None:
        sessionized_df = sessionize_data(df, session_timeout=8)
        copy_dataframe_to_db("DATABASE_KEY", sessionized_df, "webshop_events")


if __name__ == "__main__":
    main()
