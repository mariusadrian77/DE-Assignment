import psycopg2
import pandas as pd
import os
from urllib.parse import urlparse
from dotenv import load_dotenv


# SQL query to fetch the top 100 rows
def get_top_100_rows_query():
    return """
    SELECT *
    FROM webshop_events
    LIMIT 100;
    """

# Function to fetch data from the database
def fetch_data_from_db(query):
    try:
        load_dotenv()
        db_url = os.getenv('DATABASE_KEY')
        url = urlparse(db_url)

        conn_params = {
                'dbname': url.path[1:],    
                'user': url.username,       
                'password': url.password,   
                'host': url.hostname,       
                'port': url.port           
            }
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()
        cursor.execute(query)
        
        # Fetch all results from the executed query
        result = cursor.fetchall()

        # Get the column names from the cursor description
        colnames = [desc[0] for desc in cursor.description]

        cursor.close()
        conn.close()

        # Return the result as a pandas DataFrame
        return pd.DataFrame(result, columns=colnames)
    except Exception as e:
        print(f"Error: {e}")
        return None

def main():
    # Fetch the top 100 rows
    query = get_top_100_rows_query()
    top_100_df = fetch_data_from_db(query)

    if top_100_df is not None:
        print("Top 100 Rows:")
        print(top_100_df.head())
    else:
        print("Error fetching the top 100 rows.")

if __name__ == "__main__":
    main()
