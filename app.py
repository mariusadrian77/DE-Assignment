import psycopg2
import os
from flask import Flask, jsonify
from urllib.parse import urlparse
from dotenv import load_dotenv

app = Flask(__name__)


# Helper function to execute a query and fetch the first result
def fetch_single_value_from_db(query):
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
        result = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        return result
    except Exception as e:
        return str(e)

# SQL query for median visits before order (using webshop_events table)
def get_median_visits_before_order_query():
    return """
    WITH customer_sessions AS (
        SELECT 
            customer_id,
            session_id,
            MIN(timestamp) AS session_start_time
        FROM webshop_events
        GROUP BY customer_id, session_id
    ),
    first_purchase AS (
        SELECT 
            customer_id,
            MIN(timestamp) AS first_purchase_time
        FROM webshop_events
        WHERE event_type = 'placed_order'
        GROUP BY customer_id
    ),
    sessions_before_purchase AS (
        SELECT 
            cs.customer_id,
            COUNT(cs.session_id) AS session_count
        FROM customer_sessions cs
        JOIN first_purchase fp 
            ON cs.customer_id = fp.customer_id
        WHERE cs.session_start_time < fp.first_purchase_time
        GROUP BY cs.customer_id
    )
    SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY session_count) AS median_sessions_before_purchase
    FROM sessions_before_purchase;
    """

# SQL query for median session duration before order (using webshop_events table)
def get_median_session_duration_before_order_query():
    return """
    WITH session_durations AS (
        SELECT 
            customer_id,
            session_id,
            MIN(timestamp) AS session_start_time,
            MAX(timestamp) AS session_end_time,
            EXTRACT(EPOCH FROM (MAX(timestamp) - MIN(timestamp))) / 60 AS session_duration_minutes
        FROM webshop_events
        GROUP BY customer_id, session_id
    ),
    first_purchase AS (
        SELECT 
            customer_id,
            MIN(timestamp) AS first_purchase_time
        FROM webshop_events
        WHERE event_type = 'placed_order'
        GROUP BY customer_id
    ),
    durations_before_purchase AS (
        SELECT 
            sd.customer_id,
            sd.session_duration_minutes
        FROM session_durations sd
        JOIN first_purchase fp 
            ON sd.customer_id = fp.customer_id
        WHERE sd.session_start_time < fp.first_purchase_time
    )
    SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY session_duration_minutes) AS median_session_duration_before_purchase
    FROM durations_before_purchase;
    """

# Endpoint to get the metrics for orders
@app.route('/metrics/orders', methods=['GET'])
def get_order_metrics():
    # Fetch the median visits before order
    median_visits_query = get_median_visits_before_order_query()
    median_visits_before_order = fetch_single_value_from_db(median_visits_query)
    
    # Fetch the median session duration before order
    median_duration_query = get_median_session_duration_before_order_query()
    median_session_duration_before_order = fetch_single_value_from_db(median_duration_query)
    
    # Return the response in the required format
    return jsonify({
        "median_visits_before_order": median_visits_before_order,
        "median_session_duration_minutes_before_order": median_session_duration_before_order
    })

# Root endpoint
@app.route('/')
def home():
    return "Session Analysis API is running."

if __name__ == '__main__':
    app.run(debug=True)
