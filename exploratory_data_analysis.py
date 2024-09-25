import requests
import json
from tqdm import tqdm
import pandas as pd
import matplotlib.pyplot as plt
from datetime import timedelta


# Function to fetch and parse the JSON data into a pandas DataFrame
def fetch_and_transform_data():
    
    # Fetch the data from the URL
    url = 'https://storage.googleapis.com/xcc-de-assessment/events.json'
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the data line by line as a JSON object
        events = []
        for line in response.text.splitlines():
            try:
                event = json.loads(line)
                events.append(event)
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")

        # Filter events that have a non-null customer-id and a non-null IP address
        filtered_events = [event for event in events if event['event']['customer-id'] is not None and event['event']['ip'] is not None]

        # Create a pandas DataFrame from the filtered events
        df = pd.DataFrame([{
            'id': event['id'],
            'type': event['type'],
            'timestamp': event['event']['timestamp'],
            'customer_id': event['event']['customer-id'],
            'user_agent': event['event']['user-agent'],
            'ip': event['event']['ip'],
            'query': event['event'].get('query', None),
            'page': event['event'].get('page', None),
            'referrer': event['event'].get('referrer', None)
        } for event in filtered_events])

        # Convert the timestamp to pandas datetime format
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        return df
    else:
        print(f"Failed to fetch data. Status code: {response.status_code}")
        return None

# Function to group events by customer into sessions
def sessionize_data(df, session_timeout):
    # Sort the events by customer_id and timestamp
    df = df.sort_values(by=['customer_id', 'timestamp'])

    # Initialize a list to hold session ids
    session_ids = []

    # Time threshold (in minutes) for session timeout
    session_threshold = timedelta(minutes=session_timeout)

    # Initialize variables to track the previous row
    previous_customer_id = None
    previous_timestamp = None
    current_session_id = 1  # Start session ID at 1 for each customer

    # Loop through each event and assign a session ID
    for index, row in df.iterrows():
        if row['customer_id'] != previous_customer_id:
            current_session_id = 1  # Reset session ID for new customer
        # If this is the same customer but the time difference is greater than the threshold, increment session ID
        elif previous_timestamp is not None and (row['timestamp'] - previous_timestamp) > session_threshold:
            current_session_id += 1
        
        # Append the current session ID
        session_ids.append(current_session_id)

        # Update the previous customer and timestamp
        previous_customer_id = row['customer_id']
        previous_timestamp = row['timestamp']

    # Add session IDs to the DataFrame
    df['session_id'] = session_ids

    return df

def analyze_time_differences(df):
    # Sort by customer_id and timestamp
    df = df.sort_values(by=['customer_id', 'timestamp'])
    
    # Calculate time differences for each customer
    df['time_diff'] = df.groupby('customer_id')['timestamp'].diff()
    
    # Drop the NaN values for the first event of each customer
    time_diffs = df['time_diff'].dropna()

    # Plot the distribution of time differences
    time_diffs_in_minutes = time_diffs.dt.total_seconds() / 60
    plt.figure(figsize=(14, 6))
    time_diffs_in_minutes.hist(bins=120, range=(0, 40))  
    
    # Show the plot
    plt.xlabel('Time Difference (Minutes)')
    plt.ylabel('Frequency')
    plt.title('Distribution of Time Differences Between Events')
    plt.xticks(range(0, 35, 1), rotation = 45)  # X-axis ticks every 1 minute for granularity
    plt.grid(True)
    plt.savefig('/workspaces/de-assessment-mariusadrian77/plots/time_difference_plot.png')


def analyze_session_durations(df,session_timeout):
    # Group by customer_id and session_id to calculate session durations
    session_durations = df.groupby(['customer_id', 'session_id']).agg(
        session_start=('timestamp', 'min'),
        session_end=('timestamp', 'max')
    )
    session_durations['session_length'] = (session_durations['session_end'] - session_durations['session_start']).dt.total_seconds() / 60  # Minutes

    # Plot session duration distribution
    plt.figure(figsize=(14, 6))
    session_durations['session_length'].hist(bins=120, range=(0, 40))
    
    # Show the plot
    plt.xlabel(f'Session Duration {session_timeout} (Minutes)')
    plt.ylabel('Frequency')
    plt.title('Distribution of Session Durations')
    plt.xticks(range(0, 45, 1), rotation = 45)  # X-axis ticks every 1 minute for granularity
    plt.grid(True)
    plt.savefig(f'/workspaces/de-assessment-mariusadrian77/plots/session_durations/session_duration_plot_{session_timeout}.png')


def session_timeout_analysis(df, min_timeout=5, max_timeout=10):
    # Loop through session timeouts from min_timeout to max_timeout
    for timeout in tqdm(range(min_timeout, max_timeout + 1), desc="Session Timeout Analysis"):

        sessionized_df = sessionize_data(df, session_timeout=timeout)
        print(f"Analyzing session durations with a timeout of {timeout} minutes...")
        
        # Analyze session durations for the current sessionized DataFrame
        analyze_session_durations(sessionized_df, session_timeout= timeout)




def main():
    # Fetch and transform the data into a pandas DataFrame
    df = fetch_and_transform_data()

    # Call the function to analyze time differences
    analyze_time_differences(df)

    if df is not None:
        session_timeout_analysis(df, min_timeout=5, max_timeout=10)


if __name__ == "__main__":
    main()
