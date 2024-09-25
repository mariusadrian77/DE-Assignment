# Project Instructions

This readme file contains instructions on how to use the designed solution out-of-the-box.

## Steps to Get Started

### 1. Access the PostgreSQL Database
Use the following invite link to get access to the PostgreSQL database hosted on Railway:
[Railway invite link](https://railway.app/invite/Gya37GNkNZg)

### 2. Create the Database Schema and Table
Run the `db_creation.py` Python script to create the database schema and table. You might need to create a `.env` file containing the connection string with the database credentials.

> **Note:** For the sake of the project, I have made the `.env` file available (although this is not best practice). Alternatively, you can find the database credentials by following these steps:
- Navigate to the deployment.
- Click the PostgreSQL database.
- A window will open on the right side of the screen.
- Click the "Data" section where you can see the table(s).
- Above the table, there is a panel with "Connect to the database Postgres".
- Press the "Connect" button on the right side of the panel.
- A new window will open, then press the "Public Network" section.
- Click the copy button next to the "Connection URL" section.
- Paste the connection URL into your `.env` file as:
  ```ini
  DATABASE_KEY = '{connection_url}'

### 3. Run the `db_insert.py` Python Script
- This script will fetch and clean the dataset from the online JSON file.
- It will then sessionize the data.
- Lastly, it will copy the data to the database.

### 4. Run the `app.py` Python Script
- This script will connect to the PostgreSQL database, where the curated dataset was inserted, and fetch it for further analysis. It will calculate two metrics:
  1. The median number of sessions someone had before they made a purchase.
  2. The median session duration someone had before the first session in which they made a purchase.
  
- The web application will be deployed on port `5000` and can be accessed via the web browser at:
[Web API](http://127.0.0.1:5000/)


- The previously calculated metrics can be viewed at:
[Metrics](http://127.0.0.1:5000/metrics/orders)


### Troubleshooting
If any problems arise during the database creation step, you can modify line 59 of `db_creation.py` to delete the table and retry the steps.

To do this, uncomment the "DROP TABLE" statement and add it between the quotation marks, for example:

```python
drop_table_sql = "DROP TABLE IF EXISTS webshop_events;"  # Optional table drop logic
