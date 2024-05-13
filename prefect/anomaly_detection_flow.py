from prefect import flow, task
from snowflake.connector.pandas_tools import write_pandas
from snowflake import connector
import pandas as pd
import json
from prefect.tasks.notifications import SlackTask
import argparse

# setting up the snowflake connector
ctx = connector.connect(
        user="user",
        password="pass",
        account="acc",
        warehouse="wh",
        database="DBT_DB",
        schema="DBT_SCHEMA",
        role="role",
        protocol='https')

# setting up the slack task
slack = SlackTask(webhook_secret="YOUR_CUSTOM_SECRET_NAME")

@task(log_prints=True)
def show_table_columns(table_name: str):
    """
    This function will execute a query to get the columns in a table
    """

    # Create a cursor object.
    curr = ctx.cursor()

    # Execute a statement that will generate a result set.
    curr.execute("show columns in table {table_name}".format(table_name = table_name))
    all_rows = curr.fetchall()

    field_names = [i[0] for i in curr.description]

    # load the data into a pandas dataframe
    df = pd.DataFrame(all_rows, columns=field_names)

    # convert the dataframe to a dictionary for easy access
    column_data = df.to_dict('records')

    timestamp_column = None # initialize the timestamp column
    numeric_columns = [] # initialize the numeric columns list to iterate over

    # iterate over the column data to get the column name and data type
    for col in column_data:
        col_data_type = json.loads(col['data_type'])
        col_name, data_type = col['column_name'], col_data_type["type"]
        print(f"Column Name: {col_name}, Data Type: {data_type}")
        if "TIMESTAMP" in data_type:
            timestamp_column = col_name

        # check if the data type is a number type
        if data_type in ["FIXED", "REAL", "FLOAT", "DOUBLE", "NUMERIC", "DECIMAL"]:
                numeric_columns.append(col_name)
    print(f"Timestamp Column: {timestamp_column}")
    print(f"Numeric Columns: {numeric_columns}")
    if numeric_columns:
        return timestamp_column, numeric_columns
    else:
        raise ValueError("No Numeric Columns Found in the Table")


@task(log_prints=True)
def check_if_model_with_name_exists(table_name: str):
    """
    Check if there's a snowflake cortex model with the same table name
    """
    # Create a cursor object.
    curr = ctx.cursor()
    query = "SHOW SNOWFLAKE.ML.ANOMALY_DETECTION like '%{table_name}%'"
    curr.execute(query.format(table_name = table_name))
    all_rows = curr.fetchall()

    if all_rows:
        print("Snowflake Cortex Model exists")
        # use it and go to step 4
        return True
    else:
        print("Snowflake Cortex Model does not exist")
        print("Creating a new Snowflake Anomaly Detection Model for table {table_name}".format(table_name = table_name))
        # create a new snowflake cortex model and train ( step 3)
        return False
    

@task(log_prints=True)
def use_model_if_exists(table_name: str, 
                        timestamp_column: str, 
                        numeric_columns: list, 
                        model_exists: bool):
    """
    use model on table and detect anomalies
    """
    # could iterate over the numeric columns and use them as the target columns
    # but for now, we'll just use the first numeric column
    curr = ctx.cursor()
    # call the snowflake DETECT_ANOMALIES model to detect anomalies on the second half of the data
    query = '''CALL {table_name}_model!DETECT_ANOMALIES( INPUT_DATA => SYSTEM$QUERY_REFERENCE('SELECT * FROM {table_name} WHERE {timestamp_column} > (SELECT TIMESTAMPADD(SECOND, DATEDIFF(SECOND, MIN({timestamp_column}), MAX({timestamp_column})) / 2,MIN({timestamp_column})) FROM {table_name}) order by {timestamp_column} asc;'), TIMESTAMP_COLNAME =>'{timestamp_column}', TARGET_COLNAME => '{target_column}');'''
    curr = curr.execute(query.format(table_name = table_name, timestamp_column = timestamp_column, target_column = numeric_columns[0]))
    all_rows = curr.fetchall()
    field_names = [i[0] for i in curr.description]

    # load the data into a pandas dataframe
    df = pd.DataFrame(all_rows, columns=field_names)
    column_data = df.to_dict('records')
    df.to_csv("anomalies.csv", index=False)
    if df[(df['IS_ANOMALY']==True)].empty:
        print("No anomalies detected")
        return False, None
        # exit flow
    else:
        print("Anomalies detected")
        return True, df
        # send slack alert with the anomalies step 4
     

@task(log_prints=True)
def train_model_if_not_exists(table_name: str, timestamp_column: str, numeric_columns: list):
    """
    Train the model on the table
    """
    curr = ctx.cursor()
    # create a new snowflake cortex model and train
    query = curr.execute('''CREATE OR REPLACE SNOWFLAKE.ML.ANOMALY_DETECTION {table_name}_model(INPUT_DATA => SYSTEM$QUERY_REFERENCE('SELECT * FROM {table_name} WHERE {timestamp_column} <= (SELECT TIMESTAMPADD(SECOND, DATEDIFF(SECOND, MIN({timestamp_column}), MAX({timestamp_column})) / 2, MIN({timestamp_column})) FROM {table_name}) order by {timestamp_column} asc;'), TIMESTAMP_COLNAME =>'{timestamp_column}', TARGET_COLNAME => '{target_column}');'''.format(table_name = table_name, timestamp_column = timestamp_column, target_column = numeric_columns[0]))
    print(query)
    curr = curr.execute(query.format(table_name = table_name, timestamp_column = timestamp_column, target_column = numeric_columns[0]))
    print("Model Trained Successfully on first half from table {table_name}".format(table_name = table_name))
    return True


@task(log_prints=True)
def send_slack_alert(anomalies_detected: bool, df=None):
    """
    Send slack alert if anomalies detected
    """
    if anomalies_detected:
        slack(message="Anomalies detected in the data. Please check the anomalies.csv file for more details")
        return True
    else:
        print("No anomalies detected")
        return False


@flow(log_prints=True)
def main_flow(table_name: str):
    action_1 = show_table_columns(table_name)
    cond1 = check_if_model_with_name_exists(table_name)
    if cond1 == True:
        action_2 = use_model_if_exists(table_name, action_1[0], action_1[1], cond1)
        action_3 = send_slack_alert(action_2[0], action_2[1])
    else:
        action_2 = train_model_if_not_exists(table_name, action_1[0], action_1[1])
        action_3 = use_model_if_exists(table_name, action_1[0], action_1[1], True)
        action_4 = send_slack_alert(action_3[0], action_3[1])

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--table-name", type=str, help="Name of the table", required=True)
    args = parser.parse_args()
    main_flow.serve(name="snowflake--flow",
                      tags=["snowflake"],
                      parameters=[args],
                      interval=60)