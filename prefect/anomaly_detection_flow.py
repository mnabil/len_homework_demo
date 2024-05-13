from prefect import flow
from snowflake.connector.pandas_tools import write_pandas
from snowflake import connector
import pandas as pd
import json

# setting up the snowflake connector
ctx = connector.connect(
        user="supermnabil",
        password="teqqat-qujsor-gePtu6",
        account="qsevret-brb72688",
        warehouse="COMPUTE_WH",
        database="DBT_DB",
        schema="DBT_SCHEMA",
        protocol='https')

@flow(log_prints=True)
def first_step():
    """
    This function will execute a query to get the columns in a table
    """

    # Create a cursor object.
    curr = ctx.cursor()

    # Execute a statement that will generate a result set.
    try: 
        # query needs to be double slash escaped if it contains a single quote
        curr.execute("show columns in table total_sales")
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

    finally:
        curr.close()
        ctx.close()

@flow(log_prints=True)
def second_step():
    """
    Check if there's a snowflake cortex model with the same table name
    """
    # Create a cursor object.
    curr = ctx.cursor()
    query = "SHOW SNOWFLAKE.ML.ANOMALY_DETECTION like \\'%{table_name}%\\'"
    try:
        curr.execute("show columns in table total_sales")
        all_rows = curr.fetchall()
    
        if curr is not None:
            print("Snowflake Cortex Model exists")
            # use it and go to step 4
            return True
        else:
            print("Snowflake Cortex Model does not exist")
            print("Creating a new Snowflake Anomaly Detection Model for table {table_name}".format(table_name = table_name))
            # create a new snowflake cortex model and train ( step 3)
            return False
    finally:
        curr.close()
        ctx.close()

@flow(log_prints=True)
def third_step(table_name: str):
    """
    use model on table and detect anomalies
    """
    # Create a cursor object.
    try:
        curr = ctx.cursor()
        query = curr.execute('''CALL basic_model!DETECT_ANOMALIES( INPUT_DATA => SYSTEM$QUERY_REFERENCE('SELECT day, total_sales FROM dbt_db.dbt_schema.total_sales WHERE date(day) > date(\\'2023-12-31\\')'), TIMESTAMP_COLNAME =>'day', TARGET_COLNAME => 'total_sales');''')
        all_rows = curr.fetchall()
        field_names = [i[0] for i in curr.description]

        # load the data into a pandas dataframe
        df = pd.DataFrame(all_rows, columns=field_names)
        column_data = df.to_dict('records')
        df.to_csv("anomalies.csv", index=False)
        if df[(df['IS_ANOMALY']==True)].empty:
            print("No anomalies detected")
            return False
            # exit flow
        else:
            print("Anomalies detected")
            return True
            # send slack alert with the anomalies step 4

    finally:
        curr.close()
        ctx.close()
     
@flow(log_prints=True)
def fourth_step():
    """
    Train the model on the table
    """

if __name__ == "__main__":
    third_step.serve(name="snowflake--flow",
                      tags=["snowflake"],
                      interval=60)


# curr.execute('''CALL basic_model!DETECT_ANOMALIES( INPUT_DATA => SYSTEM$QUERY_REFERENCE('SELECT day, total_sales FROM dbt_db.dbt_schema.total_sales WHERE date(day) > date(\\'2023-12-31\\')'), TIMESTAMP_COLNAME =>'day', TARGET_COLNAME => 'total_sales');''')

# with open("columns.json", "w") as f:
#     f.write(df.to_json(orient="records", lines=True))