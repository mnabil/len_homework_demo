# Prefect anomaly detection workflow

A [Prefect](https://docs.prefect.io/) workflow that runs on snowflake using snowflake warehosue compute, creats and runs a [SNOWFLAKE.ML.ANOMALY_DETECTION](https://docs.snowflake.com/en/user-guide/snowflake-cortex/ml-functions/anomaly-detection) model against a target table with target column as long as the table has a timestamp column

## Run Prefect local server

 `prefect server start`

## Deploy flow
 `python3 anomaly_detection_flow.py --table-name "table_name"`

## Workflow Explanation
    1) Flow takes a table name as a parameter 
    2) Check if a SNOWFLAKE.ML.ANOMALY_DETECTION is already created with model_[table_name]
    3) if model is created already; use against our target data
    4) if model is was not created, create and train model on half of our table records and use on the other half
    5) check if anomalies are detected
    6) if anomalies are detected then send to slack, otherwise exit flow.

