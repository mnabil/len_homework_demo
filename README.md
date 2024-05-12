# Lennar Homework
# Info about project

Project consists of 3 points listed in Lennar Challenge

Requires `Python 3.11+`

# Setup Python Virtal Environment

```bash
python3 venv -m myenv && source myenv/bin/activate && pip3 install -r requirements.txt
```

# Synthesize
Three scripts to generate synthetic data to transform using our dbt_service (assumed this data is already on snowflake)

# dbt_service
Our dbt service which will extract our core data (presumed to be in snowflake already) then load it into snowflake tables under stg_tableName. referencing our staging models inside marts models to provide our reports in materialized views.

Check for related README files

# airflow dags
Our airflow project which takes as a parameter to check for data anomalies.

Check for related README files