# dbt_service
Our dbt service which will extract our core data (presumed to be in snowflake already) then load it into snowflake tables under stg_tableName. referencing our staging models inside marts models to provide our reports in materialized views.

# connect to snowflake
configure your `~/.dbt/profiles.yaml` to connect to snowflake for your `dev` profile

# models
MART : containing our aggregations & reports
 
STAGING : containing our staging models which we extract from snowflake core database

# dbt docs
 `dbt docs generate`

# dbt compile
 `dbt compile`

# run tests
 `dbt test`

# run our models
 `dbt run`

# making changes
  dbt_service uses pre-commit as well as github workflows to enforce coding standards & linting using `SQLfluff` & making sure tests are run using `dbt test`