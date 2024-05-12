/** 
    * Monthly Active Users
    * 
    * This model calculates the number of unique users who logged in each month.
    * 
    * The model is built on top of the `stg_login_activity` model
**/
SELECT
    DATE_TRUNC('month', event_time) ::Date AS activity_month,
    COUNT(DISTINCT user_id) AS mau
FROM
    {{ ref('stg_login_activity') }}
GROUP BY
    activity_month
ORDER BY
    activity_month asc