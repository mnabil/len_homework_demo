/**
Average MAU = Sum of DAU for the month /Number of days in the month
**/
WITH monthly_activities AS (
    SELECT 
        DATE_TRUNC('month', event_time) AS month,
        user_id,
        COUNT(*) AS activity_count
    FROM 
        {{ ref('stg_login_activity') }}
    GROUP BY 
        DATE_TRUNC('month', event_time),
        user_id
),
monthly_active_users AS (
    SELECT
        DATE_TRUNC('month', event_time) AS month,
        COUNT(DISTINCT user_id) AS active_users_count
    FROM
        {{ ref('stg_login_activity') }}
    GROUP BY
        DATE_TRUNC('month', event_time)
)
SELECT
    ma.month as activity_month,
    AVG(ma.activity_count) AS avg_activities_per_user
FROM
    monthly_activities ma
JOIN
    monthly_active_users mau ON ma.month = mau.month
GROUP BY
    ma.month
ORDER BY
    ma.month