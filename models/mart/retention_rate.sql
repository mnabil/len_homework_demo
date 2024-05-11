-- Monthly Retention Rate (MRR)

/**
- Monthly Retention Rate = (Number of users who logged in this month and also logged in last month) / (Number of users who logged in last month) * 100
- UC/UP * 100 UC---> Number of users who logged in this month and also logged in last month UP---> Number of users who logged in last month
**/
WITH USER_ACTIVITY as (
    SELECT DISTINCT 
        user_id,
        DATE_TRUNC('month', event_time) ::Date AS activity_month
    FROM
        {{ ref('stg_login_activity') }}
)
SELECT previous.activity_month,
    ROUND(
        COUNT(DISTINCT curr.user_id):: NUMBER /
        GREATEST(COUNT(DISTINCT previous.user_id), 1), 2) * 100.0 AS "Retention_Rate(%)"
FROM
USER_ACTIVITY as previous
left join USER_ACTIVITY as curr
on previous.user_id = curr.user_id
and previous.activity_month = (curr.activity_month - interval '1 month')
group by previous.activity_month
order by previous.activity_month asc