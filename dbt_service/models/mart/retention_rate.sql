-- Monthly Retention Rate (MRR)

/**
- Monthly Retention Rate = (Number of users who logged in this month and also logged in last month) / (Number of users who logged in last month) * 100
- UC/UP * 100 UC---> Number of users who logged in this month and also logged in last month UP---> Number of users who logged in last month
**/
WITH USER_ACTIVITY AS (
    SELECT DISTINCT
        USER_ID,
        DATE_TRUNC('month', EVENT_TIME)::Date AS ACTIVITY_MONTH
    FROM
        {{ ref('stg_login_activity') }}
)

SELECT
    PREVIOUS.ACTIVITY_MONTH,
    ROUND(
        COUNT(DISTINCT CURR.USER_ID)::NUMBER
        / GREATEST(COUNT(DISTINCT PREVIOUS.USER_ID), 1), 2
    ) * 100.0 AS "Retention_Rate(%)"
FROM
    USER_ACTIVITY AS PREVIOUS
LEFT JOIN USER_ACTIVITY AS CURR
    ON
        PREVIOUS.USER_ID = CURR.USER_ID
        AND PREVIOUS.ACTIVITY_MONTH = (CURR.ACTIVITY_MONTH - INTERVAL '1 month')
GROUP BY PREVIOUS.ACTIVITY_MONTH
ORDER BY PREVIOUS.ACTIVITY_MONTH ASC
