/** Joining all queries together to generate the final report */
SELECT *
FROM
    {{ ref('monthly_active_users') }}
INNER JOIN
    {{ ref('retention_rate') }}
    USING
        (activity_month)
INNER JOIN
    {{ ref('avg_monthly_active_users') }}
    USING
        (activity_month)
INNER JOIN
    {{ ref('popular_products_by_views') }}
    USING
        (activity_month)
