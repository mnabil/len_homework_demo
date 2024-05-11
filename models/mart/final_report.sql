/** Joining all queries together to generate the final report */
SELECT
  *
FROM
    {{ ref('monthly_active_users') }}
JOIN
    {{ ref('retention_rate') }}
USING 
    (activity_month)
join 
    {{ ref('avg_monthly_active_users') }}
USING 
    (activity_month)
join 
    {{ ref('popular_products_by_views') }}
using 
    (activity_month)