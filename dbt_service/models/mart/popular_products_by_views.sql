/**
Getting the top 3 most viewed products for each month
steps:
1. Get the unique viewers for each product in each month
2. Rank the products based on the unique viewers , using dense_rank function to handle ties
3. Select the top 3 products for each month based on the rank , splitting the products and unique viewers into separate columns
*/

WITH product_views AS (
    SELECT 
        DATE_TRUNC('month', event_time) AS activity_month,
        product_id,
        COUNT(DISTINCT user_id) AS unique_viewers
    FROM 
        {{ ref('stg_views_activity') }}
    GROUP BY 
        DATE_TRUNC('month', event_time),
        product_id
),
ranked_products AS (
    SELECT 
        activity_month,
        product_id,
        unique_viewers,
        DENSE_RANK() OVER (PARTITION BY activity_month ORDER BY unique_viewers DESC) AS rank
    FROM 
        product_views
)
SELECT 
    activity_month,
    MAX(CASE WHEN rank = 1 THEN product_id END) AS top_1_product,
    MAX(CASE WHEN rank = 1 THEN unique_viewers END) AS top_1_view_count,
    MAX(CASE WHEN rank = 2 THEN product_id END) AS top_2_product,
    MAX(CASE WHEN rank = 2 THEN unique_viewers END) AS top_2_view_count,
    MAX(CASE WHEN rank = 3 THEN product_id END) AS top_3_product,
    MAX(CASE WHEN rank = 3 THEN unique_viewers END) AS top_3_view_count
FROM 
    ranked_products
GROUP BY 
    activity_month
ORDER BY 
    activity_month