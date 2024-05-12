/**
Extract login activity from the platform events into a separate table
for analysis
**/
SELECT
    EVENT_TIME,
    EVENT_TYPE,
    ACTIVITY_DETAILS:user_id::VARCHAR AS USER_ID,
    ACTIVITY_DETAILS:product_id::VARCHAR AS PRODUCT_ID,
    ACTIVITY_DETAILS:quantity::NUMBER AS QUANTITY,
    ACTIVITY_DETAILS:status::VARCHAR AS STATUS,
    ACTIVITY_DETAILS:total_amount::NUMBER(38, 2) AS TOTAL_AMOUNT
FROM {{ source('platform_data', 'PLATFORM_EVENTS') }}
WHERE EVENT_TYPE = 'purchase'
