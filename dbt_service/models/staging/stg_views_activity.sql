/**
Extract views activity from the platform events into a separate table for analysis
**/
SELECT
    EVENT_TIME,
    EVENT_TYPE,
    ACTIVITY_DETAILS:product_id::VARCHAR AS PRODUCT_ID,
    ACTIVITY_DETAILS:user_id::VARCHAR AS USER_ID,
    ACTIVITY_DETAILS:logged_in::Boolean AS LOGGED_IN,
    ACTIVITY_DETAILS:price::NUMBER(38, 2) AS PRICE,
    ACTIVITY_DETAILS:referral::VARCHAR AS REFERRAL,
    ACTIVITY_DETAILS:user_agent::VARCHAR AS USER_AGENT
FROM {{ source('platform_data', 'PLATFORM_EVENTS') }}
WHERE EVENT_TYPE = 'view'
