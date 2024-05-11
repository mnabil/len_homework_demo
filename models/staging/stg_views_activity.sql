/**
Extract views activity from the platform events into a separate table for analysis
**/
SELECT 
    EVENT_TIME,
    EVENT_TYPE,
    ACTIVITY_DETAILS:product_id::VARCHAR as product_id,
    ACTIVITY_DETAILS:user_id::VARCHAR as user_id,
    ACTIVITY_DETAILS:logged_in::Boolean as logged_in,
    ACTIVITY_DETAILS:price::NUMBER(38,2) as price,
    ACTIVITY_DETAILS:referral::VARCHAR as referral,
    ACTIVITY_DETAILS:user_agent::VARCHAR as user_agent
FROM {{ source('platform_data', 'PLATFORM_EVENTS') }}
where EVENT_TYPE = 'view'