/**
Extract login activity from the platform events into a separate table
for analysis
**/
SELECT 
    EVENT_TIME,
    EVENT_TYPE,
    ACTIVITY_DETAILS:user_id::VARCHAR as user_id,
    ACTIVITY_DETAILS:product_id::VARCHAR as product_id,
    ACTIVITY_DETAILS:quantity::NUMBER as quantity,
    ACTIVITY_DETAILS:status::VARCHAR as status,
    ACTIVITY_DETAILS:total_amount::NUMBER(38,2) as total_amount
FROM {{ source('platform_data', 'PLATFORM_EVENTS') }}
where EVENT_TYPE = 'purchase'