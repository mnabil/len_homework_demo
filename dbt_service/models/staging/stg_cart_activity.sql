/**
Extract cart activity from the platform events into a separate table for analysis
**/
SELECT 
    EVENT_TIME,
    EVENT_TYPE,
    ACTIVITY_DETAILS:user_id::VARCHAR as user_id,
    ACTIVITY_DETAILS:product_id::VARCHAR as product_id,
    ACTIVITY_DETAILS:quantity::NUMBER as quantity
FROM {{ source('platform_data', 'PLATFORM_EVENTS') }}
where EVENT_TYPE = 'add_to_cart'