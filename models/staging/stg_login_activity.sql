/**
Extract login activity from the platform events into a separate table
for analysis
**/
SELECT 
    EVENT_TIME,
    EVENT_TYPE,
    ACTIVITY_DETAILS:user_id::VARCHAR as user_id,
    ACTIVITY_DETAILS:ip_address::VARCHAR as ip_address,
    ACTIVITY_DETAILS:username::VARCHAR as username,
    ACTIVITY_DETAILS:password::VARCHAR as password,
    ACTIVITY_DETAILS:user_agent::VARCHAR as user_agent
FROM {{ source('platform_data', 'PLATFORM_EVENTS') }}
where EVENT_TYPE = 'login'