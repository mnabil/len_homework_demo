/**
Extract login activity from the platform events into a separate table
for analysis
**/
SELECT
    EVENT_TIME,
    EVENT_TYPE,
    ACTIVITY_DETAILS:user_id::VARCHAR AS USER_ID,
    ACTIVITY_DETAILS:ip_address::VARCHAR AS IP_ADDRESS,
    ACTIVITY_DETAILS:username::VARCHAR AS USERNAME,
    ACTIVITY_DETAILS:password::VARCHAR AS PASSWORD,
    ACTIVITY_DETAILS:user_agent::VARCHAR AS USER_AGENT
FROM {{ source('platform_data', 'PLATFORM_EVENTS') }}
WHERE EVENT_TYPE = 'login'
