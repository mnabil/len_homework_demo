/*
a dimentional model for users and its attributes
*/
select *
from {{ source('platform_data', 'USERS') }}
