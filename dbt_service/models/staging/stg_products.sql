/*
a dimentional model for products and its attributes
*/
select *
from {{ source('platform_data', 'PRODUCTS') }}