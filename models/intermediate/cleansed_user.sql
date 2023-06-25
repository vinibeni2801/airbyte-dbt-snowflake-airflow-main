{{ config(materialized='table') }}

with cleansed_user as (

    select * from {{ ref('user') }}

)
select user_id as user_id,
       username as user_name, 
       gender as gender,
       json_extract_path_text(cu.employment, 'title') as title,
       json_extract_path_text(cu.subscription, 'payment_method') as payment_method
from cleansed_user as cu