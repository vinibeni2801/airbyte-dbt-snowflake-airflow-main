{{ config(materialized='table') }}

with cleansed_payments as
(
    select * from {{ ref('payments') }}
)
select
    ps.user_id,
    ps.city,
    ps.race,
    ps.country,
    ps.currency,
    ps.credit_card_type,
    TO_DECIMAL(REPLACE(ps.subscription_price, '$'),10,2) as subscription_price
from cleansed_payments as ps