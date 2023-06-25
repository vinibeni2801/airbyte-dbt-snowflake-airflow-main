{{ config(materialized='table') }}

with cleansed_subscriptions as
(
    select * from {{ ref('subscriptions') }}
)
select
    sb.user_id,
    sb.plan,
    case
        when plan in ('Business', 'Diamond', 'Gold', 'Platinum', 'Premium') then 'High'
        when plan in ('Bronze', 'Essential', 'Professional', 'Silver', 'Standard') then 'Normal'
        else 'Low'
    end as subscription_importance,
    sb.status,
    sb.payment_term,
    sb.payment_method,
    sb.subscription_term
from cleansed_subscriptions as sb