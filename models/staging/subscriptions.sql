{{ config(materialized='view') }}

with src_subscriptions as
(
    select *
    from workshop.mssql_subscriptions
)
select sb.user_id,
       sb.plan,
       sb.status,
       sb.payment_term,
       sb.payment_method,
       sb.subscription_term
from src_subscriptions as sb