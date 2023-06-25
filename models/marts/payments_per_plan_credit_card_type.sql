with cleansed_payments as (

    select * from {{ ref('cleansed_payments') }}

),

cleansed_subscriptions as (

    select * from {{ ref('cleansed_subscriptions') }}

),
    payments_per_plan_credit_card_type as (

        select
            subs.plan,
            pmts.credit_card_type,
            sum(pmts.subscription_price) as total_price
        from cleansed_payments as pmts
        inner join cleansed_subscriptions as subs
            on pmts.user_id = subs.user_id
        group by subs.plan, pmts.credit_card_type
        order by subs.plan asc, total_price desc
    )

select *
from payments_per_plan_credit_card_type