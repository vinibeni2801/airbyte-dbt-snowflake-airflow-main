with cleansed_user as 
(
    select * from {{ ref('cleansed_user') }}
),

cleansed_payments as (

    select * from {{ ref('cleansed_payments') }}

),

credit_card_usage_per_country as (

        select ps.country, ps.currency, ps.credit_card_type, count(*) as amount
        from cleansed_user as pu
        inner join cleansed_payments as ps using(user_id)
        group by ps.country, ps.currency, ps.credit_card_type
    )

select *
from credit_card_usage_per_country
