with payments_per_plan_credit_card_type as (

    select * from {{ ref('payments_per_plan_credit_card_type') }}

),

credit_card_usage_per_country as (

    select * from {{ ref('credit_card_usage_per_country') }}

),
    analysis_mastercard_country_portugal as (

        select country.country, pays.plan, pays.total_price
        from payments_per_plan_credit_card_type as pays
        inner join credit_card_usage_per_country as country
        on pays.credit_card_type = country.credit_card_type
        where pays.credit_card_type = 'mastercard' and country = 'Portugal'
        order by pays.plan asc, pays.total_price desc
    )

select *
from analysis_mastercard_country_portugal