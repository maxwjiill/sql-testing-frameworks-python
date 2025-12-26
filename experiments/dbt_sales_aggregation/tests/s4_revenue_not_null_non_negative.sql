{{ config(tags=['S4']) }}

with revenue as (
    select qty * coalesce(price, 0) * (1 - coalesce(discount, 0)) as revenue
    from {{ ref('sales') }}
),
checks as (
    select
        count(*) filter (where revenue is null) as null_count,
        sum(revenue) as total_revenue
    from revenue
)
select *
from checks
where null_count > 0 or total_revenue < 0
