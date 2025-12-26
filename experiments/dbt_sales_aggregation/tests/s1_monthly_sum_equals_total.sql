{{ config(tags=['S1']) }}

with monthly as (
    select
        date_trunc('month', sale_ts) as month,
        sum(qty * coalesce(price, 0) * (1 - coalesce(discount, 0))) as total
    from {{ ref('sales') }}
    group by month
),
overall as (
    select sum(qty * coalesce(price, 0) * (1 - coalesce(discount, 0))) as total
    from {{ ref('sales') }}
)
select
    (select sum(total) from monthly) as monthly_sum,
    (select total from overall) as overall_sum
where (select sum(total) from monthly) <> (select total from overall)
