{{ config(tags=['S6']) }}

with ranked as (
    select
        s.customer_id,
        s.sale_id,
        s.qty * coalesce(s.price, 0) * (1 - coalesce(s.discount, 0)) as revenue,
        row_number() over (
            partition by s.customer_id
            order by s.qty * coalesce(s.price, 0) * (1 - coalesce(s.discount, 0)) desc,
                s.sale_id asc
        ) as rn
    from {{ ref('sales') }} s
),
topn as (
    select *
    from ranked
    where rn <= 3
),
count_violations as (
    select customer_id
    from topn
    group by customer_id
    having count(*) > 3
),
order_violations as (
    select distinct r1.customer_id
    from ranked r1
    join ranked r2
        on r1.customer_id = r2.customer_id
        and r1.revenue = r2.revenue
        and r1.sale_id > r2.sale_id
        and r1.rn < r2.rn
)
select 'count_violation' as failure, customer_id from count_violations
union all
select 'order_violation' as failure, customer_id from order_violations
