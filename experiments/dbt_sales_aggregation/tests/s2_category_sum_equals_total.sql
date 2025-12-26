{{ config(tags=['S2']) }}

with by_category as (
    select
        p.category,
        sum(s.qty * coalesce(s.price, 0) * (1 - coalesce(s.discount, 0))) as total
    from {{ ref('sales') }} s
    join {{ ref('products') }} p on p.product_id = s.product_id
    group by p.category
),
overall as (
    select sum(qty * coalesce(price, 0) * (1 - coalesce(discount, 0))) as total
    from {{ ref('sales') }}
)
select
    (select sum(total) from by_category) as category_sum,
    (select total from overall) as overall_sum
where (select sum(total) from by_category) <> (select total from overall)
