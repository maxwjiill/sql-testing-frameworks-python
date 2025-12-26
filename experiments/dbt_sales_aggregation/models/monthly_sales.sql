select
    date_trunc('month', sale_ts) as month,
    sum(qty * coalesce(price, 0) * (1 - coalesce(discount, 0))) as total
from {{ ref('sales') }}
where extract(year from sale_ts) = 2023
group by month
order by month
