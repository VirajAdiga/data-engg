-- Creating new table on daily revenue from orders
create table order_product_revenue as
select o.order_date, round(sum(oi.order_item_subtotal)::numeric, 2) as order_revenue
from orders o
join order_items oi
on o.order_id = oi.order_item_order_id
where o.order_status in ('COMPLETE', 'CLOSED')
group by o.order_date;


-- Monthly revenue calculation
select to_char(order_date::timestamp, 'yyyy-MM') as order_month, sum(order_revenue) as order_revenue
from order_product_revenue
group by order_month;


-- Aggregation with raw data using OVER and PARTITION BY
select to_char(order_date::timestamp, 'yyyy-MM') as order_month,
    order_date,
    sum(order_revenue) over (partition by to_char(order_date::timestamp, 'yyyy-MM')) as monthly_order_revenue
from order_product_revenue
order by order_date;
