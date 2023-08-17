CREATE OR REPLACE VIEW analysis.orders AS 
select
    o.order_id,
    order_ts,
    user_id,
    bonus_payment,
    payment,
    cost,
    bonus_grant,
    os.status_id status
from
    production.orders o
left join (
    select
        order_id,
        status_id,
        row_number() over (partition by order_id order by dttm desc) row_n
    from
        production.OrderStatusLog) os 
on o.order_id = os.order_id
where row_n = 1;
