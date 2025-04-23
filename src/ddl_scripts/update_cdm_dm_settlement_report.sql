insert into cdm.dm_settlement_report
(
    restaurant_id
    , restaurant_name
    , settlement_date
    , orders_count
    , orders_total_sum
    , orders_bonus_payment_sum
    , orders_bonus_granted_sum
    , order_processing_fee
    , restaurant_reward_sum
)
select
    dr.restaurant_id
    , dr.restaurant_name
    , dt.date as settlement_date
    , count(distinct fps.order_id) as orders_count
    , sum(fps.total_sum) as orders_total_sum
    , sum(fps.bonus_payment) as orders_bonus_payment_sum
    , sum(fps.bonus_grant) as orders_bonus_granted_sum
    , sum(fps.total_sum * 0.25) as order_processing_fee
    , sum(
        fps.total_sum - (fps.total_sum * 0.25) - fps.bonus_payment
    ) as restaurant_reward_sum
from dds.fct_product_sales as fps
inner join dds.dm_products as dp
    on
        fps.product_id = dp.id
        and dp.active_to = '5999-01-01 00:00:00'
inner join dds.dm_restaurants as dr
    on
        dp.restaurant_id = dr.id
        and dr.active_to = '5999-01-01 00:00:00'
inner join dds.dm_orders as do2
    on fps.order_id = do2.id
inner join dds.dm_timestamps as dt
    on do2.timestamp_id = dt.id
where do2.order_status = 'CLOSED'
group by
    dr.restaurant_id
    , dr.restaurant_name
    , dt.date
on conflict (restaurant_id, settlement_date) do update
set
restaurant_name = excluded.restaurant_name
, orders_count = excluded.orders_count
, orders_total_sum = excluded.orders_total_sum
, orders_bonus_payment_sum = excluded.orders_bonus_payment_sum
, orders_bonus_granted_sum = excluded.orders_bonus_granted_sum
, order_processing_fee = excluded.order_processing_fee
, restaurant_reward_sum = excluded.restaurant_reward_sum;
