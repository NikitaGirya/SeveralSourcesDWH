insert into cdm.dm_courier_ledger
(
    courier_id
    , courier_name
    , settlement_year
    , settlement_month
    , orders_count
    , orders_total_sum
    , rate_avg
    , order_processing_fee
    , courier_order_sum
    , courier_tips_sum
    , courier_reward_sum
)
with order_agg as (
    select
        order_id
        , delivery_id
        , sum(total_sum) as total_order_sum
    from dds.fct_product_sales
    group by
        order_id
        , delivery_id
)
, all_join as (
    select
        dc.id as courier_id
        , dc.courier_name
        , dt.year as settlement_year
        , dt.month as settlement_month
        , oa.order_id
        , oa.total_order_sum
        , dd.courier_rate
        , dd.tip_sum
    from order_agg as oa
    inner join dds.dm_orders as do2
        on oa.order_id = do2.id
    inner join dds.dm_timestamps as dt
        on do2.timestamp_id = dt.id
    inner join dds.dm_deliveries as dd
        on oa.delivery_id = dd.id
    inner join dds.dm_couriers as dc
        on dd.courier_id = dc.id
)
, rate_agg as (
    select
        courier_id
        , settlement_year
        , settlement_month
        , avg(courier_rate) as avg_courier_rate
    from all_join
    group by
        courier_id
        , settlement_year
        , settlement_month
)
, bonus_border as (
    select
        aj.courier_id
        , aj.settlement_year
        , aj.settlement_month
        , aj.order_id
        , aj.total_order_sum
        , ra.avg_courier_rate
        , case
            when avg_courier_rate < 4
                then 0.05
            when avg_courier_rate >= 4 and avg_courier_rate < 4.5
                then 0.07
            when avg_courier_rate >= 4.5 and avg_courier_rate < 4.9
                then 0.08
            when avg_courier_rate >= 4.9
                then 0.1
        end as courier_bonus_pct
        , case
            when avg_courier_rate < 4
                then 100
            when avg_courier_rate >= 4 and avg_courier_rate < 4.5
                then 150
            when avg_courier_rate >= 4.5 and avg_courier_rate < 4.9
                then 175
            when avg_courier_rate >= 4.9
                then 200
        end as courier_bonus_border
    from all_join as aj
    inner join rate_agg as ra
        on
            aj.courier_id = ra.courier_id
            and aj.settlement_year = ra.settlement_year
            and aj.settlement_month = ra.settlement_month
)
select
    aj.courier_id
    , aj.courier_name
    , aj.settlement_year
    , aj.settlement_month
    , count(aj.order_id) as orders_count
    , sum(aj.total_order_sum) as orders_total_sum
    , avg(aj.courier_rate) as rate_avg
    , sum(aj.total_order_sum) * 0.25 as order_processing_fee
    , sum(bo.courier_bonus) as courier_order_sum
    , sum(aj.tip_sum) as courier_tips_sum
    , sum(bo.courier_bonus) + sum(aj.tip_sum) * 0.95 as courier_reward_sum
from all_join as aj
inner join
    (
        select
            courier_id
            , settlement_year
            , settlement_month
            , order_id
            , greatest(
                (total_order_sum * courier_bonus_pct), courier_bonus_border
            ) as courier_bonus
        from bonus_border
    ) as bo
    on aj.order_id = bo.order_id
group by
aj.courier_id
, aj.courier_name
, aj.settlement_year
, aj.settlement_month
on conflict (courier_id, settlement_year, settlement_month) do update
set
courier_name = excluded.courier_name
, orders_count = excluded.orders_count
, orders_total_sum = excluded.orders_total_sum
, rate_avg = excluded.rate_avg
, order_processing_fee = excluded.order_processing_fee
, courier_order_sum = excluded.courier_order_sum
, courier_tips_sum = excluded.courier_tips_sum
, courier_reward_sum = excluded.courier_reward_sum;
