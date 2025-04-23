drop table if exists unfold_json;
create temp table unfold_json as 
with increment as
	(
	select distinct object_id
	from stg.ordersystem_orders
	where
	    id > (
				select coalesce(max(cut_param), 0) as cut_param
				from dds.cut_param
				where table_name = 'fct_product_sales'
			 )
	)
select
	id
	, object_id
	, json_array_elements((regexp_replace(object_value, '''', '"', 'g')::json ->> 'order_items')::json) ->> 'id' as product_id
	, (json_array_elements((regexp_replace(object_value, '''', '"', 'g')::json ->> 'order_items')::json) ->> 'quantity')::integer as count
	, (json_array_elements((regexp_replace(object_value, '''', '"', 'g')::json ->> 'order_items')::json) ->> 'price')::numeric as price
from
    stg.ordersystem_orders 
inner join increment 
	using(object_id);

drop table if exists new_sales;
create temp table new_sales as 
select 
	'fct_product_sales' as table_name
	, uj.id
	, ord.id as order_id
	, dp.id as product_id
	, dd2.id as delivery_id
	, uj.count
	, uj.price
	, uj.count * uj.price as total_sum
	, be.bonus_payment
	, be.bonus_grant
from unfold_json as uj
inner join 
	(
	select 
		object_id as delivery_id
		, regexp_replace(object_value, '''', '"', 'g')::json ->> 'order_id' as order_id
	from stg.deliverysystem_deliveries
	) as dd
	on uj.object_id = dd.order_id
inner join 
	(
	select
		event_value::json->>'order_id' as order_id
		, json_array_elements(event_value::json -> 'product_payments') ->> 'product_id' as product_id
		, (json_array_elements(event_value::json->'product_payments')->>'bonus_payment')::numeric as bonus_payment
        , (json_array_elements(event_value::json->'product_payments')->>'bonus_grant')::numeric as bonus_grant
	from stg.bonussystem_events
	where event_type = 'bonus_transaction'
	) as be 
	on uj.object_id = be.order_id
	and uj.product_id = be.product_id
inner join dds.dm_orders as ord
	on uj.object_id = ord.order_key	
inner join dds.dm_products as dp
	on uj.product_id = dp.product_id
	and dp.active_to = '5999-01-01 00:00:00'		
inner join dds.dm_deliveries as dd2
	on dd.delivery_id = dd2.delivery_id;

insert into dds.fct_product_sales (order_id, product_id, delivery_id, count, price, total_sum, bonus_payment, bonus_grant)
select
    order_id
    , product_id
    , delivery_id
    , count
    , price
    , total_sum
    , bonus_payment
    , bonus_grant
from new_sales
where exists (select 1 from new_sales)
on conflict (order_id, product_id) do update
set 
    delivery_id = EXCLUDED.delivery_id
    , count = EXCLUDED.count
	, price = EXCLUDED.price
	, total_sum = EXCLUDED.total_sum
	, bonus_payment = EXCLUDED.bonus_payment
	, bonus_grant = EXCLUDED.bonus_grant;

insert into dds.cut_param (table_name, cut_param)
select
    table_name
    , max(id) as cut_param
from new_sales
where exists (select 1 from new_sales)
group by table_name;


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
select 	dr.restaurant_id 
		, dr.restaurant_name 
		, dt."date" as settlement_date
		, count(distinct fps.order_id) as orders_count
		, sum(fps.total_sum) as orders_total_sum
		, sum(fps.bonus_payment) as orders_bonus_payment_sum 
		, sum(fps.bonus_grant) as orders_bonus_granted_sum
		, sum(fps.total_sum * 0.25) as order_processing_fee
		, sum(fps.total_sum - (fps.total_sum * 0.25) - fps.bonus_payment) as restaurant_reward_sum
from 	dds.fct_product_sales fps 
inner join dds.dm_products dp 
		on fps.product_id = dp.id
		and dp.active_to = '5999-01-01 00:00:00'	
inner join dds.dm_restaurants dr 
		on dp.restaurant_id = dr.id		
		and dr.active_to = '5999-01-01 00:00:00'	
inner join dds.dm_orders do2
		on fps.order_id = do2.id	
inner join dds.dm_timestamps dt 
		on do2.timestamp_id = dt.id	
where 	do2.order_status = 'CLOSED'
group by 
		dr.restaurant_id 
		, dr.restaurant_name 
		, dt."date"
on conflict (restaurant_id, settlement_date) do update
set 
    restaurant_name = EXCLUDED.restaurant_name
    , orders_count = EXCLUDED.orders_count
    , orders_total_sum = EXCLUDED.orders_total_sum
    , orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum
    , orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum
    , order_processing_fee = EXCLUDED.order_processing_fee
    , restaurant_reward_sum = EXCLUDED.restaurant_reward_sum;
	