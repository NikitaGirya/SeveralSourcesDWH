drop table if exists new_orders;
create temp table new_orders as 
with increment as
	(
	select distinct object_id
	from stg.ordersystem_orders
	where
	    id > (
				select coalesce(max(cut_param), 0) as cut_param
				from dds.cut_param
				where table_name = 'dm_orders'
			 )
	)
, unfold_json as
	(
	select
		id
	    , (regexp_replace(object_value, '''', '"', 'g')::json ->> 'user')::json ->> 'id' as user_id
		, (regexp_replace(object_value, '''', '"', 'g')::json ->> 'restaurant')::json ->> 'id' as restaurant_id
	    , update_ts 
		, object_id as order_key
		, regexp_replace(object_value, '''', '"', 'g')::json ->> 'final_status' as order_status
	from
	    stg.ordersystem_orders
	inner join increment
		using(object_id)
	)		
select
	'dm_orders' as table_name 
	, uj.id 
	, du.id as user_id
	, dr.id as restaurant_id
	, dt.id as timestamp_id
	, uj.order_key
	, uj.order_status
from unfold_json as uj
inner join dds.dm_users as du
	on uj.user_id = du.user_id
	and du.active_to = '5999-01-01 00:00:00'
inner join dds.dm_restaurants as dr
	on uj.restaurant_id = dr.restaurant_id
	and dr.active_to = '5999-01-01 00:00:00'
inner join dds.dm_timestamps as dt
	on uj.update_ts	= dt.ts;

insert into dds.dm_orders (user_id, restaurant_id, timestamp_id, order_key, order_status)
select
    user_id
    , restaurant_id
    , timestamp_id
    , order_key
    , order_status
from new_orders
where exists (select 1 from new_orders)
on conflict (order_key) do update
set 
    user_id = EXCLUDED.user_id
    , restaurant_id = EXCLUDED.restaurant_id
	, timestamp_id = EXCLUDED.timestamp_id
	, order_status = EXCLUDED.order_status;

insert into dds.cut_param (table_name, cut_param)
select
    table_name
    , max(id) as cut_param
from new_orders
where exists (select 1 from new_orders)
group by table_name;
