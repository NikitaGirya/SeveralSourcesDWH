drop table if exists new_products;
create temp table new_products as 
with increment as
	(
	select distinct object_id
	from stg.ordersystem_restaurants
	where
	    id > (
				select coalesce(max(cut_param), 0) as cut_param
				from dds.cut_param
				where table_name = 'dm_products'
			 )
	)
, unfold_json as
	(
	select
		id
		, object_id
		, json_array_elements((regexp_replace(object_value, '''', '"', 'g')::json ->> 'menu')::json) ->> '_id' as product_id
	    , json_array_elements((regexp_replace(object_value, '''', '"', 'g')::json ->> 'menu')::json) ->> 'name' as product_name
	    , json_array_elements((regexp_replace(object_value, '''', '"', 'g')::json ->> 'menu')::json) ->> 'price' as product_price
	    , update_ts 
	from
	    stg.ordersystem_restaurants
	inner join increment
		using(object_id)
	)
select
	'dm_products' as table_name
	, uj.id
	, dr.id as restaurant_id
	, uj.product_id
	, uj.product_name
	, uj.product_price::numeric(14, 2)
	, uj.update_ts as active_from
    , lead((uj.update_ts - interval '1 second'), 1, '5999-01-01'::timestamp) over (partition by uj.product_id order by uj.update_ts) as active_to
from unfold_json as uj
inner join dds.dm_restaurants as dr
	on uj.object_id = dr.restaurant_id
	and uj.update_ts = dr.active_from;

insert into dds.dm_products (product_id, restaurant_id, product_name, product_price, active_from, active_to)
select
    product_id
    , restaurant_id
    , product_name
    , product_price
    , active_from
    , active_to
from new_products
where exists (select 1 from new_products)
on conflict (product_id, active_from) do update
set 
    restaurant_id = EXCLUDED.restaurant_id
    , product_name = EXCLUDED.product_name
    , product_price = EXCLUDED.product_price
	, active_from = EXCLUDED.active_from
	, active_to = EXCLUDED.active_to;

insert into dds.cut_param (table_name, cut_param)
select
    table_name
    , max(id) as cut_param
from new_products
where exists (select 1 from new_products)
group by table_name;
