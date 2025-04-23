drop table if exists new_restaurants;
create temp table new_restaurants as 
with increment as
	(
	select distinct object_id
	from stg.ordersystem_restaurants
	where
	    id > (
				select coalesce(max(cut_param), 0) as cut_param
				from dds.cut_param
				where table_name = 'dm_restaurants'
			 )
	)
select
	'dm_restaurants' as table_name
	, id
    , object_id as restaurant_id
    , regexp_replace(object_value, '''', '"', 'g')::json ->> 'name' as restaurant_name
    , update_ts as active_from
    , lead((update_ts - interval '1 second'), 1, '5999-01-01'::timestamp) over (partition by object_id order by update_ts) as active_to
from
    stg.ordersystem_restaurants
inner join increment
	using(object_id);

insert into dds.dm_restaurants (restaurant_id, restaurant_name, active_from, active_to)
select
    restaurant_id
    , restaurant_name
    , active_from
    , active_to
from new_restaurants
where exists (select 1 from new_restaurants)
on conflict (restaurant_id, active_from) do update
set 
    restaurant_name = EXCLUDED.restaurant_name
    , active_from = EXCLUDED.active_from
    , active_to = EXCLUDED.active_to;

insert into dds.cut_param (table_name, cut_param)
select
    table_name
    , max(id) as cut_param
from new_restaurants
where exists (select 1 from new_restaurants)
group by table_name;
