drop table if exists new_deliveries;
create temp table new_deliveries as 
with increment as
	(
	select distinct object_id
	from stg.deliverysystem_deliveries
	where
	    id > (
				select coalesce(max(cut_param), 0) as cut_param
				from dds.cut_param
				where table_name = 'dm_deliveries'
			 )
	)
, unfold_json as
	(
	select
		id
		, object_id
		, regexp_replace(object_value, '''', '"', 'g')::json ->> 'courier_id' as courier_id
	    , regexp_replace(object_value, '''', '"', 'g')::json ->> 'address' as address
	    , regexp_replace(object_value, '''', '"', 'g')::json ->> 'rate' as courier_rate
	    , regexp_replace(object_value, '''', '"', 'g')::json ->> 'tip_sum' as tip_sum
	    , update_ts as delivery_ts
	from
	    stg.deliverysystem_deliveries
	inner join increment
		using(object_id)
	)		
select
	'dm_deliveries' as table_name
	, uj.id
	, uj.object_id as delivery_id
	, dt.id as timestamp_id
	, dc.id as courier_id
	, uj.address
	, uj.courier_rate::numeric
	, uj.tip_sum::numeric
from unfold_json as uj
inner join dds.dm_couriers as dc
	on uj.courier_id = dc.courier_id
inner join dds.dm_timestamps as dt
	on uj.delivery_ts = dt.ts;

insert into dds.dm_deliveries (delivery_id, timestamp_id, courier_id, address, courier_rate, tip_sum)
select
    delivery_id
    , timestamp_id
    , courier_id
    , address
    , courier_rate
    , tip_sum
from new_deliveries
where exists (select 1 from new_deliveries)
on conflict (delivery_id) do update
set 
    timestamp_id = EXCLUDED.timestamp_id
    , courier_id = EXCLUDED.courier_id
	, address = EXCLUDED.address
	, courier_rate = EXCLUDED.courier_rate
    , tip_sum = EXCLUDED.tip_sum;

insert into dds.cut_param (table_name, cut_param)
select
    table_name
    , max(id) as cut_param
from new_deliveries
where exists (select 1 from new_deliveries)
group by table_name;
