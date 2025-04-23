drop table if exists new_couriers;
create temp table new_couriers as 
select
	'dm_couriers' as table_name
	, id
    , object_id as courier_id
    , regexp_replace(object_value, '''', '"', 'g')::json ->> 'name' as courier_name
from
    stg.deliverysystem_couriers
where
    id > (
			select coalesce(max(cut_param), 0) as cut_param
			from dds.cut_param
			where table_name = 'dm_couriers'
		 );

insert into dds.dm_couriers (courier_id, courier_name)
select
    courier_id
    , courier_name
from new_couriers
where exists (select 1 from new_couriers);

insert into dds.cut_param (table_name, cut_param)
select
    table_name
    , max(id) as cut_param
from new_couriers
where exists (select 1 from new_couriers)
group by table_name;
