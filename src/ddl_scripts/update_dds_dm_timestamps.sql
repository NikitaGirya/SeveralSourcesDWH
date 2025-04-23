drop table if exists new_timestamps;
create temp table new_timestamps as 
with increment_orders as
	(
	select distinct object_id
	from stg.ordersystem_orders
	where
	    id > (
				select coalesce(max(cut_param), 0) as cut_param
				from dds.cut_param
				where table_name = 'dm_timestamps_orders'
			 )
	)
, increment_deliveries as
	(
	select distinct object_id
	from stg.deliverysystem_deliveries
	where
	    id > (
				select coalesce(max(cut_param), 0) as cut_param
				from dds.cut_param
				where table_name = 'dm_timestamps_deliveries'
			 )
	)
select
	'dm_timestamps_orders' as table_name 
	, id 
	, update_ts as ts
	, extract(year from update_ts) as year
	, extract(month from update_ts) as month
	, extract(day from update_ts) as day
	, update_ts::date as date
	, update_ts::time as time
from
    stg.ordersystem_orders
inner join increment_orders
	using(object_id)
union all
select
	'dm_timestamps_deliveries' as table_name 
	, id 
	, update_ts as ts
	, extract(year from update_ts) as year
	, extract(month from update_ts) as month
	, extract(day from update_ts) as day
	, update_ts::date as date
	, update_ts::time as time
from
    stg.deliverysystem_deliveries
inner join increment_deliveries
	using(object_id);

insert into dds.dm_timestamps (ts, year, month, day, date, time)
select
    ts
    , year
    , month
    , day
    , date
    , time
from new_timestamps
where exists (select 1 from new_timestamps);

insert into dds.cut_param (table_name, cut_param)
select
    table_name
    , max(id) as cut_param
from new_timestamps
where table_name = 'dm_timestamps_orders'
	and exists (select 1 from new_timestamps where table_name = 'dm_timestamps_orders')
group by table_name;

insert into dds.cut_param (table_name, cut_param)
select
    table_name
    , max(id) as cut_param
from new_timestamps
where table_name = 'dm_timestamps_deliveries'
	and exists (select 1 from new_timestamps where table_name = 'dm_timestamps_deliveries')
group by table_name;
