drop table if exists new_users;
create temp table new_users as 
with increment as
	(
	select distinct object_id
	from stg.ordersystem_users
	where
	    id > (
				select coalesce(max(cut_param), 0) as cut_param
				from dds.cut_param
				where table_name = 'dm_users'
			 )
	)
select
	'dm_users' as table_name
	, id
    , object_id as user_id
    , regexp_replace(object_value, '''', '"', 'g')::json ->> 'name' as user_name
    , regexp_replace(object_value, '''', '"', 'g')::json ->> 'login' as user_login
    , update_ts as active_from
    , lead((update_ts - interval '1 second'), 1, '5999-01-01'::timestamp) over (partition by object_id order by update_ts) as active_to
from
    stg.ordersystem_users
inner join increment
	using(object_id);

insert into dds.dm_users (user_id, user_name, user_login, active_from, active_to)
select
    user_id
    , user_name
    , user_login
    , active_from
    , active_to
from new_users
where exists (select 1 from new_users)
on conflict (user_id, active_from) do update
set 
    user_name = EXCLUDED.user_name
    , user_login = EXCLUDED.user_login
    , active_from = EXCLUDED.active_from
    , active_to = EXCLUDED.active_to;

insert into dds.cut_param (table_name, cut_param)
select
    table_name
    , max(id) as cut_param
from new_users
where exists (select 1 from new_users)
group by table_name;
