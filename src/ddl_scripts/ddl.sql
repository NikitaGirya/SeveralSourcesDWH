----==== СЛОЙ STG ====----

-- ddl таблицы users из bonussystem (PostgreSQL)
drop table if exists stg.bonussystem_users;
create table stg.bonussystem_users
(
    id integer primary key
    , order_user_id text not null
);

-- ddl таблицы ranks из bonussystem (PostgreSQL)
drop table if exists stg.bonussystem_ranks;
create table stg.bonussystem_ranks
(
    id integer primary key
    , name varchar not null
    , bonus_percent numeric(19, 5) default 0 not null
    , min_payment_threshold numeric(19, 5) default 0 not null
);

-- ddl таблицы events из bonussystem (PostgreSQL)
drop table if exists stg.bonussystem_events;
create table stg.bonussystem_events
(
    id integer primary key
    , event_ts timestamp not null
    , event_type varchar not null
    , event_value text not null
);
create index idx_stg_bonussystem_events_event_ts on stg.bonussystem_events using btree (event_ts);

-- ddl таблицы users из ordersystem (MongoDB)
drop table if exists stg.ordersystem_users;
create table stg.ordersystem_users
(
    id serial primary key
    , object_id varchar not null
    , object_value text not null
    , update_ts timestamp not null
);

-- ddl таблицы restaurants из ordersystem (MongoDB)
drop table if exists stg.ordersystem_restaurants;
create table stg.ordersystem_restaurants
(
    id serial primary key
    , object_id varchar not null
    , object_value text not null
    , update_ts timestamp not null
);

-- ddl таблицы orders из ordersystem (Mongo DB)
drop table if exists stg.ordersystem_orders;
create table stg.ordersystem_orders
(
    id serial primary key
    , object_id varchar not null
    , object_value text not null
    , update_ts timestamp not null
);

-- ddl таблицы couriers из deliverysystem (API)
drop table if exists stg.deliverysystem_couriers;
create table stg.deliverysystem_couriers
(
    id serial primary key
    , object_id varchar not null
    , object_value text not null
);

-- ddl таблицы deliveries из deliverysystem (API)
drop table if exists stg.deliverysystem_deliveries;
create table stg.deliverysystem_deliveries
(
    id serial primary key
    , object_id varchar not null
    , object_value text not null
    , update_ts timestamp not null
);

-- ddl таблицы с параметрами подрезки слоя STG
drop table if exists stg.cut_param;
create table stg.cut_param
(
    id serial primary key
    , table_name varchar not null
    , cut_param bigint not null
    , update_ts timestamp not null default now()
);


----==== СЛОЙ DDS ====----

-- ddl измерения "Курьер"
drop table if exists dds.dm_couriers cascade;
create table dds.dm_couriers
(
    id serial primary key
    , courier_id varchar not null
    , courier_name varchar not null
    , constraint dds_dm_couriers_unique_courier_id unique (courier_id)
);

-- ddl измерения "Пользователь"
drop table if exists dds.dm_users cascade;
create table dds.dm_users
(
    id serial primary key
	, user_id varchar not null
	, user_name varchar not null
	, user_login varchar not null
    , active_from timestamp not null
    , active_to timestamp not null
    , constraint dds_dm_users_unique_user_id_active_from unique (user_id, active_from)
);

-- ddl измерения "Ресторан"
drop table if exists dds.dm_restaurants cascade;
create table dds.dm_restaurants
(
    id serial primary key
    , restaurant_id varchar not null
    , restaurant_name varchar not null
    , active_from timestamp not null
    , active_to timestamp not null
    , constraint dds_dm_restaurants_unique_restaurant_id_active_from unique (restaurant_id, active_from)
);

-- ddl измерения "Время"
drop table if exists dds.dm_timestamps cascade;
create table dds.dm_timestamps (
    id serial primary key
    , ts timestamp not null
    , year smallint not null check (year >= 2022 and year < 2500)
    , month smallint not null check (month >= 1 and month <= 12)
    , day smallint not null check (day >= 1 and day <= 31)
    , time time not null
    , date date not null
);

-- ddl измерения "Продукт"
drop table if exists dds.dm_products cascade;
create table dds.dm_products
(
    id serial primary key
    , product_id varchar not null
    , restaurant_id integer not null
    , product_name varchar not null
    , product_price numeric(14, 2) default 0 not null check (product_price >= 0)
    , active_from timestamp not null
    , active_to timestamp not null
    , constraint dds_dm_products_unique_product_id_active_from unique (product_id, active_from)
    , constraint dds_dm_products_restaurant_id_fkey foreign key (restaurant_id) references dds.dm_restaurants (id)
);

-- ddl измерения "Доставка"
drop table if exists dds.dm_deliveries cascade;
create table dds.dm_deliveries
(
    id serial primary key
    , delivery_id varchar not null
    , timestamp_id integer not null
    , courier_id integer not null
    , address text not null
    , courier_rate numeric(4, 3) default 0 not null
    , tip_sum numeric(19, 5) default 0 not null
    , constraint dds_dm_deliveries_unique_delivery_id unique (delivery_id)
    , constraint dds_dm_deliveries_courier_id_fkey foreign key (courier_id) references dds.dm_couriers (id)
	, constraint dds_dm_deliveries_timestamp_id_fkey foreign key (timestamp_id) references dds.dm_timestamps (id)         
);

-- ddl измерения "Заказ"
drop table if exists dds.dm_orders cascade;
create table dds.dm_orders (
    id serial primary key                          
    , user_id integer not null                       
    , restaurant_id integer not null            
    , timestamp_id integer not null      
    , order_key varchar not null      
    , order_status varchar not null         
    , constraint dds_dm_orders_order_key unique (order_key)
    , constraint dds_dm_orders_user_id_fkey foreign key (user_id) references dds.dm_users (id)
	, constraint dds_dm_orders_restaurant_id_fkey foreign key (restaurant_id) references dds.dm_restaurants (id)
	, constraint dds_dm_orders_timestamp_id_fkey foreign key (timestamp_id) references dds.dm_timestamps (id)      
);

-- ddl фактовой таблицы "Продажи продуктов"
drop table if exists dds.fct_product_sales cascade;
create table dds.fct_product_sales (
    id serial primary key                                  
    , order_id integer not null                     
    , product_id integer not null       
    , delivery_id integer not null 
    , count integer not null default 0 check (count >= 0)
    , price numeric(14, 2) not null default 0 check (price >= 0)
    , total_sum numeric(14, 2) not null default 0 check (total_sum >= 0)
    , bonus_payment numeric(14, 2) not null default 0 check (bonus_payment >= 0)
    , bonus_grant numeric(14, 2) not null default 0 check (bonus_grant >= 0)   
    , constraint dds_fct_product_sales_order_id_product_id unique (order_id, product_id)
    , constraint dds_fct_product_sales_product_id_fkey foreign key (product_id) references dds.dm_products (id)
	, constraint dds_fct_product_sales_order_id_fkey foreign key (order_id) references dds.dm_orders (id)
	, constraint dds_fct_product_sales_delivery_id_fkey foreign key (delivery_id) references dds.dm_deliveries (id)
);

-- ddl таблицы с параметрами подрезки слоя DDS
drop table if exists dds.cut_param;
create table dds.cut_param
(
    id serial primary key
    , table_name varchar not null
    , cut_param bigint not null
    , update_ts timestamp not null default now()
);


----==== СЛОЙ CDM ====----

-- ddl витрины взаиморасчетов с ресторанами
drop table if exists cdm.dm_settlement_report;
create table cdm.dm_settlement_report (
    id serial primary key
    , restaurant_id varchar not null
    , restaurant_name varchar not null
    , settlement_date date not null check (settlement_date >= '2022-01-01' and settlement_date < '2500-01-01')
    , orders_count integer not null default 0 check (orders_count >= 0)
    , orders_total_sum numeric(14, 2) not null default 0 check (orders_total_sum >= 0)
    , orders_bonus_payment_sum numeric(14, 2) not null default 0 check (orders_bonus_payment_sum >= 0)
    , orders_bonus_granted_sum numeric(14, 2) not null default 0 check (orders_bonus_granted_sum >= 0)
    , order_processing_fee numeric(14, 2) not null default 0 check (order_processing_fee >= 0)
    , restaurant_reward_sum numeric(14, 2) not null default 0 check (restaurant_reward_sum >= 0)
    , constraint cdm_dm_settlement_report_unique_restaurant_id_settlement_date unique (restaurant_id, settlement_date)
);

-- ddl витрины выплат курьерам
drop table if exists cdm.dm_courier_ledger;
create table cdm.dm_courier_ledger (
    id serial primary key
    , courier_id varchar not null
    , courier_name varchar not null
    , settlement_year integer not null check (settlement_year >= 2022 and settlement_year < 2500)
    , settlement_month integer not null check (settlement_month >= 1 and settlement_month <= 12)
    , rate_avg numeric(4, 3) not null check (rate_avg > 0)
    , orders_count integer not null default 0 check (orders_count >= 0)
    , orders_total_sum numeric(14, 2) not null default 0 check (orders_total_sum >= 0)
    , order_processing_fee numeric(14, 2) not null default 0 check (order_processing_fee >= 0)
    , courier_order_sum numeric(14, 2) not null default 0 check (courier_order_sum >= 0)
    , courier_tips_sum numeric(14, 2) not null default 0 check (courier_tips_sum >= 0)
    , courier_reward_sum numeric(14, 2) not null default 0 check (courier_reward_sum >= 0)
    , constraint cdm_dm_courier_ledger_unique_courier_id_settlement_year_settlement_month unique (courier_id, settlement_year, settlement_month)
);
