from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base import BaseHook
from airflow.hooks.http_hook import HttpHook
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable

import time
import requests
import pandas as pd
from bson import ObjectId
from datetime import datetime, timedelta
from pymongo import MongoClient
from sqlalchemy import create_engine


DWH_CONN = BaseHook.get_connection('PG_WAREHOUSE_CONNECTION')
DWH_ENGINE = create_engine(f'postgresql+psycopg2://{DWH_CONN.login}:{DWH_CONN.password}@{DWH_CONN.host}:{DWH_CONN.port}/{DWH_CONN.schema}')


def load_pg_data(target_table_name):

    pg_conn = BaseHook.get_connection('PG_ORIGIN_BONUS_SYSTEM_CONNECTION')
    pg_engine = create_engine(f'postgresql+psycopg2://{pg_conn.login}:{pg_conn.password}@{pg_conn.host}:{pg_conn.port}/{pg_conn.schema}')   

    with open('/lessons/src/ddl_scripts/select_stg_cut_param.sql', 'r') as select_stg_cut_param_file,\
         open(f'/lessons/src/ddl_scripts/select_stg_{target_table_name}.sql', 'r') as select_stg_file:
            pg_cut_param_query = select_stg_cut_param_file.read()
            source_select_query = select_stg_file.read()

    pg_cut_param = pd.read_sql(pg_cut_param_query.format(table_name=target_table_name), DWH_ENGINE)['cut_param'].iloc[0]

    df = pd.read_sql(source_select_query.format(cut_param=pg_cut_param), pg_engine)

    if not df.empty:
        with DWH_ENGINE.begin() as connection:
            df.to_sql(target_table_name, connection, schema='stg', if_exists='append', index=False)

            pd.DataFrame({
                'table_name': [target_table_name],
                'cut_param': [df['id'].max()]
            }).to_sql('cut_param', connection, schema='stg', if_exists='append', index=False)
    else:
        print(f"Для stg.{target_table_name} нет новых данных на источнике")
    

def load_mongo_data(source_table_name):

    mongo_db_user = Variable.get("MONGO_DB_USER")
    mongo_db_password = Variable.get("MONGO_DB_PASSWORD")
    mongo_db_host = Variable.get("MONGO_DB_HOST")
    mongo_db_replica_set = Variable.get("MONGO_DB_REPLICA_SET")
    mongo_db_database_name = Variable.get("MONGO_DB_DATABASE_NAME")
    mongo_db_certificate_path = Variable.get("MONGO_DB_CERTIFICATE_PATH")
    mongo_uri = f'mongodb://{mongo_db_user}:{mongo_db_password}@{mongo_db_host}/?replicaSet={mongo_db_replica_set}&authSource={mongo_db_database_name}&tlsCAFile={mongo_db_certificate_path}'

    with open('/lessons/src/ddl_scripts/select_stg_cut_param.sql', 'r') as select_stg_cut_param_file:
        mongo_cut_param_query = select_stg_cut_param_file.read()

    mongo_cut_param = pd.read_sql(mongo_cut_param_query.format(table_name='ordersystem_' + source_table_name), DWH_ENGINE)['cut_param'].iloc[0] / 1000000

    client = MongoClient(mongo_uri)
    db = client[mongo_db_database_name]
    collection = db.get_collection(source_table_name)
    filter = {'update_ts': {'$gt': datetime.fromtimestamp(mongo_cut_param)}}
    sort = [('update_ts', 1)]
    data = list(collection.find(filter=filter, sort=sort))

    def serialize_data(data):
        if isinstance(data, ObjectId):
            return str(data)
        elif isinstance(data, datetime):
            return data.isoformat()
        elif isinstance(data, list):
            return [serialize_data(item) for item in data]
        elif isinstance(data, dict):
            return {key: serialize_data(value) for key, value in data.items()}
        else:
            return data

    serialized_data = [serialize_data(item) for item in data]

    df = pd.DataFrame({
        'object_id': [str(item['_id']) for item in serialized_data],
        'object_value': [str(item) for item in serialized_data],
        'update_ts': [item['update_ts'] for item in serialized_data]
        })

    if not df.empty:
        with DWH_ENGINE.begin() as connection:
            df.to_sql('ordersystem_' + source_table_name, connection, schema='stg', if_exists='append', index=False)

            pd.DataFrame({
                'table_name': ['ordersystem_' + source_table_name],
                'cut_param': [int(pd.to_datetime(df['update_ts']).max().timestamp() * 1000000)]
            }).to_sql('cut_param', connection, schema='stg', if_exists='append', index=False)
    else:
        print(f"Для stg.ordersystem_{source_table_name} нет новых данных на источнике")


def load_api_data(source_name, sort_field, date_param=None):

    http_conn = HttpHook.get_connection('HTTP_DELIVERY_SYSTEM_CONNECTION')
    nickname = http_conn.extra_dejson.get('X-Nickname')
    cohort = http_conn.extra_dejson.get('X-Cohort')
    api_key = http_conn.extra_dejson.get('X-API-KEY')
    api_host = http_conn.host

    with open('/lessons/src/ddl_scripts/select_stg_cut_param.sql', 'r') as select_stg_cut_param_file:
        pg_cut_param_query = select_stg_cut_param_file.read()

    api_cut_param = pd.read_sql(pg_cut_param_query.format(table_name='deliverysystem_' + source_name), DWH_ENGINE)['cut_param'].iloc[0]

    api_url = f"https://{api_host}/{source_name}"

    headers = {
        'X-Nickname': nickname,
        'X-Cohort': cohort,
        'X-API-KEY': api_key
    }

    params = {
        'sort_field': sort_field,
        'sort_direction': 'asc',
        'limit': 50,  
        'offset': 0      
    }

    if date_param:
        params['from'] = datetime.fromtimestamp(api_cut_param)
        params['offset'] = 0
    else:
        params['offset'] = api_cut_param    

    all_data = []  

    while True:
        response = requests.get(api_url, headers=headers, params=params)

        data = response.json()
        if not data:
            break  

        all_data.extend(data)  
        params['offset'] += params['limit']
        time.sleep(0.5)
        
    df = pd.DataFrame(all_data)
    df['object_value'] = df.apply(lambda row: str(row.to_dict()), axis=1)

    if not df.empty:
        with DWH_ENGINE.begin() as connection:
            if date_param: 
                pd.DataFrame({
                    'object_id': df['delivery_id'],
                    'object_value': df['object_value'],
                    'update_ts': df['delivery_ts']
                }).to_sql('deliverysystem_' + source_name, connection, schema='stg', if_exists='append', index=False)
            else:  
                pd.DataFrame({
                    'object_id': df['_id'],
                    'object_value': df['object_value']
                }).to_sql('deliverysystem_' + source_name, connection, schema='stg', if_exists='append', index=False)

            pd.DataFrame({
                'table_name': ['deliverysystem_' + source_name],
                'cut_param': [api_cut_param + len(df) if not date_param else int(pd.to_datetime(df['delivery_ts']).dt.floor('S').max().timestamp())]
            }).to_sql('cut_param', connection, schema='stg', if_exists='append', index=False)
    else:
        print(f"Для stg.deliverysystem_{source_name} нет новых данных на источнике")


def load_pg_users_data():
    load_pg_data('bonussystem_users')

def load_pg_ranks_data():
    load_pg_data('bonussystem_ranks')

def load_pg_events_data():
    load_pg_data('bonussystem_events')

def load_mongo_users_data():
    load_mongo_data('users')

def load_mongo_restaurants_data():
    load_mongo_data('restaurants')

def load_mongo_orders_data():
    load_mongo_data('orders')       

def load_api_couriers_data():
    load_api_data('couriers', sort_field='_id')

def load_api_deliveries_data():
    load_api_data('deliveries', sort_field='date', date_param=True)  


default_args = {
    'owner': 'ngirya',
    'start_date': datetime.now() - timedelta(days=1),
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'dwh_main_dag',
    default_args=default_args,
    schedule_interval='0 */6 * * *',
)

with dag:
    with TaskGroup('stg_load_pg') as stg_load_pg:
        
        load_pg_users_task = PythonOperator(
            task_id='load_pg_users_data',
            python_callable=load_pg_users_data,
            dag=dag,
        )

        load_pg_ranks_task = PythonOperator(
            task_id='load_pg_ranks_data',
            python_callable=load_pg_ranks_data,
            dag=dag,
        )

        load_pg_events_task = PythonOperator(
            task_id='load_pg_events_data',
            python_callable=load_pg_events_data,
            dag=dag,
        )

    with TaskGroup('stg_load_mongo') as stg_load_mongo:
        
        load_mongo_users_task = PythonOperator(
            task_id='load_mongo_users_data',
            python_callable=load_mongo_users_data,
            dag=dag,
        )

        load_mongo_restaurants_task = PythonOperator(
            task_id='load_mongo_restaurants_data',
            python_callable=load_mongo_restaurants_data,
            dag=dag,
        )

        load_mongo_orders_task = PythonOperator(
            task_id='load_mongo_orders_data',
            python_callable=load_mongo_orders_data,
            dag=dag,
        )

    with TaskGroup('stg_load_api') as stg_load_api:
        
        load_mongo_users_task = PythonOperator(
            task_id='load_api_couriers_data',
            python_callable=load_api_couriers_data,
            dag=dag,
        )

        load_mongo_restaurants_task = PythonOperator(
            task_id='load_api_deliveries_data',
            python_callable=load_api_deliveries_data,
            dag=dag,
        )

    with TaskGroup('dds_load_step1') as dds_load_step1:
        
        load_dm_couriers_task = PostgresOperator(
            task_id='load_dm_couriers_task',
            postgres_conn_id='PG_WAREHOUSE_CONNECTION',
            sql=open('/lessons/src/ddl_scripts/update_dds_dm_couriers.sql').read()
        )

        load_dm_users_task = PostgresOperator(
            task_id='load_dm_users_task',
            postgres_conn_id='PG_WAREHOUSE_CONNECTION',
            sql=open('/lessons/src/ddl_scripts/update_dds_dm_users.sql').read()
        )

        load_dm_restaurants_task = PostgresOperator(
            task_id='load_dm_restaurants_task',
            postgres_conn_id='PG_WAREHOUSE_CONNECTION',
            sql=open('/lessons/src/ddl_scripts/update_dds_dm_restaurants.sql').read()
        )

        load_dm_timestamps_task = PostgresOperator(
            task_id='load_dm_timestamps_task',
            postgres_conn_id='PG_WAREHOUSE_CONNECTION',
            sql=open('/lessons/src/ddl_scripts/update_dds_dm_timestamps.sql').read()
        )

    with TaskGroup('dds_load_step2') as dds_load_step2:
        
        load_dm_products_task = PostgresOperator(
            task_id='load_dm_products_task',
            postgres_conn_id='PG_WAREHOUSE_CONNECTION',
            sql=open('/lessons/src/ddl_scripts/update_dds_dm_products.sql').read()
        )

        load_dm_deliveries_task = PostgresOperator(
            task_id='load_dm_deliveries_task',
            postgres_conn_id='PG_WAREHOUSE_CONNECTION',
            sql=open('/lessons/src/ddl_scripts/update_dds_dm_deliveries.sql').read()
        )

        load_dm_orders_task = PostgresOperator(
                task_id='load_dm_orders_task',
                postgres_conn_id='PG_WAREHOUSE_CONNECTION',
                sql=open('/lessons/src/ddl_scripts/update_dds_dm_orders.sql').read()
            )
        
    with TaskGroup('dds_load_step3') as dds_load_step3:
    
        load_fct_product_sales_task = PostgresOperator(
                task_id='load_fct_product_sales_task',
                postgres_conn_id='PG_WAREHOUSE_CONNECTION',
                sql=open('/lessons/src/ddl_scripts/update_dds_fct_product_sales.sql').read()
            )
    
    with TaskGroup('cdm_load') as cdm_load:
        
        load_dm_settlement_report_task = PostgresOperator(
            task_id='load_dm_settlement_report_task',
            postgres_conn_id='PG_WAREHOUSE_CONNECTION',
            sql=open('/lessons/src/ddl_scripts/update_cdm_dm_settlement_report.sql').read()
        )

        load_dm_courier_ledger_task = PostgresOperator(
            task_id='load_dm_courier_ledger_task',
            postgres_conn_id='PG_WAREHOUSE_CONNECTION',
            sql=open('/lessons/src/ddl_scripts/update_cdm_dm_courier_ledger.sql').read()
        )


[stg_load_pg, stg_load_mongo, stg_load_api] >> dds_load_step1 >> dds_load_step2 >> dds_load_step3 >> cdm_load
