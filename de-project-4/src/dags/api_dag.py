import datetime as dt 
import json
import requests
import logging
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.http_hook import HttpHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


# получение реквизитов подключения
http_conn_id = HttpHook.get_connection('api_id')
api_key = http_conn_id.extra_dejson.get('api_key')
base_url = http_conn_id.host

postgres_conn_id = 'PG_WAREHOUSE_CONNECTION'

nickname = 'mr.sokolov.v.v'
cohort = '5'

headers = {
    'X-Nickname': nickname,
    'X-Cohort': cohort,
    'X-Project': 'True',
    'X-API-KEY': api_key
}

# настройка логирования
task_logger = logging.getLogger('airflow.task')

# зададим дату
business_dt = '{{ ds }}'



def upload_data_to_staging(url, table, columns, headers):
    task_logger.info(f'Connect to postgresql')
    postgres_dwh_hook = PostgresHook(postgres_conn_id)
    conn_dwh_hook = postgres_dwh_hook.get_conn()
    cur_dwh_hook = conn_dwh_hook.cursor()
    
    if table == 'couriers':
        task_logger.info(f'Get offset value from service table')
        cur_dwh_hook.execute('SELECT max(offset) from stg.api_service_couriers')
        max_offset = cur_dwh_hook.fetchall()
        offset = max_offset[0][0]
        params = f'?offset={offset}&sort_field=_id&sort_direction=asc'

    elif table == 'deliveries':
        offset = 0
        params = f'?offset={offset}&sort_field=_id&sort_direction=asc&to={business_dt}'

    elif table == 'restaurants':
        offset = 0
        params = f'?offset={offset}&sort_field=_id&sort_direction=asc'

    task_logger.info(f'Upload api-data from {table} to staging layer')
    columns_insert = ','.join(columns)
    num_columns = ['%s, ' * len(columns)]
    num_columns = ','.join(num_columns)[0:-2]    

    while offset < 10**8:
        response = requests.get(url + table + params, headers=headers)
        r_json = json.loads(response.content)

        values = [[value for value in r_json[i].values()] for i in range(len(r_json))] 
        values = [values[i] + [dt.datetime.now()] for i in range(len(values))]

        cur_dwh_hook.execute('ROLLBACK')
        conn_dwh_hook.commit()

        if table == 'restaurants':
            load_rest = [values[i][0] for i in range(len(values))]

            cur_dwh_hook.execute('SELECT object_id from stg.api_restaurants')
            rest_fet = cur_dwh_hook.fetchall()
            db_rest = [rest_fet[i][0] for i in range(len(rest_fet))]
            
            if load_rest != db_rest:
                cur_dwh_hook.executemany(f'INSERT INTO stg.api_{table} ({columns_insert}) values ({num_columns})', values)
                conn_dwh_hook.commit()
        else:
            cur_dwh_hook.executemany(f'INSERT INTO stg.api_{table} ({columns_insert}) values ({num_columns})', values)
            conn_dwh_hook.commit()

        if len(r_json) == 0:
            break

        offset += 50
    
    if table == 'couriers':
        task_logger.info(f'Upload to stg.api_servuce_couriers')
        service_values = [offset, dt.datetime.now()]
        cur_dwh_hook.executemany(f'INSERT INTO from stg.api_service_couriers (offset, update_ts) values (%s, %s)', service_values)

    task_logger.info(f'Done upload to stg.api_{table}')


with DAG(
        'api_dag_review',
        schedule_interval='30 9 * * *',
        start_date=pendulum.datetime(2022, 5, 5, tz='UTC'),
        catchup=False,
        tags=['sprint5', 'project'],
        is_paused_upon_creation=False) as dag:

    begin = DummyOperator(task_id='begin')

    insert_restaurants = PythonOperator(
        task_id='insert_restaurants',
        python_callable=upload_data_to_staging,
        op_kwargs={'url': base_url,
                   'table': 'restaurants',
                   'headers': headers,
                   'columns': ['restaurant_id', 'restaurant_name', 'update_ts']})
    
    insert_couriers = PythonOperator(
        task_id='insert_couriers',
        python_callable=upload_data_to_staging,
        op_kwargs={'url': base_url,
                   'table': 'couriers',
                   'headers': headers,
                   'columns': ['courier_id', 'courier_name', 'update_ts']})
                   
    insert_deliveries = PythonOperator(
        task_id='insert_deliveries',
        python_callable=upload_data_to_staging,
        op_kwargs={'url': base_url,
                   'table': 'deliveries',
                   'headers': headers,
                   'columns': ['order_id', 'order_ts', 'delivery_id',
                                'courier_id', 'address', 'delivery_ts', 
                                'rate', 'sum', 'tip_sum']})

    insert_dds_res = PostgresOperator(
        task_id='insert_dds_res',
        postgres_conn_id=postgres_conn_id,
        sql='sql/dm_restaurants_review.sql',
        parameters={"date": {business_dt}})

    insert_dds_cour = PostgresOperator(
        task_id='insert_dds_cour',
        postgres_conn_id=postgres_conn_id,
        sql='sql/dm_couriers_review.sql',
        parameters={"date": {business_dt}})
   
    insert_dds_ts = PostgresOperator(
        task_id='insert_dds_ts',
        postgres_conn_id=postgres_conn_id,
        sql='sql/dm_timestamps_review.sql',
        parameters={"date": {business_dt}})

    insert_dds_deliv = PostgresOperator(
        task_id='insert_dds_deliv',
        postgres_conn_id=postgres_conn_id,
        sql='sql/dm_deliveries_review.sql',
        parameters={"date": {business_dt}})

    insert_cdm_cour = PostgresOperator(
        task_id='insert_cdm_cour',
        postgres_conn_id=postgres_conn_id,
        sql='sql/dm_courier_ledger.sql')

    end = DummyOperator(task_id='end')

    begin >> [insert_restaurants, insert_couriers, insert_deliveries] >> [insert_dds_res, insert_dds_cour, insert_dds_ts] >> insert_dds_deliv >> insert_cdm_cour >> end
