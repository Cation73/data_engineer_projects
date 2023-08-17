# импорт модулей
import time
import requests
import json
import pandas as pd

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.http_hook import HttpHook
from airflow.decorators import task

import logging

# получение реквизитов подключения
http_conn_id = HttpHook.get_connection('http_conn_id')
api_key = http_conn_id.extra_dejson.get('api_key')
base_url = http_conn_id.host

postgres_conn_id = 'postgresql_de'

nickname = 'mr.sokolov.v.v'
cohort = '5'

headers = {
    'X-Nickname': nickname,
    'X-Cohort': cohort,
    'X-Project': 'True',
    'X-API-KEY': api_key,
    'Content-Type': 'application/x-www-form-urlencoded'
}

# настройка логирования
task_logger = logging.getLogger('airflow.task')


# генерация отчета по api
@task.python
def generate_report(ti):
    task_logger.info('Making request generate_report')

    response = requests.post(f'{base_url}/generate_report', headers=headers)
    response.raise_for_status()
    task_id = json.loads(response.content)['task_id']
    ti.xcom_push(key='task_id', value=task_id)
    task_logger.info(f'Response is {response.content}')

# получение отчета по api
@task.python
def get_report(ti):
    task_logger.info('Making request get_report')
    task_id = ti.xcom_pull(key='task_id')

    report_id = None

    for i in range(20):
        response = requests.get(f'{base_url}/get_report?task_id={task_id}', headers=headers)
        response.raise_for_status()
        print(f'Response is {response.content}')
        status = json.loads(response.content)['status']
        if status == 'SUCCESS':
            report_id = json.loads(response.content)['data']['report_id']
            break
        else:
            time.sleep(10)

    if not report_id:
        raise TimeoutError()

    ti.xcom_push(key='report_id', value=report_id)
    task_logger.info(f'Report_id={report_id}')

# получение инкрементных файлов
@task.python
def get_increment(date, ti):
    task_logger.info('Making request get_increment')
    report_id = ti.xcom_pull(key='report_id')
    response = requests.get(
        f'{base_url}/get_increment?report_id={report_id}&date={str(date)}T00:00:00',
        headers=headers)
    response.raise_for_status()
    print(f'Response is {response.content}')

    increment_id = json.loads(response.content)['data']['increment_id']
    ti.xcom_push(key='increment_id', value=increment_id)
    task_logger.info(f'increment_id={increment_id}')

# загрузка данных в слой staging
@task.python
def upload_data_to_staging(filename, date, pg_table, pg_schema, ti):
    task_logger.info('Uploda data to staging')
    increment_id = ti.xcom_pull(key='increment_id')
    s3_filename = f'https://storage.yandexcloud.net/s3-sprint3/cohort_{cohort}/{nickname}/project/{increment_id}/{filename}'
    
    if increment_id is not None: 
        response = requests.get(s3_filename).content 
        f = io.StringIO(response.decode('utf-8')) 
        # подключение к postgresql
        postgres_hook = PostgresHook(postgres_conn_id)
    
        conn = hook.get_conn()
        cur = conn.cursor()
        # добавил этот шаг из-за возможных ошибок, при тесте возникали ошибки, rollback помог 
        cur.execute('ROLLBACK')
        conn.commit()
        # заливка данных 
        if 'status' in f.readline():
            cur.copy_expert(f'''
                            COPY {pg_schema}.{pg_table} (id, date_time, city_id, city_name, 
                                                        customer_id, first_name,
                                                        last_name, item_id, item_name, 
                                                        quantity, payment_amount, status) 
                            FROM STDIN WITH CSV''', f)   
            task_logger.info('Execute with status is completed')
        else:
            cur.copy_expert(f'''
                            COPY {pg_schema}.{pg_table} (id, date_time, city_id, city_name,
                                                        customer_id, first_name,
                                                        last_name, item_id, item_name,
                                                        quantity, payment_amount) 
                            FROM STDIN WITH CSV''', f) 
            task_logger.info('Execute without status is completed')
        conn.commit()
        conn.close()
    else:
        task_logger.warning('Task without increment_id')


args = {
    "owner": "student",
    'email': ['student@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

business_dt = '{{ ds }}'

with DAG(
        'customer_retention',
        default_args=args,
        description='Provide default dag for sprint3',
        catchup=True,
        start_date=datetime.today() - timedelta(days=8),
        end_date=datetime.today() - timedelta(days=1),
) as dag:
    generate_report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report)

    get_report = PythonOperator(
        task_id='get_report',
        python_callable=get_report)

    get_increment = PythonOperator(
        task_id='get_increment',
        python_callable=get_increment,
        op_kwargs={'date': business_dt})

    upload_user_order_inc = PythonOperator(
        task_id='upload_user_order_inc',
        python_callable=upload_data_to_staging,
        op_kwargs={'date': business_dt,
                   'filename': 'user_orders_log_inc.csv',
                   'pg_table': 'tmp_user_order_log',
                   'pg_schema': 'staging'})
    
    prepare_uol = PostgresOperator(
        task_id='prepare_user_order_log',
        postgres_conn_id=postgres_conn_id,
        sql="sql/prepare_uol.sql")
        
    update_d_item_table = PostgresOperator(
        task_id='update_d_item',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.d_item.sql")

    update_d_customer_table = PostgresOperator(
        task_id='update_d_customer',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.d_customer.sql")

    update_d_city_table = PostgresOperator(
        task_id='update_d_city',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.d_city.sql")

    update_f_sales = PostgresOperator(
        task_id='update_f_sales',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.f_sales.sql",
        parameters={"date": {business_dt}})
        
    update_f_customer_retention = PostgresOperator(
        task_id='update_f_customer_retention',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.f_customer_retention.sql")

    (
            generate_report
            >> get_report
            >> get_increment
            >> upload_user_order_inc
            >> prepare_uol
            >> [update_d_item_table, update_d_city_table, update_d_customer_table]
            >> update_f_sales 
            >> update_f_customer_retention
    )
