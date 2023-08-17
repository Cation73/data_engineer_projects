import logging
import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


# получение реквизитов подключения
postgres_conn_id = 'PG_WAREHOUSE_CONNECTION'

# настройка логирования
task_logger = logging.getLogger('airflow.task')

# зададим дату
business_dt = '{{ ds }}'

with DAG(
        'cdm_dag_review',
        schedule_interval='30 9 2 * *',
        start_date=pendulum.datetime(2022, 5, 2, tz='UTC'),
        catchup=False,
        tags=['sprint5', 'project'],
        is_paused_upon_creation=False) as dag:
    
    begin = DummyOperator(task_id='begin')

    insert_cdm_cour = PostgresOperator(
        task_id='insert_cdm_cour',
        postgres_conn_id=postgres_conn_id,
        sql='sql/dm_courier_ledger_review.sql')

    end = DummyOperator(task_id='end')

    begin >> insert_cdm_cour >> end
