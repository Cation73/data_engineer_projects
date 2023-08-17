from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable

import pendulum 
import logging
import vertica_python


vertica_host = Variable.get('VERTICA_HOST')
vertica_port = Variable.get('VERTICA_PORT')
vertica_user = Variable.get('VERTICA_USER')
vertica_password = Variable.get('VERTICA_PASSWORD')

conn_info = {'host': vertica_host,
             'port': vertica_port,
             'user': vertica_user,
             'password': vertica_password,
             'unicode_error': 'strict',
             'ssl': False,
             'autocommit': True,
             'use_prepared_statements': False,
             'connection_timeout': 5
             }

# настройка логирования
task_logger = logging.getLogger('airflow.task')

# заливка данных в витрину dwh
def load_data_dwh(path_file: str):
    try:
        # подключение к vertica
        task_logger.info('Connect to Vertica')

        conn = vertica_python.connect(**conn_info)
        cur = conn.cursor()
        
        # чтение и запись данных в БД
        task_logger.info('Read SQL file and insert data to Vertica')
        with open(path_file, 'r') as query:
            cur.execute(f'{query.read()}')
            cur.execute('commit;')  
    
    except Exception as error:
        task_logger.error(f'Dont load data to dwh: {error}')



# формирование дага
with DAG(
        dag_id = 'stg_to_dwh_vertica',
        description='load data from stg to dwh of vertica',
        catchup=False,
        schedule_interval='0 4 * * *',
        start_date=pendulum.parse('2022-11-01')
) as dag:
    
    start = EmptyOperator(task_id='begin')

    # заливка данных в витрину global_metrics
    global_metrics_dwh = PythonOperator(
                    task_id='global_metrics_dwh',
                    python_callable=load_data_dwh,
                    op_kwargs={'path_file': '/src/sql/dwh_global_metrics.sql'}
                    )
    
    
    end = EmptyOperator(task_id='end')

    start >> global_metrics_dwh  >> end