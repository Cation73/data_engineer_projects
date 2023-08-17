from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from src.py.s3_connector import S3LoadFiles

import os
import pendulum 
import logging
import vertica_python

# получение реквизитов подключения
aws_access_key_id = Variable.get('AWS_ACCESS_KEY_ID')
aws_secret_access_key = Variable.get('AWS_SECRET_ACCESS_KEY')

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

# выгрузка данных из s3
def fetch_s3_file(aws_access_key_id: str, 
                  aws_secret_access_key: str,
                  bucket: str,
                  path_data: str,
                  ti
                  ):
    
    task_logger.info('Connect to S3')

    s3_connect = S3LoadFiles(aws_access_key_id=aws_access_key_id, 
                    aws_secret_access_key=aws_secret_access_key)
    
    task_logger.info('Get name files from S3')
    s3_files = s3_connect.get_name_files(bucket=bucket)

    data_files = [f for f in os.listdir(path_data) if os.path.isfile(os.path.join(path_data, f))]

    name_files_load = [x for x in s3_files if x not in data_files]
    full_name_files_load = [x for x in s3_files if x != 'currencies_history.csv']

    
    for file in name_files_load:
        try:
            task_logger.info(f'Download file: {file}')
            s3_connect.download_files(bucket = bucket,
                                        key = file,
                                        filename = '/data/')
        except Exception as error:
            task_logger.info(f'Dont load {file} from s3: {error}')
        
        finally:
            continue


    ti.xcom_push(key = 's3_files', value = name_files_load)
    ti.xcom_push(key = 's3_files_full', value = full_name_files_load)
    task_logger.info('Task done!')

# загрузка данных в staging-слой
def load_data_to_stg(file_path: str,
                     name_table: str,
                     type_load: str, 
                     ti):
    
    try:
        # подключение к vertica
        task_logger.info('Connect to Vertica')

        # в докере в airflow нет коннектов для вертики, поэтому решил использовать vertica_python - больше функциональности для загрузки CSV файлов в Vertica
        conn = vertica_python.connect(**conn_info)
        cur = conn.cursor()

    except Exception as error:
        task_logger.error(f'Dont connect Vertica: {error}')

    # загрузка данных по курсам
    if name_table == 'currencies':
        # полная загрузка данных в таблицу
        if type_load == 'full':
            try:
                # очистка истории курсов 
                task_logger.info('Delete from currencies table')

                cur.execute(f"DELETE FROM MRSOKOLOVVVYANDEXRU__STAGING.currencies")
                conn.commit()

                # загрузка файла курсов 
                task_logger.info('Load currencies to vertica (staging layer)')

                with open(file_path + name_table + '_history.csv') as fcsv:
                    csv_file = fcsv.read()
                    cur.copy(f"COPY MRSOKOLOVVVYANDEXRU__STAGING.{name_table} FROM STDIN DELIMITER ','", csv_file)
                    conn.commit()

            except Exception as error:
                task_logger.error(f'Dont update data in currencies: {error}')

    # загрузка данных по транзакциям
    elif name_table == 'transactions':
        # получение файлов из папки /data/
        task_logger.info('Load new name files from S3')

        name_files_load = ti.xcom_pull(key = 's3_files')
        full_name_files_load = ti.xcom_pull(key = 's3_files_full')

        # итерационная загрузка файлов в vertica
        if type_load == 'increment':
            try:
                for file in name_files_load: 
                    try:
                        task_logger.info(f'Load transactions to Vertica (staging layer): {file}')
                        with open(file_path + file) as fcsv:
                            csv_file = fcsv.read() 
                            cur.copy(f"COPY MRSOKOLOVVVYANDEXRU__STAGING.{name_table} FROM STDIN DELIMITER ','", csv_file)
                            conn.commit()
                            
                    except Exception as error:
                        task_logger.error(f'Dont load {file} to Vertica: {error}')
                    
                    finally:
                        continue

            except:
                task_logger.error('No new files in s3 storage')

        elif type_load == 'full':
            for file in full_name_files_load: 
                try:
                    task_logger.info(f'Load transactions to Vertica (staging layer): {file}')
                    with open(file_path + file) as fcsv:
                        csv_file = fcsv.read()
                        cur.copy(f"COPY MRSOKOLOVVVYANDEXRU__STAGING.{name_table} FROM STDIN DELIMITER ','", csv_file)
                        conn.commit()

                except Exception as error:
                    task_logger.error(f'Dont load {file} to Vertica: {error}')
        
                finally:
                    continue

    
    task_logger.info('Close connection Vertica')
    conn.close()
    task_logger.info('Task done!')

# формирование дага
with DAG(
        dag_id = 's3_to_stg_vertica',
        description='load data from s3 to staging-layer of vertica',
        catchup=False,
        schedule_interval='0 4 * * *',
        start_date=pendulum.parse('2022-11-01')
) as dag:
    
    start = EmptyOperator(task_id='begin')

    # загрузка из S3 на ВМ
    s3_files = PythonOperator(
        task_id='download_s3_files',
        python_callable=fetch_s3_file,
        op_kwargs={'aws_access_key_id': aws_access_key_id, 
                    'aws_secret_access_key': aws_secret_access_key,
                    'bucket': 'final-project', 
                    'path_data': '/data/'})

    # загрузка данных с ВМ в БД сsv по курсу
    currence_stg_layer = PythonOperator(
                    task_id='currence_stg_layer',
                    python_callable=load_data_to_stg,
                    op_kwargs={
                        'file_path': '/data/',  
                        'name_table': 'currencies',
                        'type_load': 'full'})
    
    # загрузка данных с ВМ в БД csv по транзакциям
    transaction_stg_layer = PythonOperator(
                    task_id='transaction_stg_layer',
                    python_callable=load_data_to_stg,
                    op_kwargs={
                        'file_path': '/data/',  
                        'name_table': 'transactions',
                        'type_load': 'increment'})
    
    end = EmptyOperator(task_id='end')

    start >> s3_files >> currence_stg_layer >> transaction_stg_layer  >> end