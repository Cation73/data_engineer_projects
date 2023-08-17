from typing import Dict, List, Optional
import pandas as pd
import contextlib
import pendulum

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.hooks.base import BaseHook

import boto3

# получение реквизитов подключения
aws_conn_id = 'S3_YANDEX_CLOUD'
aws_connection = BaseHook.get_connection(aws_conn_id)
aws_access_key_id = aws_connection.extra_dejson.get('AWS_ACCESS_KEY_ID')
aws_secret_access_key = aws_connection.extra_dejson.get('AWS_SECRET_ACCESS_KEY')


vertica_conn_id = 'VERTICA_CONN'

# получение данных из s3 хранилища
def fetch_s3_file(bucket: str, key: str):
    session = boto3.session.Session()
    s3_client = session.client(
                        service_name='s3',
                        endpoint_url='https://storage.yandexcloud.net',
                        aws_access_key_id=aws_access_key_id,
                        aws_secret_access_key=aws_secret_access_key)
    s3_client.download_file(
                            Bucket=bucket,
                            Key=key,
                            Filename=f'/data/{key}')
# загрузка данных в staging-слой
def load_dataset_file_to_vertica(
                dataset_path: str,
                schema: str,
                table: str,
                columns: List[str],
                type_override: Optional[Dict[str, str]] = None):

    df = pd.read_csv(dataset_path, dtype=type_override, 
                                    na_values=['nan'], keep_default_na=False)
    num_rows = len(df)
    df.columns = columns
    vertica_conn = SqliteHook(vertica_conn_id)
    columns = ', '.join(columns)
    copy_expr = f"""
        COPY {schema}.{table} ({columns}) FROM STDIN DELIMITER ',' ENCLOSED BY ''''
        """
    chunk_size = num_rows // 100
    with contextlib.closing(vertica_conn.cursor()) as cur:
        start = 0
        while start <= num_rows:
            end = min(start + chunk_size, num_rows)
            print(f"loading rows {start}-{end}")
            cur.execute(copy_expr, df[start:end].to_csv(index=False))
            print("loaded")
            start += chunk_size + 1

    vertica_conn.close()

with DAG(
        dag_id = 'vertica_load_group_log',
        description='load data from s3 to staging-layer of vertica',
        catchup=False,
        schedule_interval=None,
        start_date=pendulum.parse('2022-11-11')
) as dag:

    start = EmptyOperator(task_id='begin')

    s3_group_log = PythonOperator(
        task_id='s3_group_log',
        python_callable=fetch_s3_file,
        op_kwargs={'bucket': 'sprint6', 'key': 'group_log.csv'})

    stage_group_log = PythonOperator(
                    task_id='stage_group_log',
                    python_callable=load_dataset_file_to_vertica,
                    op_kwargs={
                        'dataset_path': '/data/group_log.csv',  
                        'schema': 'MRSOKOLOVVVYANDEXRU__STAGING',  
                        'table': 'group_log', 
                        'columns': ['group_id', 'user_id', 'user_id_from', 'event', 'event_dt'],  
                        'type_override': {'user_id_from': 'Int64'}  
                    })
    
    end = EmptyOperator(task_id='end')

    start >> s3_group_log >> stage_group_log >> end