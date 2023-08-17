import airflow
from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
import os
from datetime import date, datetime

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

default_args = {
        'owner': 'airflow',
        'start_date':datetime(2020, 1, 1),
        }

dag_spark = DAG(
        dag_id = "geo_project_datalake",
        default_args=default_args,
        schedule_interval="00 04 * * *",
        )

copy_geo_csv = BashOperator(
        task_id='copy_geo_csv',
        bash_command='''
        hdfs dfs -rm /user/cation73/data/geo/geo.csv && wget -q https://code.s3.yandex.net/data-analyst/data_engeneer/geo.csv -O /lessons/geo.csv && hdfs dfs -copyFromLocal /lessons/geo.csv hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net/user/cation73/data/geo/geo.csv
        ''',
        retries=3,
        dag=dag_spark
        )

events_partitioned = SparkSubmitOperator(
        task_id='events_partitioned',
        dag=dag_spark,
        application ='/scripts/partition_overwrite.py' ,
        conn_id= 'yarn_spark',
        application_args = ['2022-06-21', 20, '/user/master/data/geo/events', '/user/cation73/data/geo/events'],
        conf={
        "spark.driver.maxResultSize": "20g"
        },
        executor_cores = 1,
        executor_memory = '1g'
        )

geo_event_users = SparkSubmitOperator(
        task_id='geo_event_users.py',
        dag=dag_spark,
        application ='/scripts/geo_event_users.py' ,
        conn_id= 'yarn_spark',
        application_args = ['2022-06-21', 20, '/user/cation73/data/geo/geo.csv', 
                                '/user/cation73/data/geo/events', '/user/cation73/data/analytics'],
        conf={
        "spark.driver.maxResultSize": "20g"
        },
        executor_cores = 1,
        executor_memory = '1g'
        )

geo_event_zones = SparkSubmitOperator(
        task_id='geo_event_zones.py',
        dag=dag_spark,
        application ='/scripts/geo_event_zones.py' ,
        conn_id= 'yarn_spark',
        application_args = ['2022-06-21', 20, '/user/cation73/data/analytics', '/user/cation73/data/analytics'],
        conf={
        "spark.driver.maxResultSize": "20g"
        },
        executor_cores = 1,
        executor_memory = '1g'
        )

geo_recommend_friends = SparkSubmitOperator(
        task_id='geo_recommend_friends.py',
        dag=dag_spark,
        application ='/scripts/geo_recommend_friends.py' ,
        conn_id= 'yarn_spark',
        application_args = ['2022-06-21', 20, '/user/cation73/data/analytics', '/user/cation73/data/analytics'],
        conf={
        "spark.driver.maxResultSize": "20g"
        },
        executor_cores = 1,
        executor_memory = '1g'
        )

copy_geo_csv >> events_partitioned >> geo_event_users >> geo_event_zones >> geo_recommend_friends
