import findspark
findspark.init()
findspark.find()
import os
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import datetime, timedelta
from pyspark.sql.window  import Window
import sys

def main():
        date = sys.argv[1]
        depth = sys.argv[2]
        base_input_geo_event_path = sys.argv[3]
        base_output_path = sys.argv[4]

        spark = SparkSession.builder \
                        .master("local") \
                        .appName(f"geo_recommend_friends_{date}_{depth}") \
                        .getOrCreate()

        logger= spark._jvm.org.apache.log4j.Logger
        log = logger.getLogger(__name__)

        log.info('- START RUN geo_recommend_friends')
        log.info('-- define value and function')

        URL_HDFS = 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020'


        # чтение подготовленного паркета с городами
        try:
            log.info('-- read df_geo_event')
            df_geo_event = spark.read.parquet(f'{URL_HDFS}{base_input_geo_event_path}/geo_event_{date[8:10]}_{date[5:7]}_{depth}')
        except:
            log.error('- dont read df_geo_event')

        # выделение уникальных юзеров
        try:
            log.info('-- calculate df unique users')
            df_city_from = df_geo_event.selectExpr('event.message_from as user_id', 'lat_rad', 'lon_rad', 'date', 'id')
            df_city_to = df_geo_event.selectExpr('event.message_to as user_id', 'lat_rad', 'lon_rad', 'date', 'id')

            df_users = df_city_from.union(df_city_to)
        except:
            log.error('- dont calculate df_users')

        # cross join и формирование предварительного паркета
        try:
            log.info('-- calculate prefinal parquet')
            df = df_users.join(df_users \
                                .select(F.col('user_id').alias('user_id_to'),
                                        F.col('lat_rad').alias('lat_rad_to'),
                                        F.col('lon_rad').alias('lon_rad_to'),
                                        F.col('date').alias('date_to')),
                                how='cross') \
                .where('date = date_to') \
                .where('user_id <> user_id_to') \
                .withColumn('sin_lat', F.pow(F.sin((F.col('lat_rad_to') - F.col('lat_rad'))/F.lit(2)), 2)) \
                .withColumn('sin_lon', F.pow(F.sin((F.col('lon_rad_to') - F.col('lon_rad'))/F.lit(2)), 2)) \
                .withColumn('cos_lat', F.cos(F.col('lat_rad'))) \
                .withColumn('cos_lat_rad', F.cos(F.col('lat_rad_to'))) \
                .withColumn('duration', 2*6371*F.asin(F.sqrt(F.col('sin_lat') + F.col('cos_lat')*F.col('cos_lat_rad')*F.col('sin_lon')))) \
                .where('duration <= 1') \
                .withColumn('rank', F.row_number().over(Window().partitionBy('user_id', 'user_id_to').orderBy('duration'))) \
                .where('rank = 1') \
                .withColumn('processed_dttm', F.current_timestamp()) \
                .withColumn('local_time', F.from_utc_timestamp(F.col('message_ts'), F.lit('Australia/ACT'))) \
                .select(F.col('user_id').alias('user_left'),
                        F.col('user_id_to').alias('user_right'),
                        F.col('processed_dttm'),
                        F.col('id').alias('zone_id'),
                        F.col('local_time'))
        
        except:
            log.error('- dont calculate prefinal parquet')
                
        # исключение тех пар, у которых были сообщения между друг другом и тех, кто не находится в одном сообществе
        try:
            log.info('-- calculate couples users')

            df_couple_users = df_geo_event.select(F.col('event.message_from').alias('user_from'),
                                                F.col('event.message_to').alias('user_to'))

            log.info('-- join couples users and prefinal')
            
            df = df.join(df_couple_users, 
                                on = (df['user_left'] == df_couple_users['user_from']) & (df['user_right'] == df_couple_users['user_to']),
                                how = 'leftanti') \
                    .join(df_couple_users, 
                                on = (df['user_left'] == df_couple_users['user_to']) & (df['user_right'] == df_couple_users['user_from']),
                                how = 'leftanti')
            log.info('-- calculate general channels')
            
            df_general_channels = df_geo_event \
                                            .where('event_type = "subscription"') \
                                            .select(F.col('event.subscription_channel').alias('channel_id'),
                                                    F.col('event.user').alias('user_id'))

            log.info('-- join general channels and prefinal')

            df = df.join(df_general_channels \
                                .select(F.col('user_id').alias('user_left'),
                                    F.col('channel_id').alias('channel_left')), on = 'user_left', how = 'left') \
                    .join(df_general_channels \
                                .select(F.col('user_id').alias('user_right'),
                                    F.col('channel_id').alias('channel_right')), on = 'user_right', how = 'left')

            log.info('-- filter general channels')
            result = df \
                        .where('channel_right = channel_left') \
                        .select('user_left', 'user_right',
                                'processed_dttm', 'zone_id', 'local_time')
        except:
            log.error('- dont calculate final parquet')
        
        # сохранение витрины
        try:
            log.info('-- save parquet file')
            result.repartition(1).write \
                    .parquet(f'{URL_HDFS}{base_output_path}/geo_recommend_friends_{date[8:10]}_{date[5:7]}_{depth}')

            log.info('-- task done') 
        except:
            log.error('- dont save parquet file')

        log.info('- END geo_recommend_friends')
    
if __name__ == "__main__":
        main()