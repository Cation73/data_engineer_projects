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
                        .appName(f"geo_event_zones_{date}_{depth}") \
                        .getOrCreate()

        logger= spark._jvm.org.apache.log4j.Logger
        log = logger.getLogger(__name__)

        log.info('- START RUN geo_event_zones')
        log.info('-- define value and function')

        URL_HDFS = 'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020'

        # чтение подготовленного паркета с городами
        try:
            log.info('-- read df_geo_event')
            df_geo_event = spark.read.parquet(f'{URL_HDFS}{base_input_geo_event_path}/geo_event_{date[8:10]}_{date[5:7]}_{depth}')
        except:
            log.error('- dont read df_geo_event')

        # формирование метрик
        # для недель
        try:
            log.info('-- calculate week metrics')

            df_geo_event = df_geo_event \
                            .withColumn('month', F.trunc(F.col('date'), 'month')) \
                            .withColumn('week', F.trunc(F.col('date'), 'week')) \
                            .withColumn('is_message', F.when(df_geo_event['event_type'] == 'message', 1) \
                                                       .otherwise(0)) \
                            .withColumn('is_reaction', F.when(df_geo_event['event_type'] == 'reaction', 1) \
                                                       .otherwise(0)) \
                            .withColumn('is_subscription', F.when(df_geo_event['event_type'] == 'subscription', 1) \
                                                       .otherwise(0))

            df_week_metrics = df_geo_event \
                                .groupBy(['month', 'week', 'id']) \
                                .agg(F.sum('is_message').alias('week_message'),
                                        F.sum('is_reaction').alias('week_reaction'),
                                        F.sum('is_subscription').alias('week_subscription')) \
                                .join(df_geo_event.select('month',
                                                        'week',
                                                        'id',
                                                        'event_type',
                                                        'date',
                                                        F.col('event.message_from').alias('user_id')) \
                                                .where('event_type = "message"') \
                                                .withColumn('rank', F.row_number().over(Window.partitionBy('user_id') \
                                                    .orderBy(F.asc('date')))) \
                                                .where('rank = 1') \
                                                .groupBy('month', 'week', 'id') \
                                                .count() \
                                                .withColumnRenamed('count', 'week_user'),
                                        on = ['month', 'week', 'id'],
                                        how = 'full')
            
        except:
            log.error('- dont calculate week metrics')
            
        # для месяцев
        try:
            log.info('-- calculate month metrics')
            df_month_metrics = df_geo_event \
                                .groupBy(['month', 'id']) \
                                .agg(F.sum('is_message').alias('month_message'),
                                        F.sum('is_reaction').alias('month_reaction'),
                                        F.sum('is_subscription').alias('month_subscription')) \
                                .join(df_geo_event.select('month',
                                                        'id',
                                                        'event_type',
                                                        'date',
                                                        F.col('event.message_from').alias('user_id')) \
                                                .where('event_type = "message"') \
                                                .withColumn('rank', F.row_number().over(Window.partitionBy('user_id') \
                                                                                            .orderBy(F.asc('date')))) \
                                                .where('rank = 1') \
                                                .groupBy('month', 'id') \
                                                .count() \
                                                .withColumnRenamed('count', 'month_user'),
                                        on = ['month', 'id'],
                                        how = 'full')   
            
        except:
            log.error('- dont calculate month metrics')

        # джоин месяцев и недель
        try:
            log.info('-- join weeks and months')
            df_metrics = df_week_metrics.join(df_month_metrics, 
                                             how='cross',
                                             on = ['month', 'id']) \
                                .select('month',
                                        'week',
                                        F.col('id').alias('zone_id'),
                                        'week_message',
                                        'week_reaction',
                                        'week_subscription',
                                        'week_user',
                                        'month_message',
                                        'month_reaction',
                                        'month_subscription',
                                        'month_user') \
                                .fillna(value=0) \
                                .replace('', 0)
        except:
            log.error('- dont join two df')

        # сохранение витрины
        try:
                log.info('-- save parquet file')
                df_metrics.repartition(1).write \
                        .parquet(f'{URL_HDFS}{base_output_path}/geo_event_zones_{date[8:10]}_{date[5:7]}_{depth}')

                log.info('-- task done') 
        except:
                log.error('- dont save parquet file')

        log.info('- END geo_event_zones')
    
if __name__ == "__main__":
        main()